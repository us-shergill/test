import os
import smbclient
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from airflow.hooks.base_hook import BaseHook
from airflow.configuration import conf
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from airflow.utils.email import send_mime_email

# Constants (reuse from DAG or configure via import/config)
EMAIL_SENDER = conf.get("smtp", "SMTP_MAIL_FROM")
EMAIL_RECIPIENT = 'user@example.com'

# Helper for Teradata connection
def get_td_engine(td_conn_id: str, host: str):
    td_conn = BaseHook.get_connection(td_conn_id)
    td_user = td_conn.login
    td_pass = td_conn.password
    td_extra = td_conn.extra_dejson
    td_logmech = td_extra.get("logmech", "TD2")
    engine_str = f'teradatasql://{td_user}:{td_pass}@{host}/?logmech={td_logmech}&encryptdata=true'
    return create_engine(engine_str)

def check_and_load_file(acumen_input_path, acumen_archive_path, target_table, host, user, passwd, td_conn_id):
    smbclient.register_session(host, username=user, password=passwd)
    files = smbclient.listdir(acumen_input_path)
    input_files = [f for f in files if f.startswith("SAR_PLAN_INPUT") and f.endswith(".csv")]
    if not input_files:
        print("No SAR input files found.")
        return None

    latest_file = max(input_files)
    latest_path = os.path.join(acumen_input_path, latest_file)
    with smbclient.open_file(latest_path, mode='r') as f:
        df = pd.read_csv(f)

    engine = get_td_engine(td_conn_id, host)
    with engine.connect() as conn:
        conn.execute(text(f"DELETE FROM {target_table}"))
        df['LOAD_DT'] = datetime.now().strftime('%Y-%m-%d')
        df.to_sql(target_table, conn, index=False, if_exists='append')

    ts = datetime.now().strftime('%Y%m%d%H%M%S')
    archive_name = latest_file.replace('.csv', f'_{ts}.csv')
    smbclient.rename(latest_path, os.path.join(acumen_archive_path, archive_name))
    print(f"Loaded new data and archived file: {archive_name}")

    return datetime.now().strftime('%Y-%m-%d')

def run_sar_queries(config, execution_date):
    import numpy as np

    engine = get_td_engine(config['td_conn_id'], config['host'])
    with engine.connect() as conn:
        with open(config['sql_path']) as f:
            extract_sql = f.read()
        result_df = pd.read_sql(extract_sql, conn)

    def get_fallout_comment(row):
        if row['physicalstate'] != row['countystate']:
            return f"Fallout, Physical Address State {row['physicalstate']}, SCC State {row['countystate']}"
        elif pd.isnull(row['MailAddr1']): return 'Fallout, no mailing address'
        elif pd.isnull(row['Member ID']): return 'Fallout, no member ID'
        elif pd.isnull(row['FirstName']): return 'Fallout, no first name'
        elif pd.isnull(row['LastName']): return 'Fallout, no last name'
        elif pd.isnull(row['MailCity']): return 'Fallout, no mail address city'
        elif pd.isnull(row['MailState']): return 'Fallout, no mail address state'
        elif pd.isnull(row['MailZip']): return 'Fallout, no mail address zip'
        elif pd.isnull(row['Plan Name']): return 'Fallout, no plan name'
        elif pd.isnull(row['PhyState']): return 'Fallout, no physical address state'
        elif pd.isnull(row['Material ID']): return 'BOM, Missing Data'
        elif row['CurrentStatus'] == 'Not Enrolled': return 'Do not report, not enrolled'
        elif row['CurrentStatus'] == 'Pending' and pd.to_datetime(row['LatestEffectiveDate'][:10]) < pd.to_datetime('today') and pd.isnull(row['Span_EffDate']):
            return 'Do not report, member effective date is in the past, has no span and is considered canceled'
        elif row['CurrentStatus'] == 'Pending' and pd.to_datetime(row['LatestEffectiveDate'][:10]) >= pd.to_datetime('today') and pd.isnull(row['Span_EffDate']):
            return 'Fallout, member status pending with effective date in future with no span'
        elif pd.isnull(row['SCCCode']): return 'Fallout, no SCC'
        else: return 'Valid'

    result_df['Comments'] = result_df.apply(get_fallout_comment, axis=1)
    fallout_df = result_df[result_df['Comments'] != 'Valid']
    valid_df = result_df[result_df['Comments'] == 'Valid']

    hist_df = pd.read_sql(f"SELECT * FROM {config['history_tracking_table']}", conn)
    new_records = pd.concat([
        fallout_df.assign(STATUS_TAG='Fallout'),
        valid_df[~valid_df['MEMCODNUM'].isin(fallout_df['MEMCODNUM'])].assign(STATUS_TAG='Valid')
    ])
    new_records['LOAD_DATE'] = execution_date
    new_records[['MEMCODNUM', 'Member ID', 'CContract', 'CPBP', 'CSegment', 'STATUS_TAG', 'LOAD_DATE']].to_sql(config['history_tracking_table'], conn, index=False, if_exists='append')

    date_tag = datetime.now().strftime('%Y%m%d')
    for (contract, pbp, segment), group in valid_df.groupby(['CContract', 'CPBP', 'CSegment']):
        segment_val = segment or '000'
        fname = f"2026SAR_Mailing_Fulfillment_{contract}_{pbp}_{segment_val}_{date_tag}.csv"
        group.to_csv(os.path.join(config['acumen_output_path'], fname), sep='\t', index=False)

    for (contract, pbp, segment), group in fallout_df.groupby(['CContract', 'CPBP', 'CSegment']):
        segment_val = segment or '000'
        err_fname = f"2026SAR_Mailing_Fulfillment_{contract}_{pbp}_{segment_val}_{date_tag}_error.csv"
        group.to_csv(os.path.join(config['acumen_output_path'], err_fname), sep='\t', index=False)

    fallout_df.to_csv(os.path.join(config['acumen_output_path'], config['sar_error_file']), index=False)
    summary = valid_df.groupby(['CContract', 'CPBP', 'CSegment']).size().reset_index(name='Total_Members')
    summary.columns = ['NewYearContract', 'NewYearPBP', 'NewYearSegment', 'TotalMembership']
    summary.to_csv(os.path.join(config['acumen_output_path'], config['sar_summary_file']), index=False)

    maildate_dfs = []
    for file in smbclient.listdir(config['response_files_path']):
        if file.lower().endswith(".csv") and "MailDate" in file:
            with smbclient.open_file(os.path.join(config['response_files_path'], file), mode='r') as f:
                maildate_dfs.append(pd.read_csv(f))

    maildate_ids = set()
    if maildate_dfs:
        maildate_all = pd.concat(maildate_dfs, ignore_index=True)
        maildate_ids = set(maildate_all['Member ID'].astype(str).str.strip().unique())

    recon_df = valid_df[~valid_df['Member ID'].astype(str).str.strip().isin(maildate_ids)][['Member ID', 'RecordID']].drop_duplicates()
    recon_df.to_csv(os.path.join(config['acumen_output_path'], config['sar_error_file']), index=False)

def send_summary_email(acumen_output_path, sar_summary_file, sar_error_file):
    email_msg = MIMEMultipart()
    email_msg["Subject"] = f"SAR File Load Summary - {datetime.now().strftime('%Y-%m-%d')}"

    for path in [sar_summary_file, sar_error_file]:
        full_path = os.path.join(acumen_output_path, path)
        with open(full_path, 'rb') as f:
            attach = MIMEApplication(f.read(), _subtype='csv')
            attach.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
            email_msg.attach(attach)

    body = MIMEText("SAR file processing complete. Attachments contain summary and fallout.", 'plain')
    email_msg.attach(body)

    send_mime_email(e_from=EMAIL_SENDER, e_to=EMAIL_RECIPIENT, mime_msg=email_msg)


--------------


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
from airflow.hooks.base_hook import BaseHook
import os

from utils.sar_utils import check_and_load_file, run_sar_queries, send_summary_email

dag_id = 'sar_file_loader_with_deltas'
load_env = 'DEV'
sar_base_path = r'\\mdnas1.healthspring.inside\IS\ApplicationData\EXPORT\CardFile\SARS & NR\NextYear_Production_Files'
acumen_input_path = os.path.join(sar_base_path, 'input/')
acumen_archive_path = os.path.join(sar_base_path, 'archive/')
acumen_output_path = os.path.join(sar_base_path, 'output/')
response_files_path = os.path.join(sar_base_path, 'MailDateResponseFiles')
td_conn_id = 'oss-teradata'
target_table = 'SAR_PLAN_INPUT_REFERENCE'
sar_output_table = 'SAR_OUTPUT_TABLE'
history_tracking_table = 'HST_SAR_MEMBER_STATUS'
sar_summary_file = 'GBSF_MAPD_SAR_Summary_' + datetime.now().strftime('%Y%m%d') + '.csv'
sar_error_file = 'GBSF_SAR_MissingMailDate_' + datetime.now().strftime('%Y%m%d') + '.csv'

conn = BaseHook.get_connection('comp_oper_creds')
user = conn.login
passwd = conn.password
host = conn.host

config = {
    'td_conn_id': td_conn_id,
    'host': host,
    'history_tracking_table': history_tracking_table,
    'acumen_output_path': acumen_output_path,
    'sar_summary_file': sar_summary_file,
    'sar_error_file': sar_error_file,
    'response_files_path': response_files_path,
    'sql_path': '/dags/sql/sar_extract.sql'
}

def dag_wrapper():
    execution_date = check_and_load_file(acumen_input_path, acumen_archive_path, target_table, host, user, passwd, td_conn_id)
    if execution_date:
        run_sar_queries(config, execution_date)

def email_wrapper():
    send_summary_email(acumen_output_path, sar_summary_file, sar_error_file)

with DAG(
    dag_id=dag_id,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)}
) as dag:
    process_task = PythonOperator(task_id='process_sar_pipeline', python_callable=dag_wrapper)
    email_task = PythonOperator(task_id='email_summary', python_callable=email_wrapper)
    process_task >> email_task
