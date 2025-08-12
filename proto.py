3.0	Business Requirements
1.	Vendor file
1.1.	Format 
1.1.1.	CSV
1.1.2.	Tab delimited format
1.1.3.	File per contract & PBP combination is required
1.2.	File Naming convention
1.2.1.	2026SAR_Mailing_Fulfillment_Contract_PBP_Segment_CCYYMMDD
1.2.1.1.	The date represents the date the file was generated
1.2.1.2.	If Segment is null/blank or 000 populate as 000.
1.3.	Business Rules
1.3.1.	One file should be created for each contract & PBP combination for mailing purposes. 
1.3.1.1.	Templates are different per service area which necessitates different mail files. 
1.3.2.	Return Mail address should be PO BOX 1002, Nashville, TN 37202
1.3.3.	EAM data tables are the source of truth for the data. 
1.3.4.	A row should return for any member that is active at the time of the data generation for mailing and without a future disenrollment date. 
1.3.5.	The county information should be based on the customer’s physical address and not any county associated to a mailing address. 
1.3.6.	If any record is missing required data, it should still be placed on a separate file but named _error in same format as mailing for manual correction and error with what is missing. 
1.3.7.	This can include EGWP plans but should be confirmed yearly during bid processes by business. The PBP starts with an ‘8’
1.3.7.1.	For 2026, no EGWP plans impacted by SAR.
1.3.8.	Files should be generated daily after initial file for any members that newly enrolled into a plan fitting the SAR guidance so a letter can be sent. 
1.3.8.1.	New enrollments should be identified as new enrollments or new members as of get date for the current year. 
1.3.9.	Include members where the physical state and county code is listed below for only the contract and PBPs listed below. 
1.3.10.	Exclude any members not in the above table per the criteria in the business rules.  
2.	Summary Report
2.1.	File format
2.1.1.	CSV
2.2.	File Name
2.2.1.	GBSF_MAPD_SAR_Summary_CCYYMMDD
2.3.	Business Rules
2.3.1.	Count of member records by contract and PBP on the SAR file
2.3.1.1.	This is used to reconcile membership counts to ensure all members receive a SAR
2.4.	Data Fields
2.4.1.	New year Contract
2.4.2.	New year PBP
2.4.3.	New Year Segment
2.4.4.	Total count of membership
3.	Reconciliation report
3.1.	File format
3.1.1.	CSV
3.2.	File Name
3.2.1.	GBSF_SAR_MissingMailDate_CCYYYMMDD
3.3.	Business Rules
3.3.1.	Take mail date files received from vendor and compare to outbound data files to confirm all have receipt dates
3.3.2.	Generate a report with Member ID and Record ID that do not show a mail date. 
All reports and file locations to be stored here: \\mdnas1.healthspring.inside\IS\ApplicationData\EXPORT\CardFile\SARS & NR\NextYear_Production_Files\MailDateResponseFiles

SQL
--2026SAR_Mailing_Fulfillment_Contract_PBP_Segment_CCYYMMDD.csv
--Extract SQL work in progress
SELECT
MBR.MEMCODNUM --N/A
,MBR.MemberID as "Member ID"
,MBR.CurrentEffDate as LatestEffectiveDate --N/A 
,MBR.TermDate as TermDate --N/A
,MBRS.Description as CurrentStatus --N/A 
,DMG.OECCounty --N/A
,DMG.SCC1 as DMGSCC1 --N/A
,DMG.SCC2 as DMGSCC2 --N/A
,DMG.FirstName as FirstName
,DMG.LastName as LastName
,PhyADDR.Address1 as PhyAddr1 --N/A
,PhyADDR.Address2 as PhyAddr2 --N/A
,PhyADDR.City as PhyCity --N/A
,PhyADDR.State as PhyState --N/A
,CASE 
  WHEN PhyADDR.ZipFour IS NULL THEN PhyADDR.Zip 
  ELSE PhyADDR.Zip||'-'||PhyADDR.ZipFour 
 END as PhyZip --N/A
,MailADDR.Address1 as MailAddr1
,MailADDR.Address2 as MailAddr2
,MailADDR.City as MailCity
,MailADDR.State as MailState
,CASE 
  WHEN MailADDR.ZipFour IS NULL THEN MailADDR.Zip 
  ELSE MailADDR.Zip||'-'||MailADDR.ZipFour  
 END as MailZip
,MBR.PlanID as CContract
,MBR.PBP as CPBP
,COALESCE(EAMSGMNT.SegmentID,MBR.SegmentID,'000') as CSegment
,EAMSGMNT.Span_EffDate
,EAMSGMNT.Span_TermDate
,PLN.ProductName as "Plan Name"
,COALESCE(SPAN.SPANSCC,DMG.SCC1) as SCCCode
,SPAN.SPANSCC --N/A 
,DMG.SCC1 --N/A 
,COALESCE(DMG."Language",'ENG') as LanguageText
,CASE 
	WHEN DMG.AccessibilityFormat = 1 THEN 'Braille'
	WHEN DMG.AccessibilityFormat = 2 THEN 'Large Print'
	WHEN DMG.AccessibilityFormat = 3 THEN 'Audio CD'
	WHEN DMG.AccessibilityFormat = 4 THEN 'Data CD'
	ELSE '' 
	END AS "Alternate Format"
,SC.CountyName as County
,CASE 
	WHEN PhyADDR.State = SC.State THEN PhyADDR.State 
	Else MBOMSTREF.STATE_ABBREVIATED_NAME 
	END AS PhysicalState
,SC.State as CountyState --N/A
,CASE 
	WHEN PhyADDR.State = SC.State THEN MBOM.State 
	Else MBOMNOST.State 
	END AS "Plan State"
,MemberID||'_SAR_'||(CURRENT_DATE (FORMAT 'YYYYMMDD')) as "Record ID"
,CASE 
	WHEN PhyADDR.State = SC.State THEN MBOM.RecordType
	Else MBOMNOST.RecordType 
	END AS "Record Type"
,CASE 
	WHEN PhyADDR.State = SC.State THEN MBOM.LetterMaterialID 
	Else MBOMNOST.LetterMaterialID 
	END AS "Material ID"
,CASE 
	WHEN PhyADDR.State = SC.State THEN MBOM.PLANReplacementID 
	Else MBOMNOST.PLANReplacementID 
	END AS "Plan Replacement ID"

FROM (
	SELECT
	MemberID, MemCodNum, PlanID, PBP, SegmentID, SRC_DATA_KEY, CurrentEffDate, TermDate, MemberStatus
	FROM GBS_FACET_CORE_V.EAM_tbEENRLMembers
	WHERE SRC_DATA_KEY = '210'
	and cast(substr(TermDate,1,10) as date format 'YYYY-MM-DD') > current_date --To exclude termed members 
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MemCodNum ORDER BY CurrentEffDate DESC) = 1) MBR

	JOIN GBS_FACET_CORE_V.EAM_tbMemberInfo DMG
	ON 	MBR.SRC_DATA_KEY = DMG.SRC_DATA_KEY 
	AND MBR.MemCodNum = DMG.MemCodNum
	--AND MBR.PBP NOT LIKE '8%' --Exclude EGWP

	JOIN GBS_FACET_CORE_V.EAM_tbMemberStatus MBRS
	ON MBR.MemberStatus = MBRS.Status
	--AND MBR.MemberStatus in ('1','2') --Awaiting business confirmation 
	
	LEFT JOIN 
	(
		Select 
				tbe.PlanID,
				tbe.MemCodNum,
				tbe.HIC AS SpanMBINumber,
				tbe.SPANTYPE AS SpanType,
				tbe."Value" AS SpanValue,
				CAST(tbe.STARTDATE AS DATE) AS Span_EffDate,
				CAST(tbe.ENDDATE AS DATE) AS Span_TermDate,
				CAST(tbe.LastModifiedDate AS DATE) AS LAST_MODIFIED,
				CAST(tbe.DateCreated AS DATE) AS CREATE_DATE,
				Tbt.DateCreated AS TR_CREATE_DATE,
				--SegmentId population preference: SEGC, SEGD, Transactions, Default000
				COALESCE(NULLIF(TRIM(tbe.SEGC_SegmentID), ''),NULLIF(TRIM(tbe.SEGD_SegmentID), ''),NULLIF(TRIM(tbt.SegmentID), ''), '000') AS SegmentID,
				SEGC_startDate,SEGC_EndDate,SEGD_startDate,SEGD_EndDate
                   FROM (
                   --Adding Value from SEGC,SEGD spans as SegmentID to PBP span
                   select  d."Value" as SEGD_SegmentID,d.StartDate as SEGD_startDate, d.EndDate as SEGD_EndDate, c.* from
                   (select  b."Value" as SEGC_SegmentID, b.StartDate as SEGC_startDate, b.EndDate as SEGC_EndDate,
                          a.* from GBS_FACET_CORE_V.EAM_tbENRLSpans a LEFT JOIN GBS_FACET_CORE_V.EAM_tbENRLSpans b
                           ON a.MemCodNum = b.MemCodNum and a.PlanID = b.PlanID and (b.StartDate between  a.StartDate and a.EndDate) and a.SPANTYPE = 'PBP' and b.SPANTYPE='SEGC' ) c 
               LEFT JOIN GBS_FACET_CORE_V.EAM_tbENRLSpans d
                                 ON c.MemCodNum = d.MemCodNum and c.PlanID = d.PlanID and ((SEGC_startDate is not null and c.SEGC_startDate = d.StartDate) or (SEGC_startDate is null and c.StartDate= d.StartDate)) and c.SPANTYPE = 'PBP' and d.SPANTYPE='SEGD'
                     ) tbe  LEFT JOIN GBS_FACET_CORE_V.EAM_tbTransactions tbt
                                        ON tbt.MemCodNum = tbe.MemCodNum
                                          AND tbt.PlanID = tbe.PlanID
                                          AND tbt.PBPID = tbe."Value"
                                          AND (tbe.StartDate <= tbt.EffectiveDate AND tbe.EndDate >= tbt.EffectiveDate)
                                          AND ((tbt.TransCode = '61') OR (tbt.TransCode IN ('80') AND tbt.ReplyCodes = '287'))
                                          AND tbt.TransStatus IN (5)
                                          WHERE tbe.SpanType = 'PBP'
                                          QUALIFY ROW_NUMBER() OVER (PARTITION BY tbe.MemCodNum, tbe.PlanID, tbe."Value" ORDER BY Span_EffDate DESC, Span_TermDate desc) = 1
                                          ) EAMSGMNT
										  
	ON MBR.MEMCODNUM = EAMSGMNT.MEMCODNUM
	AND MBR.PlanID = EAMSGMNT.PlanID
	AND MBR.PBP = EAMSGMNT.SpanValue

	JOIN GBS_FACET_CORE_V.EAM_tbPlan_PBP PLN
	ON MBR.PlanID = PLN.PlanID
	AND MBR.PBP = PLN.PBPID

	LEFT JOIN (
	SELECT 
	MemCodNum, Address1, Address2, City, State, Zip, ZipFour, SRC_DATA_KEY
	FROM GBS_FACET_CORE_V.EAM_MemberManagerAddress
	WHERE AddressUse = '1'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MemCodNum ORDER BY StartDate DESC) = 1) PhyADDR 
	ON MBR.SRC_DATA_KEY = PhyADDR.SRC_DATA_KEY 
	AND MBR.MemCodNum = PhyADDR.MemCodNum

	LEFT JOIN (
	SELECT 
	MemCodNum, Address1, Address2, City, State, Zip, ZipFour, SRC_DATA_KEY
	FROM GBS_FACET_CORE_V.EAM_MemberManagerAddress
	WHERE AddressUse = '2'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MemCodNum ORDER BY StartDate DESC) = 1) MailADDR 
	ON MBR.SRC_DATA_KEY = MailADDR.SRC_DATA_KEY 
	AND MBR.MemCodNum = MailADDR.MemCodNum
	
	LEFT JOIN (
	select 
	memcodnum, "value" as SPANSCC  
	FROM GBS_FACET_CORE_V.EAM_tbENRLSpans
	WHERE spantype = 'SCC'
	qualify row_number() over (partition by memcodnum order by startdate desc)=1) span
	ON dmg.memcodnum = span.memcodnum

	JOIN VT_SAR_PLAN SAR
	ON MBR.PlanID = SAR.Contract
	AND MBR.PBP = SAR.PBP
	AND CSegment = SAR.Segment
	AND SCCCode = SAR.ServiceAreaCountyCode

	LEFT JOIN GBS_FACET_CORE_V.EAM_TB_EAM_SCC_STATES SC
	ON SCCCode = SC.SCC_CODE
	
	LEFT JOIN 
	(
		SELECT distinct STATE_ABBREVIATED_NAME, STATE_NAME from REFDATA_CORE_V.STATE_COUNTY) STREF
	ON SC.State = STREF.STATE_ABBREVIATED_NAME 

	LEFT JOIN VT_SAR_NR_BOM_2026 MBOM
	ON MBR.PlanID = MBOM.Contract
	AND MBR.PBP = MBOM.PBP
	AND CSegment = MBOM.Segment
	AND STREF.STATE_NAME = MBOM.State
	
	LEFT JOIN (SELECT * 
	FROM VT_SAR_NR_BOM_2026 
	QUALIFY row_number() OVER (PARTITION BY Contract, PBP, Segment Order by State) = 1) MBOMNOST
	ON MBR.PlanID = MBOMNOST.Contract
	AND MBR.PBP = MBOMNOST.PBP
	AND CSegment = MBOMNOST.Segment
	
	LEFT JOIN 
	(
		SELECT distinct STATE_ABBREVIATED_NAME, STATE_NAME from REFDATA_CORE_V.STATE_COUNTY) MBOMSTREF
	ON MBOMNOST.State = MBOMSTREF.STATE_NAME ;
	
	--SAR Fallout and Exclusion
select 
Case 
when physicalstate <> countystate Then 'Valid, Physical Address State '||physicalstate||', SCC State '||CountyState
when MailAddr1 is NULL Then 'Fallout, no mailing address'
when "Member ID" is NULL Then 'Fallout, no member ID'
when FirstName is NULL Then 'Fallout, no first name'
when LastName is NULL Then 'Fallout, no last name'
when MailCity is NULL Then 'Fallout, no mail address city'
when MailState is NULL Then 'Fallout, no mail address state'
when MailZip is NULL Then 'Fallout, no mail address zip'
When "Plan Name" is NULL Then 'Fallout, no plan name'
When PhyState is NULL Then 'Fallout, no physical address state'
when "Material ID" is NULL Then 'BOM, Missing Data'
--when CurrentStatus = 'Not Enrolled' and RIGHT("Member ID",2)='XX' Then 'Do not report, not enrolled'
when CurrentStatus = 'Not Enrolled' Then 'Do not report, not enrolled'
when CurrentStatus = 'Pending' and cast(substr(LatestEffectiveDate,1,10) as date format 'YYYY-MM-DD') < current_date and Span_EffDate IS NULL 
	Then 'Do not report, member effective date is in the past, has no span and is considered canceled'
when CurrentStatus = 'Pending' and cast(substr(LatestEffectiveDate,1,10) as date format 'YYYY-MM-DD') >= current_date and Span_EffDate IS NULL 
	Then 'Fallout, member status pending with effective date in future with no span'
when CurrentStatus = 'Pending' Then 'Valid, status pending and effectivedate in future with span'
when SCCCode is null then 'Fallout, no SCC'
Else 'Valid' end as Comments,
SAR.*
from hslabgrowthrpt.TEST_SAR_AB_VT SAR;

---------------------------------------------
# Databricks notebook source
# Below exmple notebook allows a user to input their credentials to connect to Teradata
#
# A user can then read data from TDV and load into a Databricks table, or load data from a specified Databricks table to a specified Teradata table (and can do it quickly by using Teradata Fastload).
#
# Make sure to use a cluster that has the Teradata JDBC Driver installed, as well as the teradatasql python package (all team-level shared clusters have this)

# COMMAND ----------

##############################################################
# PART 1: ENTERING YOUR CREDENTIALS TO SETUP YOUR CONNECTION #
##############################################################

# COMMAND ----------

import getpass
import teradatasql

# COMMAND ----------

username = input("Enter username (personal LAN ID or service account username): ").strip()

# COMMAND ----------

password = getpass.getpass("Enter password for username: ").strip()

# COMMAND ----------

#############################################################
# PART 2: READING FROM TDV AND SAVING DATA INTO A DBX TABLE #
#############################################################

# COMMAND ----------

tdv_host = "HSTNTDDEV.HEALTHSPRING.INSIDE"
connection_string_read = f"jdbc:teradata://{tdv_host}/LOGMECH=LDAP,COLUMN_NAME=ON,TYPE=FASTEXPORT"
query = "SELECT * FROM REPORTING_DEV2_V.CDO_PROV_DEMG"
dbx_table = 'reporting.example_tdv_read_cdo_prov_demg'

df = (spark.read
  .format("jdbc")
  .option('driver', "com.teradata.jdbc.TeraDriver")
  .option("url", connection_string_read)
  .option("query", query)
  .option("user", username)
  .option("password", password)
  .load()
)
df.write.mode('overwrite').saveAsTable(dbx_table)

# COMMAND ----------

# Display our data that we have just read in from TDV and saved to DBX table
saved_table_dataframe = spark.table(dbx_table)

saved_table_dataframe.display()

# COMMAND ----------

############################################################
# PART 3: WRITING DATA TO TDV USING SPARK AND TDV FASTLOAD #
############################################################

# COMMAND ----------

target_tdv_table = "REPORTING_DEV2_T.EXAMPLE_TDV_WRITE_CDO_PROV_DEMG"
connection_string_write_fastload = f"jdbc:teradata://{tdv_host}/LOGMECH=LDAP,FLATTEN=ON,TYPE=FASTLOAD"
landing_table = f'{target_tdv_table}_WORKING' # Used for staging our data in TDV before moving it to the target table

# COMMAND ----------

def delete_table_if_exists(table_full):
    try:
        con = teradatasql.connect(host = tdv_host, user = username, password = password, logmech = "LDAP", encryptdata="true")
        cursor = con.cursor()

        cursor.execute(f"DROP TABLE {table_full}")
        print(f"Deleted following table '{table_full}' from TDV.")
    except Exception as e:
        print(f"Unable to delete table from TDV: {str(e)[:200]}")
    finally:
        cursor.close()
        con.close()

# COMMAND ----------

# Clean up work table if it exists from a previous load
delete_table_if_exists(landing_table)

# COMMAND ----------

print(f"Writing to OSS TDV temporary work table '{landing_table}' via FASTLOAD.")
(
    saved_table_dataframe.write
    .format("jdbc")
    .option('driver', "com.teradata.jdbc.TeraDriver")
    .option("url", connection_string_write_fastload)
    .option("dbtable", landing_table)
    .option("user", username)
    .option("password", password)
    .option("batchsize", 50000)
    .option("numPartitions", 1)
    .option("truncate", True)
    .mode("overwrite")
    .save()
)

# COMMAND ----------

# Clean up final target table if it exists
delete_table_if_exists(target_tdv_table)

# COMMAND ----------

# Creates our final target table based on the work table
# Note: The reason we create a work table first is to ensure our final table can still be queried while the load is happening.
try:
    con = teradatasql.connect(host = tdv_host, user = username, password = password, logmech = "LDAP", encryptdata="true")
    cursor = con.cursor()

    cursor.execute(f"CREATE TABLE {target_tdv_table} AS (SELECT * FROM {landing_table}) WITH DATA")
    print(f"Created table '{target_tdv_table}' from '{landing_table}'")
except Exception as e:
    print(f"Unable to create table '{target_tdv_table}' from '{landing_table}': {str(e)[:200]}")
finally:
    cursor.close()
    con.close()

# COMMAND ----------

###################
# PART 4: CLEANUP #
###################

# Clean up TDV work/landing table
delete_table_if_exists(landing_table)

# Clean up TDV target table
delete_table_if_exists(target_tdv_table)

# Clean up initial DBX table that we loaded from TDV
spark.sql(f"DROP TABLE IF EXISTS {dbx_table}")

                                                                                
                                                                                
                                                                                

