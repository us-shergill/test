#!/usr/bin/env python
# coding: utf-8
from tarfile import StreamError
from airflow.models import Variable
from airflow.decorators import dag, task
import json

import pendulum
import datetime
import logging
import math
import numpy as np
import time
from typing import List, Dict
import os
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.cloud.bigquery_storage_v1 import exceptions as bqstorageexceptions
from google.protobuf import descriptor_pb2
from COLLIBRA.utils.collibra_api import CollibraOutputModuleAPI
from COLLIBRA.utils.bq_utils import return_latest_datetime

from COLLIBRA import record_pb2

logger = logging.getLogger("airflow.task")


collibra_hostnames = {"dev": "arvest-dev.collibra.com", "test": "arvest-dev.collibra.com","prod": "arvest.collibra.com"}
ENVIRONMENT = os.environ.get("DBT_ENVIRONMENT")
COLLIBRA_URL_HOSTNAME = collibra_hostnames[ENVIRONMENT]
COLLIBRA_USERNAME = "svc_dataplatform_collibra_metadata"
COLLIBRA_PASSWORD = Variable.get("collibra_password")

PROJECT = "prj-d-data-platform-4922"
DATASET = "landing_collibra"
TABLE_NAME = "asset"

TEMPLATE_FILE = "nested_query.json"

PAGE_LENGTH = 5000

# Get the epochtime, in milliseconds, when initializing DAG
batch_epoch_time = int(str(time.time() * 1000).split(".")[0])
client = bigquery.Client(project=PROJECT)
table_full_name = f"{PROJECT}.{DATASET}.{TABLE_NAME}"


# Define Functions
def convert_asset_to_bq_proto_row(row_json: Dict[str, any]) -> str:
    """
    Given a json containing metadata, create the protobuf
    record.

    Args:
        row_json (dict): a json record containing asset_id, a last modified
            timestamp, and a payload of miscellaneous data.

    Returns:
        str: protobuf serialized record
    """
    row = record_pb2.Record()
    row.last_modified_timestamp = int(row_json["last_modified"] * 1000)
    row.asset_id = row_json["asset_id"]
    # Remove the asset_id from the payload
    row_json.pop("asset_id")
    row.json_payload = json.dumps(row_json)
    # Convert milliseconds to microseconds (needed by Google BigQuery Storage API)
    row.ingested_timestamp = batch_epoch_time*1000
    return row.SerializeToString()


def configure_and_create_stream(
    client: bigquery_storage_v1.BigQueryWriteClient, table: bigquery.Table
) -> types.WriteStream:
    """
    Create a "PENDING" type BigQuery Storage API Stream.

    Args:
        client (bigquery_storage_v1.BigQueryWriteClient): BQ Storage API Client
        table (bigquery.Table): A BigQuery object for the BQ table target.
    Returns:
        stream (types.WriteStream): BigQuery Storage API Stream
    """
    # Set target table for stream
    target_table = client.table_path(table.project, table.dataset_id, table.table_id)

    # Create stream object
    stream = types.WriteStream()

    # Use the PENDING type to wait
    # until the stream is committed before it is visible. See:
    # https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream.Type
    stream.type_ = types.WriteStream.Type.PENDING
    stream = client.create_write_stream(parent=target_table, write_stream=stream)
    logging.info(
        "Stream created to write to table %s in %s mode", target_table, stream.type_
    )

    return stream


def create_batch_commit_request(
    client: bigquery_storage_v1.BigQueryWriteClient,
    stream: types.WriteStream,
    table: bigquery.Table,
) -> types.BatchCommitWriteStreamsRequest:
    """
    Create a request to commit a batch of records to a stream.

    Args:
        client (bigquery_storage_v1.BigQueryWriteClient): BigQuery Storage Client
        stream (types.WriteStream): BigQuery Storage Stream
        table (bigquery.Table): Target table for stream

    Returns:
        request (types.BatchCommitWriteStreamsRequest): The request to commit the stream.
    """
    request = types.BatchCommitWriteStreamsRequest()
    request.parent = client.table_path(table.project, table.dataset_id, table.table_id)
    request.write_streams = [stream.name]
    return request


def create_request_template(stream, proto_class) -> types.AppendRowsRequest:
    """
    Create the template for an append rows request.

    Args:
        stream (types.WriteStream): A BigQuery Storage API Stream to which to append data.
        proto_class (record_pb2.Record): The record class for a protobuf record.

    Returns:
        types.AppendRowsRequest: Template for appending rows to stream
    """
    request_template = types.AppendRowsRequest()
    request_template.write_stream = stream.name

    # Define the proto schema
    proto_schema = types.ProtoSchema()
    proto_descriptor = descriptor_pb2.DescriptorProto()
    proto_class.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor

    # Define the proto writer using schema
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema = proto_schema

    # Load writer into request template
    request_template.proto_rows = proto_data

    logger.info("Append rows to stream request template defined.")
    return request_template


def get_next_page(last_timestamp, api, page) -> List[Dict[str, any]]:
    """
    Get the next page of records. The last_datetime is used to determine
    the start time for the query.

    Args:
        last_timestamp (float): The last datetime in the table
        api (CollibraOutputModuleAPI): API class instance to use to make the request.
        page (int): The page of records (from GraphML query) to obtain.

    Returns:
        List[Dict[str, any]]: List of metadata records
    """
    if not last_timestamp:
        # query full dataset (no time restrictions except on right side)
        results, results_size = api.get_results_page(
            page * PAGE_LENGTH, PAGE_LENGTH, batch_epoch_time, 0
        )
    else:
        # query since last time.
        epoch_start = int(last_timestamp * 1000) + 1
        results, results_size = api.get_results_page(
            page * PAGE_LENGTH, PAGE_LENGTH, batch_epoch_time, epoch_start
        )
    return results, results_size


def prepare_proto_rows_request(chunk, offset) -> types.AppendRowsRequest:
    """
    Prepare a row append request for a given chunk of a page.

    Args:
        page_number (int): page number of query being processed
        chunk (List[Dict[str, int]]): Chunk of records to append to stream

    Returns:
        types.AppendRowsRequest: The request containing the serialized rows
            from chunk.
    """
    request = types.AppendRowsRequest()
    request.offset = offset
    logger.info("Offset to be used: %s", request.offset)
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_rows = types.ProtoRows()
    for record in chunk:
        str_rec = convert_asset_to_bq_proto_row(record)
        proto_rows.serialized_rows.append(str_rec)
    proto_data.rows = proto_rows
    request.proto_rows = proto_data
    # Increment offset for next time
    offset = offset + len(chunk)
    return request, offset


@task
def create_table(project: str, dataset: str, table_id: str) -> None:
    """
    Create an asset table in bigquery with fields for asset id,
    modified date, and the asset data in a json payload only if
    the table does not yet exist.

    Ensure that the data is partitioned on the modified date.

    Args:
        project (str): GCP project id
        dataset (str): BigQuery dataset id
        table_id (str): BigQuery table id
    """
    # Create dataset if not exists
    client.create_dataset(dataset, exists_ok=True)
    dataset_ref = bigquery.DatasetReference(project, dataset)
    # Create table if not exists
    schema = [
        bigquery.SchemaField("json_payload", "JSON"),
        bigquery.SchemaField("asset_id", "STRING"),
        bigquery.SchemaField("last_modified_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("ingested_timestamp", "TIMESTAMP")
    ]
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="last_modified_timestamp",  # name of column to use for partitioning
    )
    table.clustering_fields = ["asset_id","ingested_timestamp"]

    table = client.create_table(table, exists_ok=True)
    logger.info(f"Table {table_full_name} was created if it did not yet exist.")


@task
def get_latest_date(table) -> datetime.datetime:
    """
    Obtain the latest "last_modified_timestamp" from the table.

    Args:
        table (str): The table fully qualified name containing Collibra
            metadata. e.g. project.dataset.table

    Returns:
        datetime.datetime: The last last_modified_timestamp in datetime
            (UTC) present in BQ table.
    """
    last_partition_datetime = return_latest_datetime(
        client, table, "last_modified_timestamp"
    )
    if last_partition_datetime:
        logger.info(
            "The table currently contains all modified records through"
            f"{last_partition_datetime.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        last_partition_datetime = last_partition_datetime.timestamp()
    else:
        logger.info("The table has no records yet.")
    return last_partition_datetime


@task
def write_collibra_metadata_to_BQ(last_modified: datetime.datetime) -> None:
    """
    Incrementally obtain Collibra Metadata API data by paginating.
    For each page, make an append row request to a BigQuery Storage
    API stream.

    Once all results have been appended to the stream, commit those
    records so they are visible to users. If there is an error before
    the commit step, the entire transaction is cancelled, and the
    next task run will obtain the same data.

    Args:
        last_modified (datetime.datetime): The latest modified datetime in
        the table.
    """

    # Initialize the CollibraAPI
    collibra = CollibraOutputModuleAPI(
        COLLIBRA_URL_HOSTNAME,
        COLLIBRA_USERNAME,
        COLLIBRA_PASSWORD,
    )
    collibra.get_auth()
    logger.info("Collibra API client authenticated.")

    collibra.set_query_template(__file__, TEMPLATE_FILE)
    logger.info("Collibra API client template set to %s", TEMPLATE_FILE)

    # Initialize the BQ Storage API Stream
    dataset_ref = bigquery.DatasetReference(project=PROJECT, dataset_id=DATASET)
    table = bigquery.Table(
        bigquery.TableReference(dataset_ref=dataset_ref, table_id=TABLE_NAME)
    )
    bq_write_client = bigquery_storage_v1.BigQueryWriteClient()
    stream = configure_and_create_stream(bq_write_client, table)
    request_template = create_request_template(stream, record_pb2.Record)
    active_stream = writer.AppendRowsStream(bq_write_client, request_template)

    # For each batch of new Collibra Metadata, Append to stream
    logger.info("Iterating through results")
    page_number = 0
    offset = 0
    responses = []
    while "Iterating through results":
        # Get the next result set
        results, results_size_mb = get_next_page(last_modified, collibra, page_number)

        # Append rows to request
        if results:
            logger.info(
                "Retrieved page %s of GraphQL query results with %s records",
                page_number,
                len(results),
            )
            num_chunks = math.ceil(results_size_mb / 10.0)
            if num_chunks > 1:
                logger.info(
                    (
                        "The full result set for page %s is more than the max "
                        "allowed by the BigQuery Storage API; splitting this "
                        "result set into %s chunks"
                    ),
                    page_number,
                    num_chunks,
                )
            results_chunks = np.array_split(results, num_chunks)
            # Iterate through the chunks
            for i, chunk in enumerate(results_chunks):
                request, offset = prepare_proto_rows_request(chunk, offset)
                logger.debug("Sending request to stream")
                try:
                    future = active_stream.send(request)
                except bqstorageexceptions.StreamClosedError:
                    # Create a new append row stream in the case it gets closed
                    logger.info("Creating a new stream because other writer " "closed")
                    active_stream = writer.AppendRowsStream(
                        bq_write_client, request_template
                    )
                    future = active_stream.send(request)
                    logger.info(
                        "Request for chunk %i of %i for page %i sent.",
                        i + 1,
                        num_chunks,
                        page_number,
                    )
                responses.append(future)
            # If this page of records was not full-length, finish up request
            if len(results) < PAGE_LENGTH:
                break
            # Increment page number
            page_number += 1
        else:
            logger.info("No more records left to append")
            break
    # Get results of appending requests
    results = [response.result() for response in responses]
    logger.info("Closing and finalizing stream")
    active_stream.close()
    bq_write_client.finalize_write_stream(name=stream.name)

    # Commit
    commit_request = create_batch_commit_request(bq_write_client, stream, table)
    response = bq_write_client.batch_commit_write_streams(commit_request)
    if response.stream_errors:
        raise StreamError(
            "The stream had an error committing."
            f"{[print(error) for error in response.stream_errors()]}"
        )

    logger.info("Writes to stream: '%s' have been committed.", stream.name)
    logger.info("%i rows were added to the table %s", offset, table_full_name)


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2023, 7, 19, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["metadata", "collibra", "api"],
)
def collibra_metadata_sync():
    """
    Create a dag which:
    1. Creates a table (if needed)
    2. Determines the latest date from that table
    3. Uses this date to obtain Collibra metadata
    on assets that have been modified since the last successful day run.
    """
    created_table = create_table(PROJECT, DATASET, TABLE_NAME)
    date = get_latest_date(table_full_name)
    write_stream = write_collibra_metadata_to_BQ(date)

    [created_table >> date >> write_stream]


define_dag = collibra_metadata_sync()
