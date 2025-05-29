# Testing for CI/CD
import json
import logging
import functions_framework
import os
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

PROJECT_NAME = "my-proforce-project"
BUCKET_NAME = "pest_payrix"
BQ_DATASET = "pest_payrix"
BQ_TABLE = "config"
JSON_CONFIG_PATH = "config/streams_config.json"
PROCESSED_JSON_CONFIG_PATH = "config/processed_streams_config.json"

def load_bigquery_config(project_name: str, dataset: str, table: str, frequency: str, load_type: str, src_type: str) -> list:
    """Load streams configuration from BigQuery with filters for frequency, load_type, and src_type."""
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT * 
            FROM `{project_name}.{dataset}.{table}`
            WHERE frequency = @frequency
            AND load_type = @load_type
            AND src_type = @src_type
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("frequency", "STRING", frequency),
                bigquery.ScalarQueryParameter("load_type", "STRING", load_type),
                bigquery.ScalarQueryParameter("src_type", "STRING", src_type),
            ]
        )
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        config = [dict(row) for row in results]
        if not config:
            raise ValueError(f"No data found in {dataset}.{table} for frequency={frequency}, load_type={load_type}, src_type={src_type}")
        return config
    except Exception as e:
        logger.error(f"Error loading config from BigQuery: {str(e)}")
        raise

def save_config_to_json(bucket_name: str, file_path: str, config: list) -> None:
    """Save configuration to GCS as JSON."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.upload_from_string(json.dumps(config, indent=2), content_type='application/json')
        logger.info(f"Saved config to gs://{bucket_name}/{file_path}")
    except Exception as e:
        logger.error(f"Error saving config to gs://{bucket_name}/{file_path}: {str(e)}")
        raise

def get_max_modified_from_parquet(bucket_name: str, stream: str) -> str:
    """Get max modified timestamp from Parquet files."""
    logger.info(f"Scanning Parquet files for {stream} in gs://{bucket_name}/{stream}/")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    prefix = f"{stream}/"
    blobs = bucket.list_blobs(prefix=prefix)
    max_modified = None
    modified_field = "modified"
    
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            logger.info(f"Reading Parquet file: gs://{bucket_name}/{blob.name}")
            temp_file = f"/tmp/{blob.name.split('/')[-1]}"
            blob.download_to_filename(temp_file)
            try:
                df = pd.read_parquet(temp_file, engine='pyarrow')
                if modified_field in df.columns:
                    current_max = df[modified_field].max()
                    if pd.isna(current_max):
                        continue
                    if max_modified is None or current_max > max_modified:
                        max_modified = current_max
            except Exception as e:
                logger.warning(f"Error reading {blob.name}: {str(e)}")
            finally:
                import os
                if os.path.exists(temp_file):
                    os.remove(temp_file)
    
    return str(max_modified) if max_modified else None

def calculate_datetime_range(stream_config: dict) -> tuple[str, str]:
    """Calculate start_datetime and end_datetime based on load_type and frequency."""
    stream = stream_config["stream"]
    load_type = stream_config.get("load_type", "incremental")
    frequency = stream_config.get("frequency", "daily")
    lowerbound = stream_config.get("lowerbound", "")
    upperbound = stream_config.get("upperbound", "")

    now = datetime.utcnow()
    start_datetime = None
    end_datetime = None

    # Handle upperbound = "0" (current time)
    if upperbound == "0":
        end_datetime = now
    elif upperbound:
        try:
            end_datetime = datetime.strptime(upperbound, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            logger.warning(f"Invalid upperbound format for {stream}: {upperbound}")

    # Handle lowerbound = "-1" (last period) or valid datetime
    if lowerbound == "-1":
        if frequency == "daily":
            end_datetime = end_datetime or (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
            start_datetime = end_datetime - timedelta(days=1)
        elif frequency == "hourly":
            end_datetime = end_datetime or now.replace(minute=59, second=59, microsecond=0) - timedelta(hours=1)
            start_datetime = end_datetime - timedelta(hours=1)
        elif frequency == "weekly":
            end_datetime = end_datetime or (now - timedelta(days=now.weekday() + 1)).replace(hour=23, minute=59, second=59, microsecond=0)
            start_datetime = end_datetime - timedelta(days=6)
        elif frequency == "monthly":
            end_datetime = end_datetime or (now.replace(day=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
            start_datetime = end_datetime.replace(day=1, hour=0, minute=0, second=0)
        else:
            raise ValueError(f"Unsupported frequency for {stream}: {frequency}")
    elif lowerbound:
        try:
            start_datetime = datetime.strptime(lowerbound, "%Y-%m-%dT%H:%M:%SZ")
            end_datetime = end_datetime or datetime.strptime(upperbound, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            logger.warning(f"Invalid lowerbound/upperbound format for {stream}: {lowerbound}/{upperbound}")

    # If lowerbound is empty, use max modified from Parquet
    if not lowerbound and load_type == "incremental":
        max_modified = get_max_modified_from_parquet(BUCKET_NAME, stream)
        if max_modified:
            try:
                start_datetime = datetime.strptime(max_modified, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                logger.warning(f"Invalid max_modified format for {stream}: {max_modified}")

    # Fallback calculation if start_datetime or end_datetime are not set
    if not end_datetime:
        if frequency == "daily":
            end_datetime = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        elif frequency == "hourly":
            end_datetime = now.replace(minute=59, second=59, microsecond=0) - timedelta(hours=1)
        elif frequency == "weekly":
            end_datetime = (now - timedelta(days=now.weekday() + 1)).replace(hour=23, minute=59, second=59, microsecond=0)
        elif frequency == "monthly":
            end_datetime = (now.replace(day=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        else:
            raise ValueError(f"Unsupported frequency for {stream}: {frequency}")

    if not start_datetime:
        if frequency == "daily":
            start_datetime = end_datetime - timedelta(days=1)
        elif frequency == "hourly":
            start_datetime = end_datetime - timedelta(hours=1)
        elif frequency == "weekly":
            start_datetime = end_datetime - timedelta(days=6)
        elif frequency == "monthly":
            start_datetime = end_datetime.replace(day=1, hour=0, minute=0, second=0)
        else:
            raise ValueError(f"Unsupported frequency for {stream}: {frequency}")

    return (
        start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    )

@functions_framework.http
def read_streams_config(request):
    """HTTP Cloud Function to read streams config from BigQuery, save as JSON, and calculate datetime ranges."""
    try:
        # Extract parameters from request
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"status": "error", "message": "Missing JSON payload with frequency, load_type, and src_type"}, 400
        
        frequency = request_json.get("frequency")
        load_type = request_json.get("load_type")
        src_type = request_json.get("src_type")
        
        if not all([frequency, load_type, src_type]):
            return {"status": "error", "message": "Missing required parameters: frequency, load_type, src_type"}, 400

        streams_config = load_bigquery_config(PROJECT_NAME, BQ_DATASET, BQ_TABLE, frequency, load_type, src_type)
        if not streams_config:
            logger.error("No streams configured")
            return {"status": "error", "message": "No streams configured"}, 400

        # Save the BigQuery config to GCS as JSON
        try:
            save_config_to_json(BUCKET_NAME, JSON_CONFIG_PATH, streams_config)
        except Exception as e:
            logger.warning(f"Failed to save JSON config, continuing: {str(e)}")

        results = []
        for config in streams_config:
            stream = config.get("stream")
            if not stream:
                logger.error("Missing stream name in config")
                continue

            try:
                start_datetime, end_datetime = calculate_datetime_range(config)
                stream_data = {
                    "stream": stream,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                    "path": config.get("path"),
                    "destination": config.get("destination")
                }
                results.append(stream_data)
                logger.info(f"Calculated for {stream}: {start_datetime} to {end_datetime}")
            except Exception as e:
                logger.error(f"Error processing {stream}: {str(e)}")
                continue

        if not results:
            logger.error("No valid streams processed")
            return {"status": "error", "message": "No valid streams processed"}, 400

        # Save the processed streams config to GCS as JSON
        try:
            save_config_to_json(BUCKET_NAME, PROCESSED_JSON_CONFIG_PATH, results)
        except Exception as e:
            logger.warning(f"Failed to save processed JSON config, continuing: {str(e)}")

        return {"status": "success", "streams": results}, 200

    except Exception as e:
        logger.error(f"Error in read_streams_config: {str(e)}")
        return {"status": "error", "message": str(e)}, 500


# import functions_framework

# @functions_framework.http
# def hello_http(request):
#     """HTTP Cloud Function.
#     Args:
#         request (flask.Request): The request object.
#         <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
#     Returns:
#         The response text, or any set of values that can be turned into a
#         Response object using `make_response`
#         <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
#     """
#     request_json = request.get_json(silent=True)
#     request_args = request.args

#     if request_json and 'name' in request_json:
#         name = request_json['name']
#     elif request_args and 'name' in request_args:
#         name = request_args['name']
#     else:
#         name = 'World'
#     return 'Hello {}!'.format(name)
