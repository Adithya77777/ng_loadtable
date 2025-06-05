# testing ci/cd

import json
import logging
import functions_framework
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re
import os
from config import PROJECT_NAME, BUCKET_NAME_PAYRIX, BUCKET_NAME_BRAINTREE, BQ_DATASET, BQ_TABLE, JSON_CONFIG_PATH, PROCESSED_JSON_CONFIG_PATH

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def load_bigquery_config(project_name: str, dataset: str, table: str, frequency: str) -> list:
    """Load streams configuration from BigQuery with filters for frequency."""
    try:
        bq_client = bigquery.Client()
        query = f"""
            SELECT * 
            FROM `{project_name}.{dataset}.{table}`
            WHERE LOWER(frequency) = LOWER(@frequency)
            AND is_active = 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("frequency", "STRING", frequency)
            ]
        )
        logger.debug(f"Executing BigQuery: {query} with frequency={frequency}")
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        config = [dict(row) for row in results]
        if not config:
            raise ValueError(f"No data found in {dataset}.{table} for frequency={frequency}")
        logger.info(f"Loaded {len(config)} streams for frequency={frequency}: {config}")
        return config
    except Exception as e:
        logger.error(f"Error loading config from BigQuery: {str(e)}")
        raise

def save_config_to_json(bucket_name: str, file_path: str, config: list | dict) -> None:
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

def get_max_modified_from_parquet(bucket_name: str, stream: str, src_type: str, region: str) -> str:
    """Get max timestamp from the latest Parquet file in the stream's folder structure."""
    logger.info(f"Scanning Parquet files for {stream} (src_type: {src_type}, region: {region}) in gs://{bucket_name}/")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Define prefix based on src_type and region
    if src_type.lower() == "braintree":
        prefix = f"{region.lower()}/transactions/"
    else:  # Assume payrix
        prefix = f"{region.lower()}/{stream}/"
    
    # Regex to extract timestamp from filename like transactions_20250603_071943.parquet
    timestamp_pattern = re.compile(r'_(\d{8})_(\d{6})\.parquet$')
    latest_blob = None
    latest_timestamp = None

    # Find the latest Parquet file
    blobs = bucket.list_blobs(prefix=prefix)
    blob_list = list(blobs)  # Convert to list for logging
    logger.debug(f"Checking prefix: {prefix}")
    logger.debug(f"Found {len(blob_list)} blobs in {prefix}: {[b.name for b in blob_list]}")
    for blob in blob_list:
        if blob.name.endswith(".parquet"):
            match = timestamp_pattern.search(blob.name)
            if match:
                file_timestamp = match.group(1) + match.group(2)  # e.g., 20250603_071943
                try:
                    timestamp_dt = datetime.strptime(file_timestamp, "%Y%m%d%H%M%S")
                    if latest_timestamp is None or timestamp_dt > latest_timestamp:
                        latest_timestamp = timestamp_dt
                        latest_blob = blob
                except ValueError:
                    logger.warning(f"Invalid timestamp in filename: {blob.name}")
                    continue

    if not latest_blob:
        logger.warning(f"No valid Parquet files found for {stream} in {prefix}")
        return None

    # Read the latest Parquet file
    logger.info(f"Reading latest Parquet file: gs://{bucket_name}/{latest_blob.name}")
    temp_file = f"/tmp/{latest_blob.name.split('/')[-1]}"
    try:
        latest_blob.download_to_filename(temp_file)
        df = pd.read_parquet(temp_file, engine='pyarrow')
        logger.debug(f"Parquet columns: {df.columns.tolist()}")
        # Use 'createdAt' for braintree, 'modified' for payrix
        timestamp_field = "createdAt" if src_type.lower() == "braintree" else "modified"
        if timestamp_field in df.columns:
            max_timestamp = df[timestamp_field].max()
            if pd.isna(max_timestamp):
                logger.warning(f"No valid timestamps in {timestamp_field} column of {latest_blob.name}")
                return None
            # Convert timestamp to string, handling both formats
            if isinstance(max_timestamp, str) and max_timestamp.endswith("Z"):
                return max_timestamp  # Already in ISO 8601 format
            return str(max_timestamp)  # Convert to string if not already
        else:
            logger.warning(f"Column '{timestamp_field}' not found in {latest_blob.name}")
            return None
    except Exception as e:
        logger.warning(f"Error reading {latest_blob.name}: {str(e)}")
        return None
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)

def calculate_max_modified_for_streams(streams_config: list) -> dict:
    """Calculate max modified timestamp for all streams and return as a dictionary."""
    max_modified_results = {}
    now = datetime.utcnow()
    for config in streams_config:
        stream = config.get("stream")
        if not stream:
            logger.error("Missing stream name in config")
            continue
        region = config.get("region", "").lower()
        src_type = config.get("src_type", "payrix").lower()
        frequency = config.get("frequency", "hourly").lower()
        bucket_name = BUCKET_NAME_BRAINTREE if src_type == "braintree" else BUCKET_NAME_PAYRIX

        try:
            max_modified = get_max_modified_from_parquet(bucket_name, stream, src_type, region)
            if not max_modified:
                # Fallback to 70 minutes ago for hourly frequency
                max_modified = (now - timedelta(minutes=70)).strftime("%Y-%m-%d %H:%M:%S.%f")
                logger.info(f"No valid Parquet files or timestamps for {stream} (region: {region}, src_type: {src_type}), using fallback: {max_modified}")
            
            # Use unique key to handle same stream in different regions
            max_modified_results[f"{stream}_{region}"] = {
                "stream": stream,
                "max_modified": max_modified,
                "region": region.capitalize()
            }
            logger.info(f"Max modified for {stream} (region: {region}, src_type: {src_type}): {max_modified}")
        except Exception as e:
            logger.error(f"Error processing {stream} (region: {region}, src_type: {src_type}): {e}")
            # Include stream with fallback timestamp
            max_modified = (now - timedelta(minutes=70)).strftime("%Y-%m-%d %H:%M:%S.%f")
            max_modified_results[f"{stream}_{region}"] = {
                "stream": stream,
                "max_modified": max_modified,
                "region": region.capitalize()
            }
            logger.info(f"Using fallback for {stream} (region: {region}, src_type: {src_type}): {max_modified}")

    logger.info(f"Processed {len(max_modified_results)} streams")
    return max_modified_results

def calculate_datetime_range(stream_config: dict) -> tuple[str, str]:
    """Calculate start_datetime and end_datetime based on load_type and frequency."""
    stream = stream_config.get("stream", "")
    if not stream:
        raise ValueError("Missing stream name")

    logger.debug(f"Processing stream config: {stream_config}")
    
    load_type = stream_config.get("load_type", "inc").lower()
    frequency = stream_config.get("frequency", "daily").lower()
    lower_bound = stream_config.get("lower_bound", "0")
    upper_bound = stream_config.get("upper_bound", "0")
    src_type = stream_config.get("src_type", "payrix").lower()
    region = stream_config.get("region", "").lower()
    bucket_name = BUCKET_NAME_BRAINTREE if src_type == "braintree" else BUCKET_NAME_PAYRIX

    now = datetime.utcnow()

    # Full Load: return empty strings
    if load_type == "full":
        logger.info(f"Full load for {stream}, returning empty datetimes")
        return ("", "")

    # Incremental Load
    if load_type == "inc":
        start_datetime = None
        end_datetime = now

        # Try to get max_modified from Parquet
        max_modified = get_max_modified_from_parquet(bucket_name, stream, src_type, region)
        if max_modified:
            try:
                if max_modified.endswith("Z"):
                    start_datetime = datetime.strptime(max_modified, "%Y-%m-%dT%H:%M:%S.%fZ")
                else:
                    start_datetime = datetime.strptime(max_modified, "%Y-%m-%d %H:%M:%S.%f")
                logger.debug(f"Using max_modified from Parquet for {stream}: {max_modified}")
            except ValueError:
                logger.warning(f"Invalid max_modified format from Parquet for {stream}: {max_modified}")

        # If Parquet fails, try to get max_modified from config_{frequency}.json
        if not start_datetime:
            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(BUCKET_NAME_PAYRIX)
                blob = bucket.blob(f"config/config_{frequency}.json")
                if blob.exists():
                    config_data = json.loads(blob.download_as_string())
                    for config in config_data:
                        if config.get("stream") == stream and config.get("region").lower() == region:
                            max_modified = config.get("max_modified")
                            try:
                                # Handle both ISO 8601 (Z) and standard datetime formats
                                if max_modified.endswith("Z"):
                                    start_datetime = datetime.strptime(max_modified, "%Y-%m-%dT%H:%M:%S.%fZ")
                                else:
                                    start_datetime = datetime.strptime(max_modified, "%Y-%m-%d %H:%M:%S.%f")
                                logger.debug(f"Using max_modified from config_{frequency}.json for {stream}: {max_modified}")
                            except ValueError:
                                logger.warning(f"Invalid max_modified format in config_{frequency}.json for {stream}: {max_modified}")
                            break
                else:
                    logger.warning(f"config_{frequency}.json not found in gs://{BUCKET_NAME_PAYRIX}/config/")
            except Exception as e:
                logger.warning(f"Error reading config_{frequency}.json: {str(e)}")

        # Fallback if start_datetime is still not set
        if not start_datetime:
            logger.debug(f"Using fallback datetime for {stream} with frequency {frequency}")
            if frequency == "hourly":
                start_datetime = now - timedelta(minutes=70)
                start_datetime = start_datetime.replace(minute=0, second=0, microsecond=0)
            elif frequency == "daily":
                start_datetime = now - timedelta(days=1)
                start_datetime = start_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
            elif frequency == "weekly":
                start_datetime = now - timedelta(days=7)
                days_to_monday = start_datetime.weekday()
                start_datetime = start_datetime - timedelta(days=days_to_monday)
                start_datetime = start_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
            elif frequency == "monthly":
                start_datetime = now - relativedelta(months=1)
                start_datetime = start_datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            else:
                logger.error(f"Unsupported frequency for {stream}: {frequency}")
                raise ValueError(f"Unsupported frequency for {stream}: {frequency}")

        logger.info(f"Calculated datetime for {stream}: {start_datetime} to {end_datetime}")
        return (
            start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        )

    raise ValueError(f"Unsupported load_type for {stream}: {load_type}")

@functions_framework.http
def read_streams_config(request):
    """HTTP Cloud Function to read streams config from BigQuery, save as JSON, calculate datetime ranges, and save max modified timestamps."""
    try:
        # Extract parameters from request
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"status": "error", "message": "Missing JSON payload with frequency"}, 400

        frequency = request_json.get("frequency")
        if not frequency:
            return {"status": "error", "message": "Missing required parameters: frequency"}, 400

        # Debug endpoint to return raw config
        if request_json.get("debug_config"):
            streams_config = load_bigquery_config(PROJECT_NAME, BQ_DATASET, BQ_TABLE, frequency)
            return {"status": "debug", "streams_config": streams_config}, 200

        streams_config = load_bigquery_config(PROJECT_NAME, BQ_DATASET, BQ_TABLE, frequency)
        if not streams_config:
            logger.error("No streams configured")
            return {"status": "error", "message": "No streams configured"}, 400

        # Save the BigQuery config to GCS as JSON
        try:
            save_config_to_json(BUCKET_NAME_PAYRIX, JSON_CONFIG_PATH, streams_config)
        except Exception as e:
            logger.warning(f"Failed to save JSON config, continuing: {str(e)}")

        # Calculate max modified timestamps for all streams and save to config_{frequency}.json
        try:
            # Load all streams from config_table without frequency filter
            bq_client = bigquery.Client()
            query = f"""
                SELECT * 
                FROM `{PROJECT_NAME}.{BQ_DATASET}.{BQ_TABLE}`
            """
            logger.debug(f"Executing BigQuery for all streams: {query}")
            query_job = bq_client.query(query)
            all_streams_config = [dict(row) for row in query_job.result()]
            
            max_modified_results = calculate_max_modified_for_streams(all_streams_config)
            if max_modified_results:
                max_modified_file_path = f"config/config_{frequency}.json"
                save_config_to_json(BUCKET_NAME_PAYRIX, max_modified_file_path, list(max_modified_results.values()))
                logger.info(f"Saved max modified config to gs://{BUCKET_NAME_PAYRIX}/{max_modified_file_path}")
            else:
                logger.warning(f"No max modified timestamps calculated for any streams")
        except Exception as e:
            logger.warning(f"Failed to save max modified timestamps, continuing: {str(e)}")

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
                    "context_path": config.get("context_path"),
                    "destination": config.get("destination"),
                    "region": config.get("region"),
                    "frequency": config.get("frequency"),
                    "src_type": config.get("src_type"),
                    "load_type": config.get("load_type"),
                    "lower_bound": config.get("lower_bound"),
                    "upper_bound": config.get("upper_bound")
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
            save_config_to_json(BUCKET_NAME_PAYRIX, PROCESSED_JSON_CONFIG_PATH, results)
        except Exception as e:
            logger.warning(f"Failed to save processed JSON config, continuing: {str(e)}")

        return {"status": "success", "streams": results}, 200

    except Exception as e:
        logger.error(f"Error in read_streams_config: {str(e)}")
        return {"status": "error", "message": str(e)}, 500

