# testing cloud run in gcp 
import json
import logging
import aiohttp
import pandas as pd
import functions_framework
import asyncio
import os
from datetime import datetime, timezone
from google.cloud import storage
import time
from dateutil.parser import parse as parse_date
import uuid
import traceback
from aiohttp_retry import RetryClient, ExponentialRetry



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

BUCKET_NAME = "pest_payrix"
ENV_CONFIG_PATH = "env_config.json"

def upload_debug_to_gcs(debug_info: list, run_id: str):
    """Upload debug info to GCS with run_id."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"debug/fetch_and_load_{run_id}.txt")
        blob.upload_from_string("\n".join(debug_info))
        logger.info(f"Uploaded debug to gs://{BUCKET_NAME}/{blob.name}")
    except Exception as e:
        logger.error(f"Failed to upload debug to GCS: {str(e)}")

def save_checkpoint(bucket_name: str, stream: str, page_number: int, run_id: str) -> None:
    """Save the last read page number to GCS."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"checkpoints/{stream}/{run_id}_checkpoint.json")
        checkpoint_data = {"last_page": page_number, "run_id": run_id}
        blob.upload_from_string(json.dumps(checkpoint_data))
        logger.info(f"Saved checkpoint for {stream} at page {page_number} to gs://{bucket_name}/{blob.name}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint for {stream}: {str(e)}")
        raise

def load_checkpoint(bucket_name: str, stream: str, run_id: str) -> int:
    """Load the last read page number from GCS."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"checkpoints/{stream}/{run_id}_checkpoint.json")
        if blob.exists():
            checkpoint_data = json.loads(blob.download_as_string())
            logger.info(f"Loaded checkpoint for {stream}: last_page={checkpoint_data['last_page']}")
            return checkpoint_data['last_page']
        return 1
    except Exception as e:
        logger.warning(f"No checkpoint found or error loading for {stream}: {str(e)}")
        return 1

def merge_parquet_files(bucket_name: str, temp_files: list, final_destination: str, stream: str) -> None:
    """Merge multiple Parquet files into a single Parquet file."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        dfs = []
        for temp_file in temp_files:
            blob = bucket.blob(temp_file)
            if blob.exists():
                with blob.open("rb") as f:
                    df = pd.read_parquet(f, engine='pyarrow')
                    dfs.append(df)
                blob.delete()  # Delete temporary file after reading
                logger.info(f"Read and deleted temporary file gs://{bucket_name}/{temp_file}")
            else:
                logger.warning(f"Temporary file gs://{bucket_name}/{temp_file} does not exist")
        
        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)
            final_df["load_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            blob = bucket.blob(final_destination)
            with blob.open("wb") as f:
                final_df.to_parquet(f, engine='pyarrow', index=False)
            if not bucket.get_blob(final_destination):
                logger.error(f"GCS write verification failed for {stream}: gs://{bucket_name}/{final_destination}")
                raise Exception(f"GCS write verification failed for {stream}")
            logger.info(f"Merged {len(dfs)} temporary files into gs://{bucket_name}/{final_destination} with {len(final_df)} records")
        else:
            logger.warning(f"No data to merge for {stream}")
    except Exception as e:
        logger.error(f"Error merging Parquet files for {stream}: {str(e)}")
        raise

def load_env_config(env_config_path: str) -> dict:
    """Load environment configuration (base_url and api_key)."""
    config = {}
    try:
        with open(env_config_path, 'r') as f:
            config = json.load(f)
            logger.info("Loaded env_config.json")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Error loading {env_config_path}: {str(e)}")
        config = {
            "base_url": os.getenv("BASE_URL", "https://api.payrix.com"),
            "api_key": os.getenv("API_KEY")
        }
        if not config["api_key"]:
            logger.error("API key missing")
            raise ValueError("API key missing")
    return config

def load_streams_config(streams_config_path: str) -> dict:
    """Load streams configuration from the specified path."""
    debug_info = [f"Attempting to load streams config from {streams_config_path}"]
    try:
        with open(streams_config_path, 'r') as f:
            streams_config = json.load(f)
            stream_to_path = streams_config.get("stream_to_path", {})
            if not stream_to_path:
                debug_info.append("No streams defined in stream_to_path")
                logger.error("No streams defined in stream_to_path")
                upload_debug_to_gcs(debug_info, str(uuid.uuid4()))
                raise ValueError("No streams defined in stream_to_path")
            logger.info(f"Loaded streams config from {streams_config_path}: {stream_to_path}")
            debug_info.append(f"Loaded streams config from {streams_config_path}: {stream_to_path}")
            upload_debug_to_gcs(debug_info, str(uuid.uuid4()))
            return stream_to_path
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading {streams_config_path}: {str(e)}")
        debug_info.append(f"Error loading {streams_config_path}: {str(e)}")
        upload_debug_to_gcs(debug_info, str(uuid.uuid4()))
        raise ValueError(f"Failed to load streams config: {str(e)}")

async def fetch_data(session: aiohttp.ClientSession, url: str, headers: dict, params: dict, start_datetime: str, end_datetime: str, stream: str, bucket_name: str, destination: str, run_id: str) -> list:
    """Fetch data from the API with pagination, incremental filtering, batch saving, and checkpointing."""
    all_data = []
    temp_files = []
    page_number = load_checkpoint(bucket_name, stream, run_id)
    page_limit = 100
    batch_size = 10  # Save to Parquet every 10 pages
    batch_data = []
    has_more = True
    debug_info = [f"Starting fetch for {stream} from {url} with params: {params}, starting from page {page_number}"]
    fetch_start_time = time.time()

    try:
        start_dt = parse_date(start_datetime).replace(tzinfo=timezone.utc)
        end_dt = parse_date(end_datetime).replace(tzinfo=timezone.utc)
    except ValueError as e:
        debug_info.append(f"Invalid date format for {stream}: {str(e)}")
        upload_debug_to_gcs(debug_info, run_id)
        raise ValueError(f"Invalid date format: {start_datetime}, {end_datetime}")

    # Configure retry client with exponential backoff
    retry_options = ExponentialRetry(attempts=3, start_timeout=1.0, factor=2.0, statuses={429, 500, 502, 503, 504})
    async with RetryClient(client_session=session, retry_options=retry_options) as retry_client:
        while has_more:
            request_params = params.copy()
            request_params["page[number]"] = page_number
            request_params["page[limit]"] = page_limit

            page_start_time = time.time()
            logger.info(f"Fetching page {page_number} for {stream} from {url} with params: {request_params}")
            debug_info.append(f"Fetching page {page_number} for {stream} with params: {request_params}")
            try:
                async with retry_client.get(url, headers=headers, params=request_params, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status >= 400:
                        error_text = await response.text()
                        logger.error(f"API error for {stream} on page {page_number}: {response.status} - {error_text}")
                        debug_info.append(f"API error for {stream} on page {page_number}: {response.status} - {error_text}")
                        upload_debug_to_gcs(debug_info, run_id)
                        raise Exception(f"API error: {response.status} - {error_text}")

                    data = await response.json()
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        logger.info(f"Rate limit hit for {stream} on page {page_number}, sleeping for {retry_after}s")
                        debug_info.append(f"Rate limit hit for {stream}, sleeping for {retry_after}s")
                        await asyncio.sleep(float(retry_after))

                    page_data = data.get("response", {}).get("data", [])
                    if not isinstance(page_data, list):
                        debug_info.append(f"Unexpected data format for {stream}: {page_data}")
                        logger.error(f"Unexpected data format for {stream} on page {page_number}: {page_data}")
                        upload_debug_to_gcs(debug_info, run_id)
                        raise Exception(f"Unexpected data format for {stream}: {page_data}")

                    filtered_data = []
                    for record in page_data:
                        try:
                            modified_str = record.get("modified")
                            if modified_str:
                                try:
                                    modified_dt = parse_date(modified_str).replace(tzinfo=timezone.utc)
                                    if start_dt <= modified_dt <= end_dt:
                                        filtered_data.append(record)
                                    else:
                                        debug_info.append(f"Record filtered out for {stream}: modified={modified_str} outside range {start_datetime} to {end_datetime}")
                                except ValueError:
                                    debug_info.append(f"Invalid modified date in record for {stream}: modified={modified_str}, record={record}")
                                    filtered_data.append(record)
                            else:
                                debug_info.append(f"Record missing modified field for {stream}: {record}")
                                filtered_data.append(record)
                        except Exception as e:
                            debug_info.append(f"Error processing record for {stream}: {str(e)}, record={record}")
                            filtered_data.append(record)

                    batch_data.extend(filtered_data)
                    all_data.extend(filtered_data)
                    logger.info(f"Page {page_number} for {stream}: Fetched {len(page_data)} records, Kept {len(filtered_data)} after filtering, Batch size: {len(batch_data)}, Total so far: {len(all_data)}")
                    debug_info.append(f"Page {page_number} for {stream}: Fetched {len(page_data)} records, Kept {len(filtered_data)} after filtering, Batch size: {len(batch_data)}, Total so far: {len(all_data)}")

                    # Save batch to Parquet every batch_size pages
                    if len(batch_data) > 0 and (page_number % batch_size == 0 or not has_more):
                        temp_destination = f"temp/{stream}/{run_id}_page_{page_number}.parquet"
                        save_to_parquet(batch_data, bucket_name, temp_destination, stream)
                        temp_files.append(temp_destination)
                        batch_data = []  # Clear batch after saving
                        logger.info(f"Saved batch for {stream} to gs://{bucket_name}/{temp_destination}")

                    # Save checkpoint
                    save_checkpoint(bucket_name, stream, page_number, run_id)

                    page_info = data.get("response", {}).get("page", {})
                    has_more = page_info.get("hasMore", False)
                    if not has_more and len(page_data) == page_limit:
                        logger.info(f"hasMore = False but full page returned for {stream}. Assuming more data.")
                        debug_info.append(f"hasMore = False but full page returned for {stream}. Assuming more data.")
                        has_more = True

                    page_duration = time.time() - page_start_time
                    logger.info(f"Page {page_number} fetch time for {stream}: {page_duration:.2f} seconds")
                    debug_info.append(f"Page {page_number} fetch time for {stream}: {page_duration:.2f} seconds")
                    page_number += 1

            except aiohttp.ClientError as e:
                error_msg = f"Client error fetching page {page_number} for {stream}: {str(e)}\n{traceback.format_exc()}"
                logger.error(error_msg)
                debug_info.append(error_msg)
                upload_debug_to_gcs(debug_info, run_id)
                raise
            except Exception as e:
                error_msg = f"Unexpected error fetching page {page_number} for {stream}: {str(e)}\n{traceback.format_exc()}"
                logger.error(error_msg)
                debug_info.append(error_msg)
                upload_debug_to_gcs(debug_info, run_id)
                raise

    # Save any remaining data in the batch
    if batch_data:
        temp_destination = f"temp/{stream}/{run_id}_page_{page_number}.parquet"
        save_to_parquet(batch_data, bucket_name, temp_destination, stream)
        temp_files.append(temp_destination)
        logger.info(f"Saved final batch for {stream} to gs://{bucket_name}/{temp_destination}")

    # Merge all temporary Parquet files into a single file
    if temp_files:
        merge_parquet_files(bucket_name, temp_files, destination, stream)

    fetch_duration = time.time() - fetch_start_time
    logger.info(f"Completed fetch for {stream} from {url}: Total records = {len(all_data)}, Total time: {fetch_duration:.2f} seconds")
    debug_info.append(f"Completed fetch for {stream}: Total records = {len(all_data)}, Total time: {fetch_duration:.2f} seconds")
    upload_debug_to_gcs(debug_info, run_id)
    return all_data

def save_to_parquet(data: list, bucket_name: str, destination: str, stream: str) -> None:
    """Save data to GCS as Parquet."""
    try:
        if not data:
            logger.warning(f"No data to save for {stream} to {destination}")
            return
        df = pd.DataFrame(data)
        df["load_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination)
        with blob.open("wb") as f:
            df.to_parquet(f, engine='pyarrow', index=False)
        if not bucket.get_blob(destination):
            logger.error(f"GCS write verification failed for {stream}: gs://{bucket_name}/{destination}")
            raise Exception(f"GCS write verification failed for {stream}")
        logger.info(f"Saved {len(data)} records for {stream} to gs://{bucket_name}/{destination}")
    except Exception as e:
        logger.error(f"Error saving to GCS for {stream}: {str(e)}")
        raise

@functions_framework.http
def fetch_and_load(request):
    """HTTP Cloud Function/Cloud Run handler."""
    run_id = str(uuid.uuid4())
    debug_info = [f"Raw request received: method={request.method}, headers={dict(request.headers)}, path={request.path}, run_id={run_id}"]
    logger.info(f"Raw request received: method={request.method}, headers={dict(request.headers)}, path={request.path}, run_id={run_id}")
    
    try:
        try:
            request_json = request.get_json(silent=False)
        except ValueError as e:
            logger.error(f"Failed to parse JSON: {str(e)}\n{traceback.format_exc()}")
            debug_info.append(f"Failed to parse JSON: {str(e)}\n{traceback.format_exc()}")
            upload_debug_to_gcs(debug_info, run_id)
            return {"status": "error", "message": f"Failed to parse JSON: {str(e)}"}, 400

        logger.info(f"Curl request started: {request_json}, run_id={run_id}")
        debug_info.append(f"Curl request started: {request_json}, run_id={run_id}")
        
        return asyncio.run(async_fetch_and_load(request, debug_info, run_id))
    
    except Exception as e:
        error_msg = f"Error: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        debug_info.append(error_msg)
        upload_debug_to_gcs(debug_info, run_id)
        return {"status": "error", "message": str(e)}, 500

async def async_fetch_and_load(request, debug_info: list, run_id: str):
    """Async handler for processing one or all streams."""
    try:
        request_json = request.get_json(silent=False)
        if not request_json:
            logger.error("Invalid request: JSON missing")
            debug_info.append("Invalid request: JSON missing")
            upload_debug_to_gcs(debug_info, run_id)
            return {"status": "error", "message": "Invalid request: JSON missing"}, 400

        stream = request_json.get("stream")
        path = request_json.get("path")
        start_datetime = request_json.get("start_datetime")
        end_datetime = request_json.get("end_datetime")
        destination = request_json.get("destination")
        streams_config_path = request_json.get("streams_config_path")

        logger.info(f"Loading env config")
        debug_info.append(f"Loading env config")
        env_config = load_env_config(ENV_CONFIG_PATH)

        # Load stream_to_path only if streams_config_path is provided and no path is given
        stream_to_path = {}
        if streams_config_path and not path:
            logger.info(f"Loading streams config from {streams_config_path}")
            debug_info.append(f"Loading streams config from {streams_config_path}")
            stream_to_path = load_streams_config(streams_config_path)
        elif not path and stream:
            logger.error(f"Path or streams_config_path required for stream: {stream}")
            debug_info.append(f"Path or streams_config_path required for stream: {stream}")
            upload_debug_to_gcs(debug_info, run_id)
            return {"status": "error", "message": f"Path or streams_config_path required for stream: {stream}"}, 400

        base_url = env_config.get("base_url", "https://api.payrix.com")
        api_key = env_config.get("api_key")
        if not api_key:
            logger.error("API key missing")
            debug_info.append("API key missing")
            upload_debug_to_gcs(debug_info, run_id)
            return {"status": "error", "message": "API key missing"}, 500

        results = []
        streams_to_process = [stream] if stream else stream_to_path.keys()

        if not streams_to_process:
            logger.error("No streams to process: stream or streams_config_path required")
            debug_info.append("No streams to process: stream or streams_config_path required")
            upload_debug_to_gcs(debug_info, run_id)
            return {"status": "error", "message": "No streams to process: stream or streams_config_path required"}, 400

        if not all([start_datetime, end_datetime, destination]):
            logger.error(f"Missing required fields: start_datetime, end_datetime, destination")
            debug_info.append(f"Missing required fields: start_datetime, end_datetime, destination")
            upload_debug_to_gcs(debug_info, run_id)
            return {"status": "error", "message": "Missing required fields: start_datetime, end_datetime, destination"}, 400

        try:
            parse_date(start_datetime)
            parse_date(end_datetime)
        except ValueError as e:
            logger.error(f"Invalid date format: {str(e)}\n{traceback.format_exc()}")
            debug_info.append(f"Invalid date format: {str(e)}\n{traceback.format_exc()}")
            upload_debug_to_gcs(debug_info, run_id)
            return {"status": "error", "message": f"Invalid date format: {str(e)}"}, 400

        try:
            start_dt = parse_date(start_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")
            end_dt = parse_date(end_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError as e:
            logger.error(f"Date parsing error: {str(e)}\n{traceback.format_exc()}")
            debug_info.append(f"Date parsing error: {str(e)}\n{traceback.format_exc()}")
            upload_debug_to_gcs(debug_info, run_id)
            return {"status": "error", "message": f"Date parsing error: {str(e)}"}, 400

        headers = {"APIKEY": api_key, "Content-Type": "application/json"}
        params = {"search": f"modified[greater]={start_dt}&modified[less]={end_dt}"}
        now = datetime.utcnow()
        async with aiohttp.ClientSession() as session:
            for stream in streams_to_process:
                stream_path = path or stream_to_path.get(stream)
                if not stream_path:
                    logger.error(f"Invalid or unsupported stream: {stream}")
                    debug_info.append(f"Invalid or unsupported stream: {stream}")
                    results.append({
                        "stream": stream,
                        "status": "error",
                        "message": f"Invalid or unsupported stream: {stream}"
                    })
                    continue

                url = f"{base_url.rstrip('/')}{stream_path}"
                logger.info(f"Fetching data for {stream} from {url} with params: {params}")
                debug_info.append(f"Fetching data for {stream} from {url} with params: {params}")

                try:
                    formatted_destination = destination.format(
                        year=now.year,
                        month=f"{now.month:02d}",
                        date=f"{now.day:02d}",
                        timestamp=now.strftime("%Y%m%d_%H%M%S"),
                        stream=stream
                    )
                    data = await fetch_data(session, url, headers, params, start_dt, end_dt, stream, BUCKET_NAME, formatted_destination, run_id)
                    logger.info(f"Fetched {len(data)} records for {stream}")
                    debug_info.append(f"Fetched {len(data)} records for {stream}")

                    results.append({
                        "stream": stream,
                        "status": "success",
                        "records_fetched": len(data),
                        "destination": formatted_destination
                    })

                except Exception as e:
                    error_msg = f"Error for {stream}: {str(e)}\n{traceback.format_exc()}"
                    logger.error(error_msg)
                    debug_info.append(error_msg)
                    results.append({
                        "stream": stream,
                        "status": "error",
                        "message": str(e)
                    })

        logger.info(f"Request completed for streams: {streams_to_process}")
        debug_info.append(f"Request completed for streams: {streams_to_process}")
        upload_debug_to_gcs(debug_info, run_id)

        status = "success" if all(r["status"] == "success" for r in results) else "partial_success" if any(r["status"] == "success" for r in results) else "error"
        return {"status": status, "results": results}, 200 if status != "error" else 500

    except Exception as e:
        error_msg = f"Error: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        debug_info.append(error_msg)
        upload_debug_to_gcs(debug_info, run_id)
        return {"status": "error", "message": str(e)}, 500





# import json
# import logging
# import functions_framework
# import pandas as pd
# from google.cloud import storage
# from google.cloud import bigquery
# from datetime import datetime, timedelta
# from dateutil.relativedelta import relativedelta
# import os

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
# logger = logging.getLogger(__name__)
# PROJECT_NAME = "powerful-lore-431718-e4"
# BUCKET_NAME = "proforce_payrix"
# BQ_DATASET = "pest_payrix"
# BQ_TABLE = "config_table"
# JSON_CONFIG_PATH = "config/streams_config.json"
# PROCESSED_JSON_CONFIG_PATH = "config/processed_streams_config.json"

# def load_bigquery_config(project_name: str, dataset: str, table: str) -> list:
#     """Load streams configuration from BigQuery."""
#     try:
#         bq_client = bigquery.Client()
#         query = f"SELECT * FROM `{project_name}.{dataset}.{table}`"
#         query_job = bq_client.query(query)
#         results = query_job.result()
#         config = [dict(row) for row in results]
#         if not config:
#             raise ValueError(f"No data found in {dataset}.{table}")
#         return config
#     except Exception as e:
#         logger.error(f"Error loading config from BigQuery: {str(e)}")
#         raise

# def save_config_to_json(bucket_name: str, file_path: str, config: list) -> None:
#     """Save configuration to GCS as JSON."""
#     try:
#         storage_client = storage.Client()
#         bucket = storage_client.bucket(bucket_name)
#         blob = bucket.blob(file_path)
#         blob.upload_from_string(json.dumps(config, indent=2), content_type='application/json')
#         logger.info(f"Saved config to gs://{bucket_name}/{file_path}")
#     except Exception as e:
#         logger.error(f"Error saving config to gs://{bucket_name}/{file_path}: {str(e)}")
#         raise

# def get_max_modified_from_parquet(bucket_name: str, stream: str) -> str:
#     """Get max modified timestamp from Parquet files."""
#     logger.info(f"Scanning Parquet files for {stream} in gs://{bucket_name}/{stream}/")
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     prefix = f"{stream}/"
#     blobs = bucket.list_blobs(prefix=prefix)
#     max_modified = None
#     modified_field = "modified"
    
#     for blob in blobs:
#         if blob.name.endswith(".parquet"):
#             logger.info(f"Reading Parquet file: gs://{bucket_name}/{blob.name}")
#             temp_file = f"/tmp/{blob.name.split('/')[-1]}"
#             blob.download_to_filename(temp_file)
#             try:
#                 df = pd.read_parquet(temp_file, engine='pyarrow')
#                 if modified_field in df.columns:
#                     current_max = df[modified_field].max()
#                     if pd.isna(current_max):
#                         continue
#                     if max_modified is None or current_max > max_modified:
#                         max_modified = current_max
#             except Exception as e:
#                 logger.warning(f"Error reading {blob.name}: {str(e)}")
#             finally:
#                 import os
#                 if os.path.exists(temp_file):
#                     os.remove(temp_file)
    
#     return str(max_modified) if max_modified else None

# def calculate_datetime_range(stream_config: dict) -> tuple[str, str]:
#     """Calculate start_datetime and end_datetime based on load_type and frequency."""
#     stream = stream_config["stream"]
#     load_type = stream_config.get("load_type", "incremental")
#     frequency = stream_config.get("frequency", "daily")
#     lowerbound = stream_config.get("lowerbound", "")
#     upperbound = stream_config.get("upperbound", "")

#     now = datetime.utcnow()
#     start_datetime = None
#     end_datetime = None

#     # Handle upperbound = "0" (current time)
#     if upperbound == "0":
#         end_datetime = now
#     elif upperbound:
#         try:
#             end_datetime = datetime.strptime(upperbound, "%Y-%m-%dT%H:%M:%SZ")
#         except ValueError:
#             logger.warning(f"Invalid upperbound format for {stream}: {upperbound}")

#     # Handle lowerbound = "-1" (last period) or valid datetime
#     if lowerbound == "-1":
#         if frequency == "daily":
#             end_datetime = end_datetime or (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
#             start_datetime = end_datetime - timedelta(days=1)
#         elif frequency == "hourly":
#             end_datetime = end_datetime or now.replace(minute=59, second=59, microsecond=0) - timedelta(hours=1)
#             start_datetime = end_datetime - timedelta(hours=1)
#         elif frequency == "weekly":
#             end_datetime = end_datetime or (now - timedelta(days=now.weekday() + 1)).replace(hour=23, minute=59, second=59, microsecond=0)
#             start_datetime = end_datetime - timedelta(days=6)
#         elif frequency == "monthly":
#             end_datetime = end_datetime or (now.replace(day=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
#             start_datetime = end_datetime.replace(day=1, hour=0, minute=0, second=0)
#         else:
#             raise ValueError(f"Unsupported frequency for {stream}: {frequency}")
#     elif lowerbound:
#         try:
#             start_datetime = datetime.strptime(lowerbound, "%Y-%m-%dT%H:%M:%SZ")
#             end_datetime = end_datetime or datetime.strptime(upperbound, "%Y-%m-%dT%H:%M:%SZ")
#         except ValueError:
#             logger.warning(f"Invalid lowerbound/upperbound format for {stream}: {lowerbound}/{upperbound}")

#     # If lowerbound is empty, use max modified from Parquet
#     if not lowerbound and load_type == "incremental":
#         max_modified = get_max_modified_from_parquet(BUCKET_NAME, stream)
#         if max_modified:
#             try:
#                 start_datetime = datetime.strptime(max_modified, "%Y-%m-%d %H:%M:%S.%f")
#             except ValueError:
#                 logger.warning(f"Invalid max_modified format for {stream}: {max_modified}")

#     # Fallback calculation if start_datetime or end_datetime are not set
#     if not end_datetime:
#         if frequency == "daily":
#             end_datetime = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
#         elif frequency == "hourly":
#             end_datetime = now.replace(minute=59, second=59, microsecond=0) - timedelta(hours=1)
#         elif frequency == "weekly":
#             end_datetime = (now - timedelta(days=now.weekday() + 1)).replace(hour=23, minute=59, second=59, microsecond=0)
#         elif frequency == "monthly":
#             end_datetime = (now.replace(day=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
#         else:
#             raise ValueError(f"Unsupported frequency for {stream}: {frequency}")

#     if not start_datetime:
#         if frequency == "daily":
#             start_datetime = end_datetime - timedelta(days=1)
#         elif frequency == "hourly":
#             start_datetime = end_datetime - timedelta(hours=1)
#         elif frequency == "weekly":
#             start_datetime = end_datetime - timedelta(days=6)
#         elif frequency == "monthly":
#             start_datetime = end_datetime.replace(day=1, hour=0, minute=0, second=0)
#         else:
#             raise ValueError(f"Unsupported frequency for {stream}: {frequency}")

#     return (
#         start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
#         end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
#     )

# @functions_framework.http
# def read_streams_config(request):
#     """HTTP Cloud Function to read streams config from BigQuery, save as JSON, and calculate datetime ranges."""
#     try:
#         streams_config = load_bigquery_config(PROJECT_NAME, BQ_DATASET, BQ_TABLE)
#         if not streams_config:
#             logger.error("No streams configured")
#             return {"status": "error", "message": "No streams configured"}, 400

#         # Save the BigQuery config to GCS as JSON
#         try:
#             save_config_to_json(BUCKET_NAME, JSON_CONFIG_PATH, streams_config)
#         except Exception as e:
#             logger.warning(f"Failed to save JSON config, continuing: {str(e)}")

#         results = []
#         for config in streams_config:
#             stream = config.get("stream")
#             if not stream:
#                 logger.error("Missing stream name in config")
#                 continue

#             try:
#                 start_datetime, end_datetime = calculate_datetime_range(config)
#                 stream_data = {
#                     "stream": stream,
#                     "start_datetime": start_datetime,
#                     "end_datetime": end_datetime,
#                     "path": config.get("path"),
#                     "destination": config.get("destination")
#                 }
#                 results.append(stream_data)
#                 logger.info(f"Calculated for {stream}: {start_datetime} to {end_datetime}")
#             except Exception as e:
#                 logger.error(f"Error processing {stream}: {str(e)}")
#                 continue

#         if not results:
#             logger.error("No valid streams processed")
#             return {"status": "error", "message": "No valid streams processed"}, 400

#         # Save the processed streams config to GCS as JSON
#         try:
#             save_config_to_json(BUCKET_NAME, PROCESSED_JSON_CONFIG_PATH, results)
#         except Exception as e:
#             logger.warning(f"Failed to save processed JSON config, continuing: {str(e)}")

#         return {"status": "success", "streams": results}, 200

#     except Exception as e:
#         logger.error(f"Error in read_streams_config: {str(e)}")
#         return {"status": "error", "message": str(e)}, 500
