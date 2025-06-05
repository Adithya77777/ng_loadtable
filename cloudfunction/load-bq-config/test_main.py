import pytest
from unittest.mock import patch, MagicMock
from main import load_bigquery_config, save_config_to_json, get_max_modified_from_parquet, calculate_max_modified_for_streams

@pytest.fixture
def mock_bq_rows():
    return [
        {
            "stream": "test_stream",
            "region": "us",
            "src_type": "payrix",
            "frequency": "hourly",
            "load_type": "inc",
            "lower_bound": "0",
            "upper_bound": "0",
            "context_path": "/test/path",
            "destination": "test_dest",
            "is_active": 1
        }
    ]

def test_load_bigquery_config_returns_expected_data(mock_bq_rows):
    with patch("main.bigquery.Client") as MockClient:
        mock_client = MockClient.return_value
        mock_query_job = MagicMock()
        # Return the dicts directly, as load_bigquery_config expects iterable of dict-like rows
        mock_query_job.result.return_value = mock_bq_rows
        mock_client.query.return_value = mock_query_job

        result = load_bigquery_config(
            project_name="mock_project",
            dataset="mock_dataset",
            table="mock_table",
            frequency="hourly"
        )
        print(result)  # For debugging purposes
        assert result == mock_bq_rows

def test_save_config_to_json():
    with patch("main.storage.Client") as MockStorageClient:
        mock_client = MockStorageClient.return_value
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket

        config = {"foo": "bar"}
        # Should not raise any exceptions
        save_config_to_json("mock_bucket", "mock_path.json", config)
        mock_client.bucket.assert_called_with("mock_bucket")
        mock_bucket.blob.assert_called_with("mock_path.json")
        mock_blob.upload_from_string.assert_called()

def test_get_max_modified_from_parquet():
    with patch("main.storage.Client") as MockStorageClient, \
         patch("main.pd.read_parquet") as mock_read_parquet:
        mock_client = MockStorageClient.return_value
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_blob.exists.return_value = True
        # Mock parquet data
        mock_df = MagicMock()
        mock_df["modified"].max.return_value = "2024-06-01T00:00:00Z"
        mock_read_parquet.return_value = mock_df

        result = get_max_modified_from_parquet("mock_bucket", "test_stream", "payrix", "us")
        assert result == "2024-06-01T00:00:00Z"

def test_calculate_max_modified_for_streams():
    streams_config = [
        {"stream": "s1", "region": "us", "src_type": "payrix"},
        {"stream": "s2", "region": "us", "src_type": "payrix"},
    ]
    with patch("main.get_max_modified_from_parquet", side_effect=["2024-06-01T00:00:00Z", "2024-06-02T00:00:00Z"]):
        result = calculate_max_modified_for_streams(streams_config)
        assert result == {
            "s1": "2024-06-01T00:00:00Z",
            "s2": "2024-06-02T00:00:00Z"
        }
if __name__ == "__main__":
    from unittest.mock import patch, MagicMock

    class MockRequest:
        def __init__(self, json_data):
            self._json = json_data
        def get_json(self, silent=False):
            return self._json

    # Mock BigQuery and Storage clients
    with patch("main.bigquery.Client") as MockBQClient, \
         patch("main.storage.Client") as MockStorageClient:
        # Setup BigQuery mock
        mock_bq_client = MockBQClient.return_value
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            {
                "stream": "test_stream",
                "region": "us",
                "src_type": "payrix",
                "frequency": "hourly",
                "load_type": "inc",
                "lower_bound": "0",
                "upper_bound": "0",
                "context_path": "/test/path",
                "destination": "test_dest",
                "is_active": 1
            }
        ]
        mock_bq_client.query.return_value = mock_query_job

        # Setup Storage mock (no-op for upload/download)
        mock_storage_client = MockStorageClient.return_value
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.bucket.return_value = mock_bucket
        mock_blob.exists.return_value = False  # Simulate config file not found

        mock_request = MockRequest({
            "frequency": "hourly",
            "debug_config": True
        })
        response = read_streams_config(mock_request)
        print(response)