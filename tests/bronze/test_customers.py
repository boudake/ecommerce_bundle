from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from src.bronze.customers import bronze_customers_stream


class TestBronzeCustomers:

    @classmethod
    def setup_class(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("test-bronze-customers")
            .getOrCreate()
        )

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    @patch("src.bronze.customers.spark")
    def test_bronze_customers_stream_reads_configured_path(self, mock_spark):

        # Mock configuration
        mock_spark.conf.get.return_value = (
            "/Volumes/ecommerce/raw/data"
        )

        # Mock DataFrame reader
        mock_reader = MagicMock()
        mock_spark.readStream = mock_reader

        (
            mock_reader
            .format.return_value
            .option.return_value
            .option.return_value
            .load.return_value
        )

        bronze_customers_stream()

        # Verify configuration was read
        mock_spark.conf.get.assert_called_once_with(
            "ecommerce.raw_path"
        )

        # Verify Auto Loader
        mock_reader.format.assert_called_once_with(
            "cloudFiles"
        )

        # Verify CSV
        mock_reader.format.return_value.option.assert_any_call(
            "cloudFiles.format",
            "csv"
        )

        # Verify source path
        mock_reader.format.return_value.option.return_value.option.return_value.load.assert_called_once_with(
            "/Volumes/ecommerce/raw/data/customers"
        )