# %run ./main

from pyspark.sql import Row

class TestDoubleProcessingPipeline:
    def __init__(self, config):
        self.config = config
        self.pipeline = DoublePipeline(config)
        self.base_path = "/tmp/test_data/"

    def cleanup_data(self, path):
        """
        Cleanup data in the specified path.
        """
        dbutils.fs.rm(path, recurse=True)

    def test_read_data(self):
        """
        Test reading data.
        """
        self.cleanup_data(self.config['source_path'])
        data = [("1", 10), ("2", 20)]
        columns = ["id", "existing_column"]
        df = spark.createDataFrame(data, columns)

        df.write.mode("overwrite").option("header", "true").csv(self.base_path)
        
        read_df = self.pipeline.read_data()
        assert read_df.count() == 2
        

    def test_process_data(self):
        """
        Test processing data.
        """
        data = [Row(id=1, existing_column=10),
                Row(id=2, existing_column=20),
                Row(id=3, existing_column=30)]
        df = spark.createDataFrame(data)
        
        processed_df = self.pipeline.process_data(df)
        
        expected_data = [Row(id=1, existing_column=10, processed_column=20),
                         Row(id=2, existing_column=20, processed_column=40),
                         Row(id=3, existing_column=30, processed_column=60)]
        expected_df = spark.createDataFrame(expected_data)
        
        assert processed_df.collect() == expected_df.collect()

    def run_tests(self):
        self.test_read_data()
        self.test_process_data()
        self.cleanup_data(self.base_path)

# Configuration for testing
test_config = {
    "source_path": "/tmp/test_data/*.csv",
    "destination_path": "/tmp/test_processed_data/"
}

# Run tests
test_pipeline = TestDoubleProcessingPipeline(test_config)
test_pipeline.run_tests()

# Cleanup after tests

