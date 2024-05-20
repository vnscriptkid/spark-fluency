from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

class DoublePipeline():
    def __init__(self, config):
        self.source_path = config["source_path"]
        self.destination_path = config["destination_path"]

    def read_data(self) -> DataFrame:
        """
        Reads CSV data from the given source path.
        """
        return spark.read.csv(self.source_path, header=True, inferSchema=True)

    def process_data(self, df: DataFrame) -> DataFrame:
        """
        Processes the data by adding a new column.
        """
        return df.withColumn("processed_column", col("existing_column") * 2)

    def write_data(self, df: DataFrame):
        """
        Writes the processed data to the given destination path.
        """
        df.write.mode("overwrite").option("header", "true").csv(self.destination_path)
    
    def run(self):
        """
        Runs the data processing pipeline.
        """
        df = self.read_data()
        processed_df = self.process_data(df)
        self.write_data(processed_df)

# Run the pipeline
if __name__ == "__main__":
    config = {
        "source_path": "/FileStore/tables/data*.csv",
        "destination_path": "/FileStore/tables/processed_data/"
    }
    pipeline = DoublePipeline(config)
    pipeline.run()
