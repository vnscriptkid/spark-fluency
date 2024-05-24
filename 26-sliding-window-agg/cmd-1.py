from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    
class Gold:
    def __init__(self):
        self.sensorSchema = StructType([
            StructField("CreatedTime", TimestampType(), True),
            StructField("Reading", DoubleType(), True),
        ])
    
    def read_stream(self):
        df = spark.readStream.format("delta").table("sensor_kafka")

        # Map to sensor struct
        parsed_df = df.select(
            col("key").alias("ID"),
            from_json(col("value"), self.sensorSchema).alias("value")
        ).select("ID", "value.*")
        
        return parsed_df
    
    def process_stream(self, df):
        from pyspark.sql.functions import max, window
        
        # watermark: How long to wait before considering a record as late and dropping it
        aggregated_df = df \
        .withWatermark("CreatedTime", "30 minutes") \
        .groupBy(
            # col, window size, slide size
            window(col("CreatedTime"), "15 minutes", "5 minutes")
        ).agg(
            max("Reading").alias("MaxReading"),
        ).select("window.start", "window.end", "MaxReading")

        return aggregated_df
    
    def write_stream(self, df):
        return df.writeStream \
            .queryName("sensor-summary") \
            .option("checkpointLocation", f"/tmp/checkpoints/sensor_summary") \
            .outputMode("complete") \
            .toTable("sensor_summary")
    
    def run(self):
        df = self.read_stream()
        processed_df = self.process_stream(df)
        query = self.write_stream(processed_df)
        return query

# Drop existing tables
spark.sql("drop table if exists sensor_kafka")
spark.sql("drop table if exists sensor_summary")

# Delete existing checkpoint directory
checkpoint_dir = "/tmp/checkpoints/sensor_summary"
dbutils.fs.rm(checkpoint_dir, True)

# Recreate the sensor_kafka table
spark.sql("create table if not exists sensor_kafka (key string, value string) using delta")

g = Gold()
print('Init')
goldQuery = g.run()
print('Ran gold processing')

