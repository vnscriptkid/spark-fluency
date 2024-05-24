from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    
class Gold:
    def __init__(self):
        self.tradeSchema = StructType([
            StructField("CreatedTime", TimestampType(), True),
            StructField("Type", StringType(), True),
            StructField("Amount", DoubleType(), True),
            StructField("BrokerCode", StringType(), True)
        ])
    
    def read_stream(self):
        df = spark.readStream.format("delta").table("trade_kafka")

        # Map to trade struct
        parsed_df = df.select(
            col("key"),
            from_json(col("value"), self.tradeSchema).alias("value")
        ).select("value.*") \
        .withColumn("CreatedTime", expr("to_timestamp(CreatedTime, 'yyyy-MM-dd HH:mm:ss')")) \
        .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end")) \
        .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))
        
        return parsed_df
    
    def process_stream(self, df):
        from pyspark.sql.functions import sum, window
        
        aggregated_df = df.groupBy(
            window(col("CreatedTime"), "15 minutes")
        ).agg(
            sum("Buy").alias("TotalBuy"),
            sum("Sell").alias("TotalSell")
        ).select("window.start", "window.end", "TotalBuy", "TotalSell")

        return aggregated_df
    
    def write_stream(self, df):
        return df.writeStream \
            .queryName("trade-summary") \
            .option("checkpointLocation", f"/tmp/checkpoints/trade_summary") \
            .outputMode("complete") \
            .toTable("trade_summary")
    
    def run(self):
        df = self.read_stream()
        processed_df = self.process_stream(df)
        query = self.write_stream(processed_df)
        return query

# Drop existing tables
spark.sql("drop table if exists trade_kafka")
spark.sql("drop table if exists trade_summary")

# Delete existing checkpoint directory
checkpoint_dir = "/tmp/checkpoints/trade_summary"
dbutils.fs.rm(checkpoint_dir, True)

# Recreate the trade_kafka table
spark.sql("create table if not exists trade_kafka (key string, value string) using delta")

g = Gold()
print('Init')
goldQuery = g.run()
print('Ran gold processing')

