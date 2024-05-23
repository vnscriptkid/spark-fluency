from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class Bronze:
    def __init__(self):
        self.BOOTSTRAP_SERVERS="pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092"
        self.SECURITY_PROTOCOL="SASL_SSL"
        self.SASL_MECHANISMS="PLAIN"
        self.JAAS_MODULE="org.apache.kafka.common.security.plain.PlainLoginModule"
        self.SASL_USERNAME="xxx"
        self.SASL_PASSWORD="xxx"

        self.invoiceSchema = StructType([
            StructField("invoice_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("invoice_date", StringType(), True),
            StructField("due_date", StringType(), True)
        ])

    def read_stream(self):
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS) \
            .option("kafka.security.protocol", self.SECURITY_PROTOCOL) \
            .option("kafka.sasl.mechanism", self.SASL_MECHANISMS) \
            .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.SASL_USERNAME}' password='{self.SASL_PASSWORD}';") \
            .option("startingOffsets", "earliest") \
            .option("subscribe", "invoices") \
            .load()
        return df

    def process_stream(self, df):
        processed_df = df.select(
            df.key.cast("string").alias("key"),
            from_json(df.value.cast("string").alias("value"), self.invoiceSchema).alias("value"),
            "topic",
            "timestamp"
        )
        return processed_df
    
    def write_stream(self, df):
        return df.writeStream \
            .queryName("invoice-ingestion") \
            .option("checkpointLocation", f"/tmp/checkpoints/invoices") \
            .outputMode("append") \
            .toTable("invoice_bz")
    
    def run(self):
        df = self.read_stream()
        processed_df = self.process_stream(df)
        query = self.write_stream(processed_df)
        return query
    
class Gold:
    def read_stream(self):
        df = spark.readStream \
            .table("invoice_bz")
        return df
    
    # Aggregate amount by customer_id
    def process_stream(self, df):
        from pyspark.sql.functions import sum
        
        return df.groupBy("value.customer_id") \
            .agg(sum("value.amount").alias("total_amount"))
    
    def write_stream(self, df):
        return df.writeStream \
            .queryName("invoice-aggregation") \
            .option("checkpointLocation", f"/tmp/checkpoints/invoice_aggregation") \
            .outputMode("complete") \
            .toTable("invoice_gl")
    
    def run(self):
        df = self.read_stream()
        processed_df = self.process_stream(df)
        query = self.write_stream(processed_df)
        return query

b = Bronze()
g = Gold()
print('Init')
bronzeQuery = b.run()
print('Ran bronze processing')
goldQuery = g.run()
print('Ran gold processing')

