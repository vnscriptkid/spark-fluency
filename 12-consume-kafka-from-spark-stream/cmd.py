from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class Processor:
    def __init__(self):
        self.BOOTSTRAP_SERVERS="pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092"
        self.SECURITY_PROTOCOL="SASL_SSL"
        self.SASL_MECHANISMS="PLAIN"
        self.JAAS_MODULE="org.apache.kafka.common.security.plain.PlainLoginModule"
        self.SASL_USERNAME="xyz"
        self.SASL_PASSWORD="xyz"

        self.invoiceSchema = StructType([
            StructField("invoice_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("invoice_date", StringType(), True),
            StructField("due_date", StringType(), True)
        ])

    def process_batch_stream(self, df, epoch_id):
        # Process the streaming DataFrame
        print(f"Batch {epoch_id}")
        processed_df = df.select(
            df.key.cast("string").alias("key"),
            from_json(df.value.cast("string").alias("value"), self.invoiceSchema),
            "topic",
            "timestamp"
        )

        # Display the processed DataFrame
        processed_df.show()

    def process_stream(self):
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS) \
            .option("kafka.security.protocol", self.SECURITY_PROTOCOL) \
            .option("kafka.sasl.mechanism", self.SASL_MECHANISMS) \
            .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.SASL_USERNAME}' password='{self.SASL_PASSWORD}';") \
            .option("startingOffsets", "earliest") \
            .option("subscribe", "invoices") \
            .load()

        # Process and display the streaming DataFrame
        processed_df = df.select(
            df.key.cast("string").alias("key"),
            from_json(df.value.cast("string").alias("value"), self.invoiceSchema).alias("value"),
            "topic",
            "timestamp"
        )

        # Display the processed DataFrame
        return processed_df.writeStream \
            .queryName("invoice-ingestion") \
            .option("checkpointLocation", f"/tmp/checkpoints/invoices") \
            .outputMode("append") \
            .toTable("invoice_bz")

    def process_batch(self):
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS) \
            .option("kafka.security.protocol", self.SECURITY_PROTOCOL) \
            .option("kafka.sasl.mechanism", self.SASL_MECHANISMS) \
            .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.SASL_USERNAME}' password='{self.SASL_PASSWORD}';") \
            .option("subscribe", "invoices") \
            .load()

        # Process and display the streaming DataFrame
        processed_df = df.select(
            df.key.cast("string").alias("key"),
            from_json(df.value.cast("string").alias("value"), self.invoiceSchema).alias("value"),
            "topic",
            "timestamp"
        )

        display(processed_df)


p = Processor()
query = p.process_stream()
query.awaitTermination()