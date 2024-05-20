from pyspark.sql.functions import col

source_path = "/FileStore/tables/data*.csv"
destination_path = "/FileStore/tables/processed_data/"

df = spark.read.csv(source_path, header=True, inferSchema=True)

processed_df = df.withColumn("processed_column", col("existing_column") * 2)

processed_df.write.mode("overwrite").option("header", "true").csv(destination_path)