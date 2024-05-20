destination_path = "/FileStore/tables/processed_data/*.csv"

df = spark.read.csv(destination_path, header=True, inferSchema=True)

display(df)