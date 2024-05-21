# CMD 1
BOOTSTRAP_SERVERS="pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092"
SECURITY_PROTOCOL="SASL_SSL"
SASL_MECHANISMS="PLAIN"
JAAS_MODULE="org.apache.kafka.common.security.plain.PlainLoginModule"
SASL_USERNAME="xyz"
SASL_PASSWORD="xyz"

# CMD 2
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("kafka.security.protocol", SECURITY_PROTOCOL) \
    .option("kafka.sasl.mechanism", SASL_MECHANISMS) \
    .option("kafka.sasl.jaas.config", f"{JAAS_MODULE} required username='{SASL_USERNAME}' password='{SASL_PASSWORD}';") \
    .option("subscribe", "invoices") \
    .load()

# CMD 3
display(df)