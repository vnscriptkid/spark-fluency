FROM bitnami/spark:3.3.0

USER root

RUN install_packages openjdk-11-jdk

# Set Spark packages environment variable to include Kafka connector
ENV SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1"
ENV PYSPARK_SUBMIT_ARGS="--packages ${SPARK_PACKAGES} pyspark-shell"

USER 1001

# Start Spark master by default
CMD ["/opt/bitnami/scripts/spark/entrypoint.sh", "/opt/bitnami/scripts/spark/run.sh", "start-spark"]
