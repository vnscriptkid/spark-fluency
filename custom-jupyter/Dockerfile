FROM jupyter/all-spark-notebook:latest

RUN conda install -c conda-forge openjdk=11

RUN $SPARK_HOME/bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 -i /dev/null

# Clean up to reduce image size
RUN conda clean --all --yes

# Set Spark packages environment variable to include Kafka connector
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 pyspark-shell"

CMD ["start-notebook.sh"]
