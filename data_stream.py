import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', TimestampType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True),
])


def run_spark_job(spark):

    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9097') \
        .option('subscribe', 'police.calls') \
        .option('startingOffsets', 'earliest') \
        .option('maxOffsetsPerTrigger', 200) \
        .option('maxRatePerPartition', 10) \
        .option('stopGracefullyOnShutdown', "true") \
        .load()

    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    distinct_table = service_table\
        .select('original_crime_type_name', 'disposition', 'call_date_time') \
        .distinct() \
        .withWatermark('call_date_time', "1 minute")

    agg_df = distinct_table\
        .dropna() \
        .select('original_crime_type_name') \
        .groupby('original_crime_type_name') \
        .agg({'original_crime_type_name': 'count'}) \
        .orderBy('count(original_crime_type_name)')

    query = agg_df \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query.awaitTermination()

    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    radio_code_df = radio_code_df.withColumnRenamed(
        "disposition_code", "disposition")

    join_query = agg_df\
        .join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .queryName("join") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .config("spark.ui.port", 3000) \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
