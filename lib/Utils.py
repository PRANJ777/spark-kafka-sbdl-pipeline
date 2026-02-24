from pyspark.sql import SparkSession

def get_spark_session(env):

    spark = SparkSession.builder \
        .appName("SBDL_Project") \
        .master("local[*]") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        ) \
        .getOrCreate()

    return spark