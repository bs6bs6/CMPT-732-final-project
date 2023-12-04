import sys
from pyspark.sql.window import Window

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from datetime import datetime, timedelta
import json
from db_connect import create_session

clean_schema = types.StructType(
    [
        types.StructField("date", types.StringType()),
        types.StructField("dayofweek", types.IntegerType()),
        types.StructField("hour", types.IntegerType()),
        types.StructField("candidate", types.StringType()),
        types.StructField("online_age", types.StringType()),
        types.StructField("state", types.StringType()),
        types.StructField("tweet", types.StringType()),
        types.StructField("sentiment", types.StringType()),
        types.StructField("language", types.StringType()),
    ]
)


def main(credentials):
    # main logic starts here

    spark = (
        SparkSession.builder.appName("Cassandra Integration")
        .config("spark.cassandra.connection.host", "cassandra.us-west-2.amazonaws.com")
        .config("spark.cassandra.connection.port", "9142")
        .config("spark.cassandra.input.consistency.level", "LOCAL_QUORUM")
        .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
        .config("spark.cassandra.auth.username", "shiqi-732-at-437956307664")
        .config("spark.cassandra.auth.password", credentials["password"])
        .config("spark.cassandra.connection.ssl.enabled", "true")
        .config("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
        .config(
            "spark.cassandra.connection.ssl.trustStore.path",
            "./resources/cassandra_truststore.jks",
        )
        .config("spark.cassandra.connection.ssl.trustStore.password", "123456")
        .getOrCreate()
    )
    session = create_session()
    session.set_keyspace("final")
    session.execute(
        "CREATE TABLE IF NOT EXISTS "
        + "date_cumulative"
        + " (Date TEXT, Language TEXT, Cumulative INT, PRIMARY KEY(Language, Date));"
    )
    session.shutdown()

    data = spark.read.csv("language-version", header=False, schema=clean_schema)
    data = (
        data.groupBy("date", "language")
        .agg(functions.count("tweet").alias("count"))
        .sort("date")
    ).cache()
    start_date = datetime(2020, 10, 15)
    end_date = datetime(2020, 11, 8)
    date_list = [
        (start_date + timedelta(days=x)).strftime("%Y-%m-%d")
        for x in range((end_date - start_date).days + 1)
    ]
    dates = spark.createDataFrame(date_list, "string").withColumnRenamed(
        "value", "date"
    )

    languages = data.select("language").distinct().cache()

    # Create All Combinations of Dates and Languages
    combinations = dates.crossJoin(languages).cache()

    # Join with Original Data
    combined = (
        combinations.join(data, ["date", "language"], "left_outer").fillna(0).cache()
    )

    # Define the Window Specification
    windowSpec = (
        Window.partitionBy("language")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Calculate Cumulative Sum
    cumulative = combined.withColumn(
        "cumulative", functions.sum("count").over(windowSpec)
    ).select("date", "language", "cumulative")

    cumulative.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="date_cumulative", keyspace="final"
    ).save()


if __name__ == "__main__":
    with open("credentials.json", "r") as file:
        credentials = json.load(file)
    main(credentials)
