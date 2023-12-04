import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col
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
        + "state_analyze"
        + " (State TEXT, TrumpPositive INT, TrumpNeutral INT, TrumpNegative INT, BidenPositive INT, BidenNeutral INT, BidenNegative INT, Total INT, PRIMARY KEY(State));"
    )
    session.shutdown()
    # main logic starts here
    data = spark.read.csv("language-version", header=False, schema=clean_schema)
    data = data.groupBy("state").agg(
        functions.sum(
            functions.when(
                (data["candidate"] == "Trump") & (data["sentiment"] == "Positive"), 1
            ).otherwise(0)
        ).alias("trumppositive"),
        functions.sum(
            functions.when(
                (data["candidate"] == "Trump") & (data["sentiment"] == "Neutral"), 1
            ).otherwise(0)
        ).alias("trumpneutral"),
        functions.sum(
            functions.when(
                (data["candidate"] == "Trump") & (data["sentiment"] == "Negative"), 1
            ).otherwise(0)
        ).alias("trumpnegative"),
        functions.sum(
            functions.when(
                (data["candidate"] == "Biden") & (data["sentiment"] == "Positive"), 1
            ).otherwise(0)
        ).alias("bidenpositive"),
        functions.sum(
            functions.when(
                (data["candidate"] == "Biden") & (data["sentiment"] == "Neutral"), 1
            ).otherwise(0)
        ).alias("bidenneutral"),
        functions.sum(
            functions.when(
                (data["candidate"] == "Biden") & (data["sentiment"] == "Negative"), 1
            ).otherwise(0)
        ).alias("bidennegative"),
    )
    data = data.withColumn(
        "total",
        col("trumppositive")
        + col("trumpneutral")
        + col("trumpnegative")
        + col("bidenpositive")
        + col("bidenneutral")
        + col("bidennegative"),
    ).sort(functions.desc("total"))
    data.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="state_analyze", keyspace="final"
    ).save()


if __name__ == "__main__":
    with open("credentials.json", "r") as file:
        credentials = json.load(file)
    main(credentials)
