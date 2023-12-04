import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
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
        + "hour"
        + " (Hour INT, Trump INT, Biden INT, PRIMARY KEY(Hour));"
    )
    session.shutdown()

    data = spark.read.csv("cleaned", header=False, schema=clean_schema)
    data = (
        data.groupBy("hour")
        .agg(
            functions.sum(
                functions.when(data["candidate"] == "Trump", 1).otherwise(0)
            ).alias("trump"),
            functions.sum(
                functions.when(data["candidate"] == "Biden", 1).otherwise(0)
            ).alias("biden"),
        )
        .sort("hour")
    )
    data.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="hour", keyspace="final"
    ).save()


if __name__ == "__main__":
    with open("credentials.json", "r") as file:
        credentials = json.load(file)
    main(credentials)
