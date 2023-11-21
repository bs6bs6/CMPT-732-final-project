import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from geopy.geocoders import Nominatim
from pyspark.sql import SparkSession, functions, types
import time

election_schema = types.StructType(
    [
        types.StructField("created_at", types.StringType()),
        types.StructField("tweet_id", types.StringType()),
        types.StructField("tweet", types.StringType()),
        types.StructField("likes", types.IntegerType()),
        types.StructField("retweet_count", types.IntegerType()),
        types.StructField("source", types.StringType()),
        types.StructField("user_id", types.StringType()),
        types.StructField("user_name", types.StringType()),
        types.StructField("user_screen_name", types.StringType()),
        types.StructField("user_description", types.StringType()),
        types.StructField("user_join_date", types.StringType()),
        types.StructField("user_followers_count", types.IntegerType()),
        types.StructField("user_location", types.IntegerType()),
        types.StructField("lat", types.StringType()),
        types.StructField("long", types.StringType()),
        types.StructField("city", types.StringType()),
        types.StructField("country", types.StringType()),
        types.StructField("continent", types.IntegerType()),
        types.StructField("state", types.IntegerType()),
        types.StructField("state_code", types.StringType()),
        types.StructField("collected_at", types.StringType()),
    ]
)


@functions.udf(returnType=types.StringType())
def get_country(lat, lon):
    geolocator = Nominatim(user_agent="Country Locator")
    try:
        location = geolocator.reverse((lat, lon), exactly_one=True)
        country = location.raw["address"].get("country", "Unknown")
        return country
    except:
        return "Error"
    finally:
        time.sleep(1)  # To avoid hitting rate limits


# add more functions as necessary
@functions.udf(returnType=types.StringType())
def candidates(filename):
    if "donaldtrump" in filename:
        return "Trump"
    elif "joebiden" in filename:
        return "Biden"
    else:
        return "Unknown"


def main(inputs, output):
    # main logic starts here
    data = (
        spark.read.csv(inputs, schema=election_schema)
        .withColumn("filename", functions.input_file_name())
        .select(
            "created_at",
            "likes",
            "retweet_count",
            "user_join_date",
            "user_followers_count",
            "lat",
            "long",
            "collected_at",
            "filename",
            # "tweet"
        )
        .cache()
    )
    data = (
        data.withColumn("candidate", candidates(data["filename"]))
        .withColumn("Country", get_country(data["lat"], data["long"]))
        .drop("filename")
        .repartition(16)
        .cache()
    )
    # data = data[data["Country"] == "United States"]
    data.write.csv(output, mode="overwrite")
    # data.show()


if __name__ == "__main__":
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName("read election data").getOrCreate()
    assert spark.version >= "3.0"  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(inputs, output)
