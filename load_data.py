import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql.functions import (
    to_timestamp,
    dayofweek,
    hour,
    months_between,
    date_format,
)
from pyspark.sql import SparkSession, functions, types
import re

election_schema = types.StructType(
    [
        types.StructField("created_at", types.StringType()),
        types.StructField("tweet_id", types.StringType()),
        types.StructField("tweet", types.StringType()),
        types.StructField("likes", types.StringType()),
        types.StructField("retweet_count", types.StringType()),
        types.StructField("source", types.StringType()),
        types.StructField("user_id", types.StringType()),
        types.StructField("user_name", types.StringType()),
        types.StructField("user_screen_name", types.StringType()),
        types.StructField("user_description", types.StringType()),
        types.StructField("user_join_date", types.StringType()),
        types.StructField("user_followers_count", types.StringType()),
        types.StructField("user_location", types.StringType()),
        types.StructField("lat", types.FloatType()),
        types.StructField("long", types.FloatType()),
        types.StructField("city", types.StringType()),
        types.StructField("country", types.StringType()),
        types.StructField("continent", types.StringType()),
        types.StructField("state", types.StringType()),
        types.StructField("state_code", types.StringType()),
        types.StructField("collected_at", types.StringType()),
    ]
)
state_boundaries = {
    "Alabama": (30.20, 35.01, -88.47, -84.89),
    "Alaska": (51.22, 71.50, -179.15, 129.99),
    "Arizona": (31.33, 37.00, -114.82, -109.04),
    "Arkansas": (33.00, 36.50, -94.62, -89.64),
    "California": (32.53, 42.01, -124.48, -114.13),
    "Colorado": (36.99, 41.00, -109.06, -102.04),
    "Connecticut": (40.98, 42.03, -73.73, -71.79),
    "Delaware": (38.45, 39.84, -75.79, -75.04),
    "Florida": (24.55, 31.00, -87.63, -80.03),
    "Georgia": (30.36, 35.00, -85.61, -80.84),
    "Hawaii": (18.91, 28.40, -178.44, -154.81),
    "Idaho": (42.00, 49.00, -117.24, -111.04),
    "Illinois": (36.97, 42.50, -91.51, -87.50),
    "Indiana": (37.77, 41.76, -88.10, -84.78),
    "Iowa": (40.37, 43.50, -96.64, -90.14),
    "Kansas": (36.99, 40.00, -102.05, -94.59),
    "Kentucky": (36.49, 39.15, -89.57, -81.96),
    "Louisiana": (28.92, 33.02, -94.04, -89.02),
    "Maine": (43.05, 47.46, -71.08, -66.95),
    "Maryland": (37.91, 39.72, -79.49, -75.05),
    "Massachusetts": (41.24, 42.88, -73.50, -69.93),
    "Michigan": (41.70, 48.31, -90.42, -82.13),
    "Minnesota": (43.50, 49.38, -97.24, -89.49),
    "Mississippi": (30.20, 34.99, -91.65, -88.09),
    "Missouri": (35.99, 40.61, -95.77, -89.10),
    "Montana": (44.36, 49.00, -116.05, -104.04),
    "Nebraska": (40.00, 43.00, -104.06, -95.31),
    "Nevada": (35.00, 42.00, -120.00, -114.04),
    "New Hampshire": (42.70, 45.30, -72.56, -70.71),
    "New Jersey": (38.92, 41.36, -75.56, -73.90),
    "New Mexico": (31.33, 37.00, -109.05, -103.00),
    "New York": (40.50, 45.01, -79.76, -71.85),
    "North Carolina": (33.84, 36.59, -84.32, -75.46),
    "North Dakota": (45.93, 49.00, -104.05, -96.55),
    "Ohio": (38.40, 42.33, -84.82, -80.52),
    "Oklahoma": (33.62, 37.00, -103.00, -94.43),
    "Oregon": (41.99, 46.29, -124.57, -116.47),
    "Pennsylvania": (39.72, 42.27, -80.52, -74.70),
    "Rhode Island": (41.14, 42.02, -71.86, -71.12),
    "South Carolina": (32.04, 35.21, -83.35, -78.56),
    "South Dakota": (42.48, 45.94, -104.06, -96.44),
    "Tennessee": (34.98, 36.68, -90.31, -81.65),
    "Texas": (25.84, 36.50, -106.65, -93.51),
    "Utah": (36.99, 42.00, -114.06, -109.04),
    "Vermont": (42.73, 45.01, -73.44, -71.50),
    "Virginia": (36.54, 39.46, -83.68, -75.24),
    "Washington": (45.54, 49.00, -124.79, -116.92),
    "West Virginia": (37.20, 40.64, -82.65, -77.72),
    "Wisconsin": (42.49, 47.08, -92.89, -86.25),
    "Wyoming": (40.99, 45.01, -111.05, -104.06),
    "Washington D.C.": (38.79, 38.995, -77.12, -76.91),
}


@functions.udf(returnType=types.StringType())
def phrase_date(line):
    match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2})", line)
    if match:
        time_part = match.group(1)
        return time_part
    else:
        return "Wrong Format"


@functions.udf(returnType=types.StringType())
def get_state(lat, lon):
    for state, (min_lat, max_lat, min_lon, max_lon) in state_boundaries.items():
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            return state
    return "Unknown Region"


# add more functions as necessary
@functions.udf(returnType=types.StringType())
def candidates(filename):
    if "donaldtrump" in filename:
        return "Trump"
    elif "joebiden" in filename:
        return "Biden"
    else:
        return "Wrong File"


def main():
    # main logic starts here
    data = (
        spark.read.csv("archive", schema=election_schema)
        .withColumn("filename", functions.input_file_name())
        .select(
            "created_at",
            "user_join_date",
            "lat",
            "long",
            "filename",
            "tweet",
        )
    )
    data = (
        data.filter(
            ~data["lat"].isNull()
            & ~data["long"].isNull()
            & ~data["created_at"].isNull()
        )
        .withColumn("candidate", candidates(data["filename"]))
        .withColumn("state", get_state(data["lat"], data["long"]))
        .withColumn("time", phrase_date(data["created_at"]))
        .withColumn("join_time", phrase_date(data["user_join_date"]))
    )
    data = (
        data.filter(
            (data["time"] != "Wrong Format")
            & (data["join_time"] != "Wrong Format")
            & (data["state"] != "Unknown Region")
            & (data["candidate"] != "Wrong File")
        )
        .withColumn("timestamp", to_timestamp(data["time"]))
        .withColumn("join_timestamp", to_timestamp(data["join_time"]))
    )

    data = (
        data.withColumn("date", date_format("timestamp", "yyyy-MM-dd"))
        .withColumn("dayofweek", dayofweek(data["timestamp"]))
        .withColumn("hour", hour(data["timestamp"]))
        .withColumn("online_age", months_between("timestamp", "join_timestamp"))
        .select(
            "date",
            "dayofweek",
            "hour",
            "candidate",
            "online_age",
            "state",
            "tweet",
        )
    )
    data.repartition("state").write.csv("cleaned", mode="overwrite", header=False)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("read election data").getOrCreate()
    assert spark.version >= "3.0"  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main()
