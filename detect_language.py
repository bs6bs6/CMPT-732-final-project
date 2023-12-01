import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from langdetect import detect
from textblob import TextBlob

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
language_map = {
    "af": "Afrikaans",
    "ar": "Arabic",
    "bg": "Bulgarian",
    "bn": "Bengali",
    "ca": "Catalan",
    "cs": "Czech",
    "cy": "Welsh",
    "da": "Danish",
    "de": "German",
    "el": "Greek",
    "en": "English",
    "es": "Spanish",
    "et": "Estonian",
    "fa": "Persian",
    "fi": "Finnish",
    "fr": "French",
    "he": "Hebrew",
    "hi": "Hindi",
    "hr": "Croatian",
    "hu": "Hungarian",
    "id": "Indonesian",
    "it": "Italian",
    "ja": "Japanese",
    "ko": "Korean",
    "lt": "Lithuanian",
    "lv": "Latvian",
    "nl": "Dutch",
    "no": "Norwegian",
    "pa": "Punjabi",
    "pl": "Polish",
    "pt": "Portuguese",
    "ro": "Romanian",
    "ru": "Russian",
    "sk": "Slovak",
    "sl": "Slovenian",
    "so": "Somali",
    "sq": "Albanian",
    "sv": "Swedish",
    "sw": "Swahili",
    "ta": "Tamil",
    "te": "Telugu",
    "th": "Thai",
    "tl": "Tagalog",
    "tr": "Turkish",
    "uk": "Ukrainian",
    "ur": "Urdu",
    "vi": "Vietnamese",
    "zh-cn": "Chinese (Simplified)",
    "zh-tw": "Chinese (Traditional)",
}


@functions.udf(returnType=types.StringType())
def detect_language(text):
    try:
        lang_code = detect(text)
        return language_map.get(lang_code, "Unknown Language")
    except:
        return "Unknown Language"


@functions.udf(returnType=types.StringType())
def phrase_label(tweet):
    analysis = TextBlob(tweet)
    if analysis.sentiment.polarity > 0:
        return "Positive"
    elif analysis.sentiment.polarity == 0:
        return "Neutral"
    else:
        return "Negative"


def main():
    # main logic starts here
    data = spark.read.csv("cleaned", header=False, schema=clean_schema)

    data = data.withColumn("sentiment", phrase_label(data["tweet"])).withColumn(
        "language", detect_language(data["tweet"])
    )
    data = data[data["language"] != "Unknown Language"]

    data.repartition("language").write.csv(
        "language-version", mode="overwrite", header=False
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("analyze by time").getOrCreate()
    assert spark.version >= "3.0"  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main()
