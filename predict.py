from pyspark.sql import SparkSession, functions
from pyspark.ml import PipelineModel
from datetime import datetime, timedelta
import sys
from pyspark.ml.feature import IndexToString

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary


def main(model):
    # main logic starts here
    model = PipelineModel.load(model)
    features = spark.createDataFrame(
        [
            (8, 3, 13, "Trump", "English", ""),
        ],
        ["hour", "dayofweek", "online_age", "candidate", "language", "sentiment"],
    )
    stringIndexerModel = model.stages[-6]

    predicted = model.transform(features)
    # Create an IndexToString transformer
    labelConverter = IndexToString(
        inputCol="prediction",
        outputCol="predictedLabel",
        labels=stringIndexerModel.labels,
    )

    # Convert numeric predictions back to original sentiment labels
    converted_predictions = labelConverter.transform(predicted)
    prediction = converted_predictions.select("predictedLabel").collect()[0][
        "predictedLabel"
    ]

    print("Predicted sentiment:", prediction)
    


if __name__ == "__main__":
    model = sys.argv[1]
    spark = SparkSession.builder.appName("weather tomorrow").getOrCreate()
    assert spark.version >= "3.0"  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    main(model)
