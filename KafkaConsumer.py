from confluent_kafka import Consumer, Producer, KafkaError
import json
from pyspark.sql import SparkSession, functions
from pyspark.ml import PipelineModel
from datetime import datetime, timedelta
import sys
from pyspark.ml.feature import IndexToString

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# Kafka 消费者配置
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'CMPT_732',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['election-predict-request'])

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def produce_message(topic, message):
    message_bytes = json.dumps(message).encode('utf-8')
    producer.produce(topic, message_bytes)
    producer.flush()


def main(model):
    # 持续监听 Topic A 的消息
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
    
        
         # main logic starts here
        try:
            message_str = msg.value().decode('utf-8')
            message_json = json.loads(message_str)
            print(message_json)
            features = spark.createDataFrame(
                [
                    (message_json["hour"], message_json["dayofweek"], message_json["online_age"], message_json["candidate"], message_json["language"], ""),
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
            # 提取uuid并创建预测结果
            response = {"uuid": message_json["uuid"], "prediction": prediction}
            print(prediction)

            produce_message('election-predict-response', response)
        except KeyError as e:
                print(f"Data format error: Missing key {e}. Message skipped.")
                continue  # 跳过当前消息，继续下一条消息

        except Exception as e:
                print(f"An error occurred: {e}. Message skipped.")
                continue  # 跳过当前消息，继续下一条消息

    # 关闭消费者连接
    consumer.close()
if __name__ == "__main__":
    model = sys.argv[1]
    spark = SparkSession.builder.appName("weather tomorrow").getOrCreate()
    assert spark.version >= "3.0"  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    model = PipelineModel.load(model)
    main(model)

