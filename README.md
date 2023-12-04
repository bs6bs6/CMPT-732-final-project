### Project website: http://54.213.254.255/


### Before running the python files, you have to download our source data from https://www.kaggle.com/datasets/manchunhui/us-election-2020-tweets, name the file "archive" and put it under the db_connect folder.

## Part 1: data analyse
#### 1. load_data.py
How to run it:
```
time spark-submit load_data.py
```

#### 2. state_analyze.py
How to run it: 
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 state_analyze.py
```
#### 3. hourly_analyze.py
How to run it: 
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 hourly_analyze.py
```
#### 4. detect_language.py
How to run it: 
```
time spark-submit detect_language.py 
```
 (this file may take around 30 mins, be careful)
#### 5. language_analyze.py
How to run it: 
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 language_analyze.py
```

#### 6. time_language_analyze.py
How to run it: 
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 time_language_analyze.py
```

#### 7. state_sentiment.py
How to run it: 
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 state_sentiment.py
```

#### 8 . sentiment_train.py
How to run it: 
```
time spark-submit sentiment_train.py
```

#### 9. predict.py
How to run it: 
```
time spark-submit predict.py sentiment_model 
```

## Part 2: Backend and Frontend deployment

#### 1. Prerequisites

run kafka container from image:

```cmd
docker-compose -f docker-compose-kafka.yml -p final-project-kafka up -d
```

create kafka topic:

##### (1) get into docker container:

```
docker exec -it my-kafka /bin/bash
```

##### (2) create kafka topics:

```
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic election-predict-request
```

```
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic election-predict-response
```



#### 2. run back-end

```
pip install -r requirements.txt
```

```
spark-submit KafkaConsumer.py sentiment_model_2
```

```
nohup & java -jar back-end-1.0-SNAPSHOT.jar
```

#### 3. run front-end

```
cd frontend
```

```
npm install
```

```
npm start
```
Then you can visit http://localhost:3000 to check our demo.


