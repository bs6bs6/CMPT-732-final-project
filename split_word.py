import os
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')


def main():
    print("splitting^_^")
    df = spark.read.csv('partial_dataset/hashtag_donaldtrump_sample.csv', header=True).select('tweet')
    words = df.rdd.flatMap(lambda row: row[0].split(" ") if row[0] is not None else [])
    word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False)

    path = os.path.join('split_word_output')
    if os.path.exists(path):
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))
    os.rmdir(path)
    
    word_counts.saveAsTextFile('split_word_output')

if __name__ == '__main__':
    main()