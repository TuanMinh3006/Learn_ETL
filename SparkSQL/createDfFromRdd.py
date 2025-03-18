import random

from pyspark.sql import SparkSession

spark=SparkSession.builder\
    .appName("MinhDz") \
    .master('local[*]') \
    .config("spark.executor.memory","4g") \
    .getOrCreate()

rdd= spark.sparkContext.parallelize(range(1,11)) \
    .map(lambda x: (x,random.randint(0,99)))

print(rdd.collect())
df = spark.createDataFrame(rdd, ['Key', 'Value'])
df.show()