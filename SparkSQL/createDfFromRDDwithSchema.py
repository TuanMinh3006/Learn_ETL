import random

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,StringType,IntegerType

spark=SparkSession.builder\
    .appName("MinhDz") \
    .master('local[*]') \
    .config("spark.executor.memory","4g") \
    .getOrCreate()



rdd=spark.sparkContext.parallelize(
    Row(name="minh",
        age=22)
)

schema=StructType(
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)

)

















"""
Pa
"""