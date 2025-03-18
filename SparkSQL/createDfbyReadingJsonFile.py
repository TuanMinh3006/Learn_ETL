
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, LongType

spark=SparkSession.builder\
    .appName("MinhDz") \
    .master('local[*]') \
    .config("spark.executor.memory","4g") \
    .getOrCreate()


jsonFile=spark.read.json("path")
jsonFile.show() #spark doc file json linh tinh thu tu schema,=> tao schema theo y m


jsonSchema=StructType([
    StructField('id', StringType(), True),
    StructField("type",StringType(), True),
    StructField("actor", StructType([
        StructField('id', LongType(), True),
        StructField('login', StringType(), True),
        StructField('gravatar_id', StringType(), True),
        StructField('url', StringType(), True),
        StructField('url', StringType(), True),
    ]), True),
    StructField("paylod",StructType([
        StructField('action', StringType(), True),
        StructField('issue', StructType([
            StructField('url', StringType(), True),
            StructField('labels_url', StringType(), True),
        ]), True),

    ]),True)
])
jsonFile2=spark.read.schema(jsonSchema).json('path')
jsonFile2.select(
    'id',
    "type",
    "actor.id",
    "actor.login"
).show(truncate=False)
