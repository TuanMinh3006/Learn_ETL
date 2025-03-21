from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from pyspark.sql.types import TimestampType,ArrayType,StructType, StructField, StringType, BooleanType, LongType, IntegerType

spark=(SparkSession.builder \
    .appName("minhdx") \
    .master('local[*]') \
    .config("spark.executor.memory","4g")\
    .getOrCreate())

#jsonFile.show()

schemaJson=StructType([
    StructField("id", StringType(), True),
    StructField("type",StringType(),True),
    StructField("actor",StructType([StructField("id",StringType(),True),
                                    StructField("login",StringType(),True),
                                    StructField("gravatar_id",StringType(),True),
                                    StructField("url",StringType(),True),
                                    StructField("avatar_url",StringType(),True)
                                    ]),True),
    StructField("repo", StructType([
        StructField("id",StringType(),True),
        StructField("name",StringType(),True),
        StructField("url",StringType(),True),
    ]), True),
    StructField("payload", StructType([
        StructField("action", StringType(), True),
        StructField("issue", StructType([
            StructField("url",StringType(),True),
            StructField("label_url",StringType(),True),
            StructField("comments_url",StringType(),True),
            StructField("events_url",StringType(),True),
            StructField("html_url",StringType(),True),
            StructField("id",StringType(),True),
            StructField("number",LongType(),True),
            StructField("Title",StringType(),True),
            StructField("user",StructType([
                StructField("login",StringType(),True),
                StructField("id", StringType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gist_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscription_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("event_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", StringType(), True)
            ]),True),
            StructField("labels",ArrayType(StructType([
                StructField("url",StringType(),True),
                StructField("name",StringType(),True),
                StructField("color",StringType(),True)
            ])),True),
            StructField("state",StringType(),True),
            StructField("locked",BooleanType(),True),
            StructField("assignee",StringType(),True),
            StructField("milestone", StringType(), True),
            StructField("Comments", IntegerType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("closed_at", TimestampType(), True),
            StructField("body", StringType(), True),
        ]), True),
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("Created_At",TimestampType(), True)
])

jsonData =spark.read.json("../data/2015-03-01-17.json",schema=schemaJson)
jsonData.show(truncate=False)



jsonData.selectExpr(
    '*',
    'actor.id- (actor.id%2) as kk'
).show()





