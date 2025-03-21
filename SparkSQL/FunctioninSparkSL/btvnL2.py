
from py4j.protocol import ARRAY_TYPE
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType,ArrayType,StructType, StructField, StringType, BooleanType, LongType, IntegerType
from datetime import datetime
spark=(SparkSession.builder\
       .appName("Minh dz") \
       .master("local[*]")\
       .config("spark.executor.memory","4g")\
       .getOrCreate())
data = [("11/12/2025",),("27/02.2014",),("2023.01.09",),("28-12-2005",)]
df = spark.createDataFrame(data , ["date"])

formats=["%d-%m-%Y","%Y-%m-%d"]
def transform(n):
    for fm in formats:
        try:
            date_object = datetime.strptime(n,fm)
            return date_object.strftime("%d-%m-%Y")
        except:
            continue

def day_month_year(n):
    n=n.replace(".","-").replace("/","-")
    n=transform(n)
    result=n.split("-")
    return result

day_month_year_udf=udf(day_month_year,ArrayType(StringType()))
ouput=df.withColumn("day",day_month_year_udf(col("date"))[0])\
       .withColumn("month",day_month_year_udf(col("date"))[1])\
       .withColumn("year",day_month_year_udf(col("date"))[2])\
       .show()
