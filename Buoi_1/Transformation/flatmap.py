from pyspark import SparkContext,SparkConf

conf=SparkConf().setAppName("Minhdz").setMaster("local[2]").set("spark.executor.memory","2g")

sc=SparkContext(conf=conf)

fileRdd=sc.textFile("../Data/data.txt")
flatmap=fileRdd.flatMap(lambda line: line.split(" "))
print(flatmap.collect())