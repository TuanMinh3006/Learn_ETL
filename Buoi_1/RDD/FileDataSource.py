from pyspark import SparkContext,SparkConf

#Khoi tao theo kieu khacs
conf=SparkConf().setAppName("Minhdz").setMaster("local[*]").set("spark.executor.memory","2g")
# SetMaster laf set phan vung
# local[*] la set phan vung vao all loi CPU (local[n] de xet vao n loi)
#2g laf set 2g
sc=SparkContext(conf=conf)

fileRDD =sc.textFile("duong dan")