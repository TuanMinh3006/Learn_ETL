from pyspark import SparkContext,SparkConf

conf=SparkConf().setAppName("Minhdz").setMaster("local[2]").set("spark.executor.memory","2g")

sc=SparkContext(conf=conf)

number =[1,2,3,4,5,67,8,9,11]
rdd=sc.parallelize(number)
print(rdd.getNumPartitions())
"""
partition1:1,2,3,4
partition2:5,67,8,9
"""
#using Transformation for create rdd
squareRdd= rdd.map(lambda x: x*x)
print(squareRdd.collect())

filerRDD=rdd.filter(lambda x: x > 5)
print(filerRDD.collect())

flatmapRD=rdd.flatMap(lambda x: [x,x*2])
#[1,2,2,4,3,6,...]
print(flatmapRD.collect())
