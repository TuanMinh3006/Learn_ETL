from pyspark import SparkContext,SparkConf
from random import Random
import time
conf=SparkConf().setAppName("Minhdz").setMaster("local[2]").set("spark.executor.memory","2g")

sc=SparkContext(conf=conf)

data=['Dat','Golden','kkk','hhh']
rdd=sc.parallelize(data)
#iterator: Là một partition của RDD (một tập hợp các phần tử từ danh sách gốc).
def numsPartition(iterator):
    # create 1 nums fo map Partiton data
    rand=Random(int(time.time()*1000)+Random().randint(0,1000))
    return [f'{item}:{rand.randint(1,1000)}' for item in iterator]

result=rdd.mapPartitions(numsPartition) #Áp dụng numsPartition cho toàn bộ partition của RDD

print(result.collect())

#khoong su dung mapPartitons



