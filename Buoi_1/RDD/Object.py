
from pyspark import SparkContext

sc = SparkContext('local',"Minh dz")

data=[
    {'id':1,'name':'dat'},
    {'id':2,'name':'heuy'},
    {'id':3,'name':'12'},
]
print(data)
#creat rdd from data
rdd=sc.parallelize(data)
print(rdd.collect())# print list format
print(f"number of data:{rdd.count()}")

