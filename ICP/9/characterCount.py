from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql import Row

sc = SparkContext.getOrCreate()

a = sc.textFile("/home/yong/Desktop/CS5590_Spark/ICP/9/input.txt")
print("Total character count:", a.map(lambda x:len(x)).sum())
