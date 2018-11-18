from graphframes import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create spark session
spark = SparkSession.builder.appName("Lab 4").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define input path
input_path = "/home/yong/Desktop/CS5590_Spark/Lab/4/1/"

# Load vertics and edges 
v = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "meta-members.csv")\
    .select(col("member_id").alias("id"), col("name"))
e = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "member-edges.csv")\
    .select(col("member1").alias("src"), col("member2").alias("dst"), col("weight").alias("relationship"))
# Construct graph
g = GraphFrame(v, e)
# Run PageRank until convergence to tolerance "tol"
results = g.pageRank(resetProbability=0.15, tol=0.01)
# Display resulting pageranks and final edge weights
results.vertices.select("id", "pagerank").show(10, False)
results.edges.select("src", "dst", "weight").show(10, False)
