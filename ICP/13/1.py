from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.functions import col, concat, lit

# Create spark session
spark = SparkSession.builder.appName("ICP 12").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Load the csv data and create graph
input_path = "/home/yong/Desktop/CS5590_Spark/ICP/13/"
trip_data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "/201508_trip_data.csv")
station_data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "/201508_station_data.csv")
# Create vertex and edge dataframes
v = station_data_df.select(col("name").alias("id"), "lat", "long")
e = trip_data_df.select(col("Start Station").alias("src"), col("End Station").alias("dst"), col("Subscriber Type").alias("relationship"))
# Create graph
g = GraphFrame(v, e)

# 2. Triangle count
results = g.triangleCount()
results.select("id", "count").show(10, False)

# 3. Find shortest path
results = g.shortestPaths(landmarks=["2nd at Folsom", "California Ave Caltrain Station"])
results.select("id", "distances").show(10, False)

# 4. Applay page rank
results = g.pageRank(resetProbability=0.15, tol=0.01)
results.vertices.select("id", "pagerank").show(10, False)
results.edges.select("src", "dst", "weight").distinct().show(10, False)

# 5. Save graph
g.vertices.write.parquet(input_path + "vertices")
g.edges.write.parquet(input_path + "/edges")
