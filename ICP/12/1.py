from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.functions import concat, col, lit

# Create spark session
spark = SparkSession.builder.appName("ICP 12").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Load tge csv data and create graph
# 6. Create vertics
input_path = "/home/yong/Desktop/CS5590_Spark/ICP/12/"
trip_data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "/201508_trip_data.csv")
station_data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "/201508_station_data.csv")
# Create vertex and edge dataframes
v = station_data_df.select(col("name").alias("id"), "lat", "long")
e = trip_data_df.select(col("Start Station").alias("src"), col("End Station").alias("dst"), col("Subscriber Type").alias("relationship"))
# Create graph
g = GraphFrame(v, e)

# 2. Concatenate columns
station_data_df.select(concat(col("lat"), lit(" "), col("long")).alias("loc")).show(10, False)

# 3. Remove duplicates
# 5. Output dataframe
station_data_df.select("dockcount").distinct().show()

# 7. Show some vertics
g.vertices.show(10, False)

# 8. Show some edges
g.edges.show(10, False)

# 9. Vertex in-degree
g.inDegrees.show(10, False)

# 10. Vertex out-degree
g.outDegrees.show(10, False)
