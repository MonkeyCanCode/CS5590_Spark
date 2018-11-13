from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.functions import col, concat, lit

# Create spark session
spark = SparkSession.builder.appName("ICP 12").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Load the csv data and create graph
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
station_data_df.select("dockcount").distinct().show()

# 4. Name columns
station_data_df.columns

# 5. Output dataframe
station_data_df.write.parquet(input_path + "/data.parquet")

# 7. Show some vertics
g.vertices.show(10, False)

# 8. Show some edges
g.edges.show(10, False)

# 9. Vertex in-degree
g.inDegrees.show(10, False)

# 10. Vertex out-degree
g.outDegrees.show(10, False)

# Bonus 1
g.degrees.show(10, False)

# Bonus 2
g.find("(a)-[e]->(b); (b)-[e2]->(a)").distinct().show(10, False)

# Bonus 3
g.vertices.write.parquet(input_path + "/vertices")
g.edges.write.parquet(input_path + "/edges")
