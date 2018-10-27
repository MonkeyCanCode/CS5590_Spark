from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Create spark session
spark = SparkSession.builder.appName("Lab 3").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define input path
input_path_world_cups = "/home/yong/Desktop/CS5590_Spark/Lab/3/source/2/WorldCups.csv"
input_path_world_cup_matches = "/home/yong/Desktop/CS5590_Spark/Lab/3/source/2/WorldCupMatches.csv"

# Load inptu data and consturct dataframe 
world_cups_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path_world_cups)
world_cup_matches_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path_world_cup_matches)

# Create temp view
world_cups_df.createOrReplaceTempView("world_cups")
world_cup_matches_df.createOrReplaceTempView("world_cup_matches")

# Query 1 (*): Get the most recent year of world cup
print("Get the most recent year of world cup")
spark.sql("select max(year) as year from world_cups").show()

# Query 2: Get winner countries order by the number of winning in decreasing order
print(" Get winner countries order by the number of winning in decreasing order")
spark.sql("select Winner, count(*) as count from world_cups group by Winner order by 2 desc").show()

# Query 3: Get all rows where the value of Winner starts with Germany
print("Get all rows where the value of Winner starts with Germany")
spark.sql("select * from world_cups where Winner like 'Germany%'").show()

# Query 4 (*): Get row where Country is USA
print("Get row where Country is USA")
spark.sql("select * from world_cups where Country = 'USA'").show()

# Query 5 (*): Get total row count of the input data set
print("Get total row count of the input data set")
spark.sql("select count(*) from world_cups").show()

# Query 6 (*): Get year where country is same as winner
print("Get year where country is same as winner")
spark.sql("select year, Country, Winner from world_cups where Country = Winner").show()

# Query 7 (*): Get total number of matches played
print("Get total number of matches played")
spark.sql("select sum(MatchesPlayed) from world_cups").show() 

# Query 8: Get country and a list of Stadium when year is 1994
print("Get country and a list of Stadium when year is 1994")
spark.sql("""
    select distinct a.Country, b.Stadium
    from world_cups a
    join world_cup_matches b on a.year = b.year
    where a.year = 1994
""").show()

# Query 9: Get row with longest length of country name
print("Get row with longest length of country name")
spark.sql("""
    select a.*
    from world_cups a
    join (
        select country, length(country) as size
        from world_cups
        group by country
        order by 2 desc
        limit 1
    ) b on a.country = b.country
""").show()

# Query 10: Get country name where they got 4th place after getting 1st place
print("Get country name where they got 4th place after getting 1st place")
spark.sql("""
    select a.winner country_name, a.year as winning_year, b.year as fourth_year
    from world_cups a, world_cups b
    where a.Winner = b.Fourth and a.year < b.year
""").show()


# Generate RDD
world_cups_rdd = world_cups_df.rdd

# RDD 1 (*): Get the most recent year of world cup
print("RDD - Get the most recent year of world cup")
print(world_cups_rdd.max(lambda x: x[0])[0])
print()

# RDD 2 (*): Get row where Country is USA
print("RDD - Get row where Country is USA")
print(world_cups_rdd.filter(lambda x: x[1] == 'USA').collect())
print()

# RDD 3 (*): Get total row count of the input data set
print("RDD - Get total row count of the input data set")
print(world_cups_rdd.count())
print()

# RDD 4 (*): Get year where country is same as winner
print("RDD - Get year where country is same as winner")
print([(_[0], _[1]) for _ in world_cups_rdd.filter(lambda x: x[1] == x[2]).collect()])
print()

# RDD 5 (*): Get total number of matches played
print("RDD - Get total number of matches played")
print(world_cups_rdd.map(lambda x: x[8]).sum())
print()
