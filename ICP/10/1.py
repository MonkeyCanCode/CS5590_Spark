from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import udf
from pyspark.sql import Row

# Create spark session
spark = SparkSession.builder.appName("Lab 3").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

input_path = "/home/yong/Desktop/CS5590_Spark/ICP/10/ConsumerComplaints.csv"

# 1.Import the dataset and create data framesdirectly on import
print("Task 1:")
data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path)

# 2.Save data to file.
print("Task 2:")
data_df.write.save("/home/yong/Desktop/CS5590_Spark/ICP/10/consumer_complaints", format="csv", header="true")

# 3.Count the number of repeated record in the dataset.
print("Task 3:")
print("Total number of duplicate records: %d" % (data_df.count() - data_df.dropDuplicates().count()))

# 4.Apply Union operation on the dataset and order the output by Company Name alphabetically.
print("Task 4:")
data_df.union(data_df).orderBy(data_df["Company"].desc()).select("Company").show(10, False)

# 5.Use Groupby Query based on Zip Codes
print("Task 5:")
data_df.groupBy("Zip Code").count().show()

# 6 Apply join
print("Task 6.a:")
print(data_df.crossJoin(data_df).columns)

# 6 Apply aggregate
print("Task 6.b:")
data_df.where("Company = 'Bank of America'").count()

# 7 Show 13 rows
data_df.show(13, False)

# 8 Parse line with comma-delimited row
def splitFunc(line):
    return line.split(",")

# Create UDF
splitUDF = udf(splitFunc)
# Create dummy data
df = sc.parallelize([Row(input="a,b,c,d")]).toDF()
# Apply UDF
new_df = df.select(splitUDF("input"))
# Show data
new_df.show()
