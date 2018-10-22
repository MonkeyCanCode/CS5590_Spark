from pyspark import SparkContext

# Create spark context
sc = SparkContext.getOrCreate()

# Change log level to error
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

# Read input file
text_file = sc.textFile("/home/yong/Desktop/CS5590_Spark/ICP/8/input.txt")

# Get count
counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Show output
print(counts.collect())
