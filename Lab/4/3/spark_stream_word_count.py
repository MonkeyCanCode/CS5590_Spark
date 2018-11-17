from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from collections import namedtuple

sc = SparkContext(appName="Lab 4")

# Change log level to error
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

ssc = StreamingContext(sc, 3)

Tweet = namedtuple("Data", ("tag", "count"))

# Split each line into words and use map reduce to count occurance of token then print word count
ssc.socketTextStream("localhost", 8000).flatMap(lambda line: line.split(" ")).map(lambda word: (word.lower(), 1)).reduceByKey(lambda x, y: x + y).map(lambda rec: Tweet(rec[0], rec[1])).pprint()

# Start spark streaming 
ssc.start()
ssc.awaitTermination()