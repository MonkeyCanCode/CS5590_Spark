from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="PysparkStreaming")

# Change log level to error
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

ssc = StreamingContext(sc, 3)

lines = ssc.socketTextStream("localhost", 8000)

counts = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
counts.pprint()

counts.pprint()
ssc.start()
ssc.awaitTermination()