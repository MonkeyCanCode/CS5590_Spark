from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
This is use for create streaming of text from txt files that creating dynamically 
from files.py code. This spark streaming will execute in each 3 seconds and It'll
show number of words count from each files dynamically
"""

sc = SparkContext(appName="PysparkStreaming")
# Change log level to error
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

ssc = StreamingContext(sc, 3)   #Streaming will execute in each 3 seconds
lines = ssc.textFileStream('/home/yong/Desktop/CS5590_Spark/ICP/11/1/log')  #'log/ mean directory name
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
counts.pprint()
ssc.start()
ssc.awaitTermination()
