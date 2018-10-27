from pyspark import SparkContext

# Create custom mapper for generating mutual friends
def mutual_friend_mapper(line):
    parts = line.split(" -> ")
    user = parts[0]
    friends = parts[1].split(" ")
    result = []
    for friend in friends:
        result.append((" ".join(sorted([user, friend])), friends))
    return result

# Create spark context
sc = SparkContext.getOrCreate()

# Define input file path
input_file_path = "/home/yong/Desktop/CS5590_Spark/Lab/3/source/1/input.txt"

# Change log level to error
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

# Show output
print(sc.textFile(input_file_path).flatMap(mutual_friend_mapper).reduceByKey(lambda key, value: set(key) & set(value)).collect())
