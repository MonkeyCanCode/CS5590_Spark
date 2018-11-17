from pyspark.sql import SparkSession
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics

# Create spark session
spark = SparkSession.builder.appName("Lab 3").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define input path
input_path = "/home/yong/Desktop/CS5590_Spark/Lab/4/2/"

# Load data and select feature and label columns
data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ";").load(input_path + "Absenteeism_at_work.csv")
data = data.withColumnRenamed("Social drinker", "label").select("label", "Distance from Residence to Work", "Son", "Pet")
data = data.select(data.label.cast("double"), "Distance from Residence to Work", "Son", "Pet")

# Create vector assembler for feature columns
assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)

# Split data into training and test data set
training, test = data.select("label", "features").randomSplit([0.6, 0.4])

# Create Navie Bayes model and fit the model with training dataset 
nb = NaiveBayes()
model = nb.fit(training)

# Generate prediction from test dataset 
predictions = model.transform(test)

# Evuluate the accuracy of the model
evaluator = MulticlassClassificationEvaluator()
accuracy = evaluator.evaluate(predictions)

# Show model accuracy 
print("Accuracy:", accuracy)

# Report 
predictionAndLabels = predictions.select("label", "prediction").rdd
metrics = MulticlassMetrics(predictionAndLabels)
print("Confusion Matrix:", metrics.confusionMatrix())
print("Precision:", metrics.precision())
print("Recall:", metrics.recall())
print("F-measure:", metrics.fMeasure())
