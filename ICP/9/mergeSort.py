from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql import Row

sc = SparkContext.getOrCreate()
SQLContext(sc)

# Merge sort implementation 
def mergeSort(input_data):
    if len(input_data)>1:
        mid = len(input_data)//2
        lefthalf = input_data[:mid]
        righthalf = input_data[mid:]
        mergeSort(lefthalf)
        mergeSort(righthalf)
        i=0
        j=0
        k=0
        while i < len(lefthalf) and j < len(righthalf):
            if lefthalf[i] < righthalf[j]:
                input_data[k]=lefthalf[i]
                i=i+1
            else:
                input_data[k]=righthalf[j]
                j=j+1
            k=k+1
        while i < len(lefthalf):
            input_data[k]=lefthalf[i]
            i=i+1
            k=k+1
        while j < len(righthalf):
            input_data[k]=righthalf[j]
            j=j+1
            k=k+1
    return input_data

# input data set
input_data = [54,26,93,17,77,31,44,55,20]

# Create UDF
mergeSortUDF = udf(mergeSort)

# Create dataframe for the input data set
df = sc.parallelize([Row(input=input_data)]).toDF()

# Run merge sort with Spark
df.withColumn("otuput", mergeSortUDF(df.input)).show(1, False)
