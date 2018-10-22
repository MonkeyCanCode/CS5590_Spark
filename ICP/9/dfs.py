from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql.functions import lit
import json

sc = SparkContext.getOrCreate()
SQLContext(sc)

def dfs_iterative(graph, start):
    graph = json.loads(graph)
    stack, path = [start], []
    while stack:
        vertex = stack.pop()
        if vertex in path:
            continue
        path.append(vertex)
        for neighbor in graph[vertex]:
            stack.append(neighbor)
    return path


# input data set
graph_str = '{"1": ["2", "3"], "2": ["4", "5"], "3": ["5"], "4": ["6"], "5": ["6"], "6": ["7"], "7": []}'

# Create UDF
dfsUDF = udf(dfs_iterative)

# Create dataframe for the input data set
df = sc.parallelize([Row(input=graph_str)]).toDF()

# Run merge sort with Spark
df.withColumn("otuput", dfsUDF(df.input, lit("1"))).show(1, False)
