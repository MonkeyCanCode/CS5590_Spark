from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql.functions import lit
import json

sc = SparkContext.getOrCreate()
SQLContext(sc)

# visits all the nodes of a graph (connected component) using BFS
def bfs_connected_component(graph, start):
    graph = json.loads(graph)
    # keep track of all visited nodes
    explored = []
    # keep track of nodes to be checked
    queue = [start]
    levels = {}         # this dict keeps track of levels
    levels[start]= 0    # depth of start node is 0
    visited= [start]     # to avoid inserting the same node twice into the queue
    # keep looping until there are nodes still to be checked
    while queue:
       # pop shallowest node (first node) from queue
        node = queue.pop(0)
        explored.append(node)
        neighbours = graph[node]
        # add neighbours of node to queue
        for neighbour in neighbours:
            if neighbour not in visited:
                queue.append(neighbour)
                visited.append(neighbour)
                levels[neighbour]= levels[node]+1
                # print(neighbour, ">>", levels[neighbour])
    print(levels)
    return explored

# input data set
graph_str = '{"A": ["B", "C", "E"],"B": ["A","D", "E"],"C": ["A", "F", "G"],"D": ["B"],"E": ["A", "B","D"],"F": ["C"],"G": ["C"]}'

# Create UDF
bfsUDF = udf(bfs_connected_component)

# Create dataframe for the input data set
df = sc.parallelize([Row(input=graph_str)]).toDF()

# Run merge sort with Spark
df.withColumn("otuput", bfsUDF(df.input, lit("A"))).show(1, False)
