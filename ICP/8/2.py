from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix, BlockMatrix

# Create spark and SQL contexts
sc = SparkContext.getOrCreate()
SQLContext(sc)

# Change log level to error
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

# Load data for matrix m
m = sc.parallelize([[2, 2, 2], [3, 3, 3]]).zipWithIndex()
# Load data for matrix n
n = sc.parallelize([[1, 1], [4, 4], [3, 3]]).zipWithIndex()

# Create block matrix for m
matrix1 = IndexedRowMatrix(m.map(lambda row: IndexedRow(row[1], row[0]))).toBlockMatrix()
# Create block matrix for n
matrix2 = IndexedRowMatrix(n.map(lambda row2: IndexedRow(row2[1], row2[0]))).toBlockMatrix()

# Get output of matrix multiplication
output = matrix1.multiply(matrix2).toLocalMatrix()

# Show output
print(output)