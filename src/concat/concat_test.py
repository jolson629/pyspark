from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import *
import pyspark.sql.functions as F
import numpy as np
import pandas as pd

# Take a spark array of floats, returns a serialized np array 
# Is there a difference between a spark float and a numpy float?
# Hate this loop. What to do?
def to_serialized_np_array(spark_float_array):
   python_array=[]
   for a_float in spark_float_array:
      python_array.append(np.float32(a_float)) 
     
   return np.asarray(python_array).tobytes()

#FloatType: Represents 4-byte single-precision floating point numbers.
#DoubleType: Represents 8-byte double-precision floating point numbers.

config = (SparkConf().setAppName("s3a_test")
        )
sparkContext = SparkContext(conf=config)
sqlContext = SQLContext(sparkContext)

to_serialized_np_array_udf = F.udf(to_serialized_np_array)

df1 = sparkContext.parallelize([[1,2,3],[4,5,6]]).toDF(("A","B","C"))
df1.show()

# Convert all columns to a PySpark array of floats to simulate the customer feature set.
df1 = df1.withColumn("to_array", to_serialized_np_array_udf(F.array(*[F.col(x).cast(FloatType()) for x in df1.columns])))
df1.show()

#df2 = df1.select(to_serialized_np_array_udf(F.col("to_array")))
#df2.show()

#to_numpy_single_udf = F.udf(to_numpy_single, np.single)
#df1 = df1.withColumn("concat", F.concat_ws(",", *[F.col(x) for x in df1.columns]))
#df1 = df1.withColumn("serialized", np.array(list(F.concat_ws(",", *[F.col(x) for x in df1.columns]))).tobytes())
#df1 = df1.withColumn("serialized", F.col_ws(",", *[F.col(x) for x in df1.columns]))

