from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import *
import pyspark.sql.functions as F


from configparser import RawConfigParser
import os
import sys

cfg = RawConfigParser()

def init_spark():

   aws_token = "FwoGZXIvYXdzECEaDAq73maLUtz0PvBLTCLdAYX10bvncMxJxWPFrjo767BLbp8HEXyY3CPdl4oXwuAT24VvpCzm+jxC8mdxbykOktUGyXRlImqwSbhhAw8UeY8PwLOdRdnIxMLY6Al0asKGRENMNJUG0jQ/RPVJff2Yd1nibRRxjTp+iyDhDg8KyEx840Lw4/A7CmrErbbLNm0GjfYyy0dKF48oNAhmrOHld5AK+X3mJyAANIYm3gfuA3JFDV+W6RrMcdn2EqU+rN4ATQb67O0WiUYWOD0EM0Opuy7it455sWOvWehVqpsCqSwjiRWTqUxj/lSEWZhYKLumtvUFMjLcdABmhyLxlLq6NArPP+vRxhHVggc9Cs8pi6xt3zOM24sdLfj6yk49neB2bxECMPxoeA=="

   config = (SparkConf().setAppName("s3a_test")
           )
   spark = SparkContext(conf=config)

   hadoopConf = spark._jsc.hadoopConfiguration()
   hadoopConf.set('fs.s3a.endpoint', 's3-us-east-1.amazonaws.com')
   hadoopConf.set('fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
   hadoopConf.set('fs.s3a.access.key', 'ASIATAOTGW3MIKBM5CNO')
   hadoopConf.set('fs.s3a.secret.key', 'hK7GA6I7+mGm+glnDNDxY2EbvCduZKsT9L0JTMWR')
   hadoopConf.set('fs.s3a.session.token',aws_token)

   hadoopConf.set('com.amazonaws.services.s3a.enableV4', 'true')
   hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
   


   sqlContext = SQLContext(spark)
   return spark,sqlContext

# Test this more
def getCmdLineParser():
   import argparse
   desc = 'Cache loader'
   parser = argparse.ArgumentParser(description=desc)

   parser.add_argument('-i', '--input', help='input path')
   parser.add_argument('-d', '--delimiter', help='input delimiter')
   parser.add_argument('-o', '--output', help='output path')

   return parser


def main(argv):

   # Overhead to manage command line opts and config file
   p = getCmdLineParser()
   args = p.parse_args()

   spark,sqlContext = init_spark()

   source_df = sqlContext.read.option("header", "true") \
      .option("delimiter", ",") \
      .option("inferSchema", "true") \
      .csv(args.input)
   
   source_df.show()

   #source_df.write.format('com.databricks.spark.csv') \
   #   .mode('overwrite').option("header", "true").save(args.output)
 

if __name__ == "__main__":
    main(sys.argv[1:])
