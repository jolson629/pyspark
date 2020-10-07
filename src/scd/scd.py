from pyspark.sql.functions import udf, lit, when, date_sub
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType, BooleanType, DateType, TimestampType
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import udf


from datetime import datetime
from configparser import RawConfigParser
import os
import sys
import json


cfg = RawConfigParser()


def init_spark():
   appName = "Spark SCD Example"
   master = "local"
   conf = SparkConf().setAppName(appName).setMaster(master)
   spark = SparkContext(conf=conf)
   sqlContext = SQLContext(spark)

   return spark,sqlContext


def getCmdLineParser():
   import argparse
   desc = 'Cache loader'
   parser = argparse.ArgumentParser(description=desc)

   parser.add_argument('-c', '--current_state', help='current state data')
   parser.add_argument('-i', '--input', help='new input data')

   return parser

def quiet_logs(sc):
   logger = sc._jvm.org.apache.log4j
   logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
   logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def to_boolean(inval):
    if inval.strip() == 'True': return True
    elif inval.strip() == 'False': return False
    else: return None


def main(argv):

   # Overhead to manage command line opts and config file
   p = getCmdLineParser()
   args = p.parse_args()

   # set up contexts
   spark,sqlContext = init_spark()
   
   # hide info logs
   quiet_logs(spark)

   # register udfs
   to_boolean_udf = udf(to_boolean, BooleanType())
   
   
   # Consturct the current state off the input file
   current_state_raw_schema = StructType([
	   StructField("id", IntegerType(), True),
	   StructField("attr", StringType(), True),
	   StructField("is_current", StringType(), True),
	   StructField("is_deleted", StringType(), True),
	   StructField("active_date", TimestampType(), True),
	   StructField("inactive_date", TimestampType(), True)
   ])
   
   
   df_current_state_raw = sqlContext.read.csv(args.current_state, schema = current_state_raw_schema)
   df_current_state = df_current_state_raw.withColumn('is_current', to_boolean_udf(df_current_state_raw.is_current)).withColumn('is_deleted', to_boolean_udf(df_current_state_raw.is_deleted))
   df_current_state.show(df_current_state.count(), False)
   df_current_state.printSchema() 
  
  

   # Construct the new input

   input_schema = StructType([
      StructField("src_id", IntegerType(), True),
      StructField("src_attr", StringType(), True)
   ])

   df_input = sqlContext.read.csv(args.input, schema = input_schema)

   df_input.show(df_input.count(), False)
   df_input.printSchema()
   
   high_time = datetime.strptime('3001-01-01 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
   print(high_time)
   current_time = datetime.now()
   print(current_time)
   
   
   # Prepare for merge - Added effective and end date
   df_input_new = df_input.withColumn('src_active_date', lit(current_time)).withColumn('src_inactive_date', lit(high_time))

   # FULL Merge, join on key column and also high time column to make only join to the latest records
   df_merge = df_current_state.join(df_input_new, (df_input_new.src_id == df_current_state.id) & (df_input_new.src_inactive_date == df_current_state.inactive_date), how='fullouter')

   # Derive new column to indicate the action
   df_merge = df_merge.withColumn('action',
      when(df_merge.attr != df_merge.src_attr, 'UPSERT')
         .when(df_merge.src_id.isNull() & df_merge.is_current, 'DELETE')
         .when(df_merge.id.isNull(), 'INSERT')
         .otherwise('NOACTION')
   )

   df_merge.show(df_merge.count(),False)

   # Generate the new data frames based on action code
   column_names = ['id', 'attr', 'is_current', 'is_deleted', 'active_date', 'inactive_date']

   # For records that needs no action
   df_merge_p1 = df_merge.filter(df_merge.action == 'NOACTION').select(column_names)

   # For records that needs insert only
   df_merge_p2 = df_merge.filter(df_merge.action == 'INSERT').select(
      df_merge.src_id.alias('id'),
      df_merge.src_attr
         .alias('attr'),lit(True)
         .alias('is_current'),lit(False)
         .alias('is_deleted'),
         df_merge.src_active_date.alias('active_date'), df_merge.src_inactive_date.alias('inactive_date')
   )
   # For records that needs to be deleted
   df_merge_p3 = df_merge.filter(df_merge.action == 'DELETE').select(column_names).withColumn('is_current', lit(False)).withColumn('is_deleted', lit(True))

   # For records that needs to be expired and then inserted
   df_merge_p4_1 = df_merge.filter(df_merge.action == 'UPSERT').select(
      df_merge.src_id.alias('id'),
      df_merge.src_attr.alias('attr'),lit(True)
      .alias('is_current'),lit(False)
      .alias('is_deleted'),
      df_merge.src_active_date.alias('active_date'),
      df_merge.src_inactive_date.alias('inactive_date')
   )

   df_merge_p4_2 = df_merge.filter(df_merge.action == 'UPSERT').withColumn('inactive_date', df_merge.src_active_date).withColumn('is_current', lit(False)).withColumn('is_deleted', lit(False)).select(column_names)

   # Union all records together
   df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p3).unionAll(df_merge_p4_1).unionAll(df_merge_p4_2)
   
   df_merge_final.orderBy(['id', 'active_date']).show(df_merge_final.count(), False)
   

if __name__ == "__main__":
    main(sys.argv[1:])
