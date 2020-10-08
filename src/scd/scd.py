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

   input_raw_schema = StructType([
      StructField("input_id", IntegerType(), True),
      StructField("input_attr", StringType(), True)
   ])

   df_input_raw = sqlContext.read.csv(args.input, schema = input_raw_schema)

   df_input_raw.show(df_input_raw.count(), False)
   df_input_raw.printSchema()
   
   high_time = datetime.strptime('3001-01-01 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
   print(high_time)
   current_time = datetime.now()
   print(current_time)
   
   
   # Prepare for merge - Added effective and end date
   df_input = df_input_raw.withColumn('input_active_date', lit(current_time)).withColumn('input_inactive_date', lit(high_time))

   # FULL Merge, join on key column and also high time column to make only join to the latest records
   df_merge = df_current_state.join(df_input, (df_input.input_id == df_current_state.id) & (df_input.input_inactive_date == df_current_state.inactive_date), how='fullouter')

   # Derive new column to indicate the action
   df_merge = df_merge.withColumn('action',
      when(df_merge.attr != df_merge.input_attr, 'UPSERT')
         .when(df_merge.input_id.isNull() & df_merge.is_current, 'DELETE')
         .when(df_merge.id.isNull(), 'INSERT')
         .otherwise('NOACTION')
   )

   df_merge.show(df_merge.count(),False)

   # Generate the new data frames based on action code
   column_names = ['id', 'attr', 'is_current', 'is_deleted', 'active_date', 'inactive_date']

   # For records that needs no action
   df_no_action = df_merge.filter(df_merge.action == 'NOACTION').select(column_names)

   # For records that needs insert only
   df_insert = df_merge.filter(df_merge.action == 'INSERT').select(
      df_merge.input_id.alias('id'),
      df_merge.input_attr.alias('attr'),
         lit(True).alias('is_current'),
         lit(False).alias('is_deleted'),
         df_merge.input_active_date.alias('active_date'), 
         df_merge.input_inactive_date.alias('inactive_date')
   )
   # For records that needs to be deleted
   df_delete = df_merge.filter(df_merge.action == 'DELETE').select(
      df_merge.id.alias('id'),
      df_merge.attr.alias('attr'),
      lit(False).alias('is_current'),
      lit(True).alias('is_deleted'),
      df_merge.active_date.alias('active_date'),
      lit(current_time).alias('inactive_date')
   )

   # For records that needs to be expired and then inserted
   df_upsert_expired_inserted = df_merge.filter(df_merge.action == 'UPSERT').select(
      df_merge.input_id.alias('id'),
      df_merge.input_attr.alias('attr'),
      lit(True).alias('is_current'),
      lit(False).alias('is_deleted'),
      df_merge.input_active_date.alias('active_date'),
      df_merge.input_inactive_date.alias('inactive_date')
   )

   df_upsert_expired = df_merge.filter(df_merge.action == 'UPSERT').withColumn('inactive_date', df_merge.input_active_date).withColumn('is_current', lit(False)).withColumn('is_deleted', lit(False)).select(column_names)

   # Union all records together
   df_merge_final = df_no_action.unionAll(df_insert).unionAll(df_delete).unionAll(df_upsert_expired_inserted).unionAll(df_upsert_expired)
   
   df_merge_final.orderBy(['id', 'active_date']).show(df_merge_final.count(), False)
   

if __name__ == "__main__":
    main(sys.argv[1:])
