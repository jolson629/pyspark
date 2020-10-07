from pyspark.sql.functions import udf, lit, when, date_sub
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType, BooleanType, DateType
import json
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import Row
from datetime import datetime


appName = "Spark SCD Merge Example"
master = "local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def quiet_logs(sc):
   logger = sc._jvm.org.apache.log4j
   logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
   logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# hide info logs
quiet_logs(sc)
# Target data set
data_target = [
Row(1, "Hello!", False, False, datetime.strptime('2020-10-05', '%Y-%m-%d'), datetime.strptime('2020-10-05', '%Y-%m-%d')),
Row(1, "Hello World!", True, False, datetime.strptime('2019-01-01', '%Y-%m-%d'), datetime.strptime('9999-12-31', '%Y-%m-%d')),
Row(2, "Hello Spark!", True, False, datetime.strptime('2019-02-01', '%Y-%m-%d'), datetime.strptime('9999-12-31', '%Y-%m-%d')),
Row(3, "Hello Old World!", True, False, datetime.strptime('2019-02-01', '%Y-%m-%d'), datetime.strptime('9999-12-31', '%Y-%m-%d'))
]

schema_target = StructType([
StructField("id", IntegerType(), True),
StructField("attr", StringType(), True),
StructField("is_current", BooleanType(), True),
StructField("is_deleted", BooleanType(), True),
StructField("start_date", DateType(), True),
StructField("end_date", DateType(), True)
])

df_target = sqlContext.createDataFrame(
   sc.parallelize(data_target),
   schema_target
   )
   
df_target.show()
df_target.printSchema()

# Source data set
data_source = [
   Row(1, "Hello World!"),
   Row(2, "Hello PySpark!"),
   Row(4, "Hello Scala!")
]

schema_source = StructType([
   StructField("src_id", IntegerType(), True),
   StructField("src_attr", StringType(), True)
])

df_source = sqlContext.createDataFrame(
   sc.parallelize(data_source),
   schema_source
)

df_source.show()
df_source.printSchema()
high_date = datetime.strptime('9999-12-31', '%Y-%m-%d').date()
print(high_date)
current_date = datetime.today().date()
print(current_date)

# Prepare for merge - Added effective and end date
df_source_new = df_source.withColumn('src_start_date', lit(
   current_date)).withColumn('src_end_date', lit(high_date))

# FULL Merge, join on key column and also high date column to make only join to the latest records
df_merge = df_target.join(df_source_new, (df_source_new.src_id == df_target.id) & (df_source_new.src_end_date == df_target.end_date), how='fullouter')

# Derive new column to indicate the action
df_merge = df_merge.withColumn('action',
   when(df_merge.attr != df_merge.src_attr, 'UPSERT')
      .when(df_merge.src_id.isNull() & df_merge.is_current, 'DELETE')
      .when(df_merge.id.isNull(), 'INSERT')
      .otherwise('NOACTION')
)

df_merge.show()

# Generate the new data frames based on action code

column_names = ['id', 'attr', 'is_current', 'is_deleted', 'start_date', 'end_date']

# For records that needs no action
df_merge_p1 = df_merge.filter(
df_merge.action == 'NOACTION').select(column_names)

# For records that needs insert only
df_merge_p2 = df_merge.filter(df_merge.action == 'INSERT').select(df_merge.src_id.alias('id'),
df_merge.src_attr
   .alias('attr'),lit(True)
   .alias('is_current'),lit(False)
   .alias('is_deleted'),
   df_merge.src_start_date.alias(
'start_date'),
df_merge.src_end_date.alias(
'end_date')
)
# For records that needs to be deleted
df_merge_p3 = df_merge.filter(
df_merge.action == 'DELETE').select(column_names).withColumn('is_current', lit(False)).withColumn('is_deleted', lit(True))

# For records that needs to be expired and then inserted
df_merge_p4_1 = df_merge.filter(df_merge.action == 'UPSERT').select(
   df_merge.src_id.alias('id'),
   df_merge.src_attr.alias('attr'),lit(True)
   .alias('is_current'),lit(False)
   .alias('is_deleted'),
   df_merge.src_start_date.alias('start_date'),
   df_merge.src_end_date.alias('end_date')
)

df_merge_p4_2 = df_merge.filter(df_merge.action == 'UPSERT').withColumn('end_date', date_sub(df_merge.src_start_date, 1)).withColumn('is_current', lit(False)).withColumn('is_deleted', lit(False)).select(column_names)

# Union all records together
df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(
df_merge_p3).unionAll(df_merge_p4_1).unionAll(df_merge_p4_2)
df_merge_final.orderBy(['id', 'start_date']).show()
# At last, you can overwrite existing data using this new data frame.
# ...
