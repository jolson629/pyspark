from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import *
import pyspark.sql.functions as F

def init_spark():
   spark = SparkSession.builder.appName("FOJTest").getOrCreate()
   sc = spark.sparkContext
   return spark,sc

def do_match(row):
    #return row.mkString(", ")
   if row.source_name is None:
      return "NONE"
   else:
      return row.source_name

def main():
   spark,sc = init_spark()

   source_df = spark.read.option("header", "true") \
      .option("delimiter", ",") \
      .option("inferSchema", "true") \
      .csv("../data/source.csv")

   source_df2 = source_df.select(*(col(x).alias('source_' + x) for x in source_df.columns))

   target_df = spark.read.option("header", "true") \
      .option("delimiter", ",") \
      .option("inferSchema", "true") \
      .csv("../data/target.csv")

   target_df2 = target_df.select(*(col(x).alias('target_' + x) for x in target_df.columns))

   source_df2.show(10, truncate=False)
   target_df2.show(10, truncate=False)


   match_udf = udf(lambda z: do_match(z), StringType())
   spark.udf.register("match_udf", match_udf)

   foj_df = source_df2.join(target_df2, source_df2.source_id == target_df2.target_id, how='full')
   foj_df.show()

   foj_match_df = foj_df.withColumn("match", match_udf(struct([foj_df[x] for x in foj_df.columns])))

   foj_match_df.show()


if __name__ == '__main__':
  main()
