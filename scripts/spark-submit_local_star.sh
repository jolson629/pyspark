export PYSPARK_PYTHON=python3

spark-submit --master local[*] --jars /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-aws-3.2.1.jar,/opt/hadoop-3.2.1/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar ../src/df/df_test.py --input s3a://axt-model-datastore/tmp/input/customer_csv/customer_features.csv --delimiter ',' --output s3a://axt-model-datastore/tmp/output/customer_csv/customer_features.csv
