#final code for project 2 glue job number 1 (g4_p2_parquet)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, concat, lit, monotonically_increasing_id, array, substring, current_date, to_date
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, ArrayType, DoubleType
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="g4_p2",
    table_name="project2_parquet__1__parquet",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1686632877864 = glueContext.create_dynamic_frame.from_catalog(
    database="g4_p2",
    table_name="acct_master_g4",
    transformation_ctx="AWSGlueDataCatalog_node1686632877864",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("trnrefid", "string", "trnrefid", "string"),
        ("code", "string", "code", "string"),
        ("tdate", "string", "tdate", "string"),
        ("trn_amount", "double", "Product Sales", "double"),
        ("vat", "double", "Value Added Tax", "double"),
        ("excise_duty", "double", "Excise Duty", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)


df_data = ApplyMapping_node2.toDF() 
df_data1 = df_data.select(col("code").alias("voucher_code"), col("tdate").alias("txn_date"), col("Product Sales").alias("txn_amt"), col("trnrefid").alias("source_system_txn_id"))
df_data1 = df_data1.withColumn("acc", lit("Product Sales"))
df_data2 = df_data.select(col("code").alias("voucher_code"), col("tdate").alias("txn_date"), col("Value Added Tax").alias("txn_amt"), col("trnrefid").alias("source_system_txn_id"))
df_data2 = df_data2.withColumn("acc", lit("Value Added Tax"))
df_data3 = df_data.select(col("code").alias("voucher_code"), col("tdate").alias("txn_date"), col("Excise Duty").alias("txn_amt"), col("trnrefid").alias("source_system_txn_id"))
df_data3 = df_data3.withColumn("acc", lit("Excise Duty"))

df_fin = df_data1.union(df_data2).union(df_data3)
df_fin = df_fin.withColumn("txn_id", concat(df_fin.txn_date, lit('_'), monotonically_increasing_id()))
df_fin = df_fin.withColumn("txn_type", lit("C"))
df_fin = df_fin.withColumn("source_system_id", lit("1"))

df = AWSGlueDataCatalog_node1686632877864.toDF()

df_inner = df.join(df_fin, df["acc_name"] == df_fin["acc"], "inner")
df_final = df_inner.select((col("txn_id")), col("voucher_code"), col("txn_type"), col("txn_date").alias("date"), col("acc_no"), col("txn_amt"), col("source_system_id"), col("source_system_txn_id"))
df_final = df_final.withColumn("txn_date", to_date(col('date'), 'dd-MMM-yy'))

df_1 = df_final.select((col("txn_id")), col("voucher_code"), col("txn_type"), col("txn_date"), col("acc_no"), col("txn_amt"), col("source_system_id"), col("source_system_txn_id")).filter(col("txn_date") <= current_date())
df_2 = df_final.select((col("txn_id")), col("voucher_code"), col("txn_type"), col("txn_date"), col("acc_no"), col("txn_amt"), col("source_system_id"), col("source_system_txn_id")).filter(col("txn_date") > current_date())

dynamic_frame1 = DynamicFrame.fromDF(df_1, glueContext, "dynamic_frame1")

glueContext.write_dynamic_frame_from_options(

frame= dynamic_frame1,

connection_type="dynamodb",

connection_options={

    "dynamodb.output.tableName": "ledger_txn_g4",

    "dynamodb.throughput.write.percent": "100.0"

})

dynamic_frame2 = DynamicFrame.fromDF(df_2, glueContext, "dynamic_frame2")


# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame2,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://g4-project2", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

job.commit()
