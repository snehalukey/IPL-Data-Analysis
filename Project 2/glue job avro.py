#final code for project 2 glue job number 2 (g4_p2_avro)

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
    table_name="project2_avro__1__avro",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1686636132099 = glueContext.create_dynamic_frame.from_catalog(
    database="g4_p2",
    table_name="acct_master_g4",
    transformation_ctx="AWSGlueDataCatalog_node1686636132099",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("no", "long", "no", "long"),
        ("tran_ref_id", "string", "tran_ref_id", "string"),
        ("transaction_dt", "string", "tdate", "string"),
        ("transaction_desc", "string", "transaction_desc", "string"),
        ("amt", "string", "amt", "string"),
        ("gst", "string", "gst", "string"),
        ("custom_duty", "string", "custom_duty", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)


df_data = ApplyMapping_node2.toDF() 
 
df_data1 = df_data.select(col("tdate").alias("txn_date"), col("amt").alias("txn_amt"), col("tran_ref_id").alias("source_system_txn_id"))
df_data1 = df_data1.withColumn("acc", lit("Product Sales"))
df_data2 = df_data.select(col("tdate").alias("txn_date"), col("gst").alias("txn_amt"), col("tran_ref_id").alias("source_system_txn_id"))
df_data2 = df_data2.withColumn("acc", lit("Goods and Service Tax"))
df_data3 = df_data.select(col("tdate").alias("txn_date"), col("custom_duty").alias("txn_amt"), col("tran_ref_id").alias("source_system_txn_id"))
df_data3 = df_data3.withColumn("acc", lit("Custom Duty"))

df_fin = df_data1.union(df_data2).union(df_data3)
df_fin = df_fin.withColumn("txn_id", concat(df_fin.txn_date, lit('_'), monotonically_increasing_id()))
df_fin = df_fin.withColumn("voucher_code", concat(col("source_system_txn_id"), lit("_"), lit("VC")))
df_fin = df_fin.withColumn("txn_type", lit("C"))
df_fin = df_fin.withColumn("source_system_id", lit("1"))

df = AWSGlueDataCatalog_node1686636132099.toDF()

df_inner = df.join(df_fin, df["acc_name"] == df_fin["acc"], "inner")
df_final = df_inner.select((col("txn_id")), col("voucher_code"), col("txn_type"), col("txn_date").alias("date"), col("acc_no"), col("txn_amt"), col("source_system_id"), col("source_system_txn_id"))
df_final = df_final.withColumn("txn_date",to_date(col('date'), 'dd-MMM-yy'))

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
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://g4-project2", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

job.commit()
