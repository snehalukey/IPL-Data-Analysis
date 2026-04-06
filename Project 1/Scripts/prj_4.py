import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="rg_db",
    table_name="ipl_ball_by_ball_2008_2022_csv",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1686311123009 = glueContext.create_dynamic_frame.from_catalog(
    database="rg_db",
    table_name="ipl_matches_2008_2022_csv",
    transformation_ctx="AmazonS3_node1686311123009",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "long", "id", "int"),
        ("innings", "long", "innings", "int"),
        ("overs", "long", "overs", "int"),
        ("bowler", "string", "bowler", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("total_run", "long", "total_run", "int"),
        ("iswicketdelivery", "long", "iswicketdelivery", "int"),
        ("kind", "string", "kind", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Change Schema
ChangeSchema_node1686311123809 = ApplyMapping.apply(
    frame=AmazonS3_node1686311123009,
    mappings=[("id", "long", "id", "int"), ("season", "string", "season", "string")],
    transformation_ctx="ChangeSchema_node1686311123809",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1686311538011 = ApplyMapping.apply(
    frame=ChangeSchema_node1686311123809,
    mappings=[("id", "int", "mid", "int"), ("season", "string", "season", "string")],
    transformation_ctx="RenamedkeysforJoin_node1686311538011",
)

# Script generated for node Join
Join_node1686311130833 = Join.apply(
    frame1=RenamedkeysforJoin_node1686311538011,
    frame2=ApplyMapping_node2,
    keys1=["mid"],
    keys2=["id"],
    transformation_ctx="Join_node1686311130833",
)

# Script generated for node Change Schema
ChangeSchema_node1686311134265 = ApplyMapping.apply(
    frame=Join_node1686311130833,
    mappings=[
        ("mid", "int", "mid", "int"),
        ("season", "string", "season", "string"),
        ("id", "int", "id", "int"),
        ("innings", "int", "innings", "int"),
        ("overs", "int", "overs", "int"),
        ("bowler", "string", "bowler", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("total_run", "int", "total_run", "int"),
        ("iswicketdelivery", "int", "iswicketdelivery", "int"),
        ("kind", "string", "kind", "string"),
    ],
    transformation_ctx="ChangeSchema_node1686311134265",
)

df = ChangeSchema_node1686311134265.toDF()
df.createOrReplaceTempView("table")

query="""
WITH CTE1 AS(
    SELECT 
    bowler,
    id, 
    COUNT(DISTINCT id) AS Matches, 
    COUNT(DISTINCT innings) AS Innings,
    SUM(CASE WHEN kind IN ("caught","caught and bowled","stumped","bowled","lbw") THEN 1
    ELSE 0
    END) AS Wickets, 
    SUM(total_run) AS Runsgiven,
    COUNT(DISTINCT overs) AS Overs,
    CASE
        WHEN CAST(SUM(isWicketDelivery) AS INT)=4 THEN 1
        ELSE 0
    END AS 4H,
    CASE
        WHEN CAST(SUM(isWicketDelivery) AS INT)>4 THEN 1
        ELSE 0
    END AS 5H, 
    season
    FROM table
    WHERE extra_type NOT IN ('byes', 'legbyes', 'penalty')
    GROUP BY bowler, id, season
)

SELECT 
ROW_NUMBER() OVER(ORDER BY SUM(Wickets) DESC) AS POS, 
bowler, 
SUM(Matches) AS Mat,
SUM(Innings) AS Inns, 
SUM(Wickets) AS Wkts,
SUM(Runsgiven) AS Runs, 
SUM(Overs) AS Ov, 
ROUND((SUM(Runsgiven)/CEILING(SUM(Overs))), 2) AS Econ,
SUM(4H) AS 4W,
 SUM(5H) AS 5W, 
season
FROM CTE1
GROUP BY bowler, season
"""

result = spark.sql(query).coalesce(1)
dynamic_frame = DynamicFrame.fromDF(result, glueContext, "dynamic_frame")

# Script generated for node Amazon S3
AmazonS3_node1686311146000 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://rg-prj-1/result/Req_4/",
        "partitionKeys": ["season"],
    },
    transformation_ctx="AmazonS3_node1686311146000",
)

job.commit()
