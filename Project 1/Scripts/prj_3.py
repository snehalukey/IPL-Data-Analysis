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
AmazonS3_node1686305924900 = glueContext.create_dynamic_frame.from_catalog(
    database="rg_db",
    table_name="ipl_matches_2008_2022_csv",
    transformation_ctx="AmazonS3_node1686305924900",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "long", "id", "int"),
        ("innings", "long", "innings", "int"),
        ("ballnumber", "long", "ballnumber", "int"),
        ("batter", "string", "batter", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("batsman_run", "long", "batsman_run", "int"),
        ("player_out", "string", "player_out", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Change Schema
ChangeSchema_node1686305927460 = ApplyMapping.apply(
    frame=AmazonS3_node1686305924900,
    mappings=[("id", "long", "id", "int"), ("season", "string", "season", "string")],
    transformation_ctx="ChangeSchema_node1686305927460",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1686306093398 = ApplyMapping.apply(
    frame=ChangeSchema_node1686305927460,
    mappings=[("id", "int", "mid", "int"), ("season", "string", "season", "string")],
    transformation_ctx="RenamedkeysforJoin_node1686306093398",
)

# Script generated for node Join
Join_node1686305929373 = Join.apply(
    frame1=RenamedkeysforJoin_node1686306093398,
    frame2=ApplyMapping_node2,
    keys1=["mid"],
    keys2=["id"],
    transformation_ctx="Join_node1686305929373",
)

# Script generated for node Change Schema
ChangeSchema_node1686306179624 = ApplyMapping.apply(
    frame=Join_node1686305929373,
    mappings=[
        ("mid", "int", "mid", "int"),
        ("season", "string", "season", "string"),
        ("id", "int", "id", "int"),
        ("innings", "int", "innings", "int"),
        ("ballnumber", "int", "ballnumber", "int"),
        ("batter", "string", "batter", "string"),
        ("batsman_run", "int", "batsman_run", "int"),
        ("extra_type", "string", "extra_type", "string"),
        ("player_out", "string", "player_out", "string"),
    ],
    transformation_ctx="ChangeSchema_node1686306179624",
)

df = ChangeSchema_node1686306179624.toDF()
df.createOrReplaceTempView("table")

query = '''
            WITH DATA AS(
    SELECT BATTER, ID, COUNT(DISTINCT ID) AS MATCHES, COUNT(DISTINCT INNINGS) AS INNINGS,
    COUNT(DISTINCT INNINGS) - SUM(
        CASE
            WHEN BATTER=PLAYER_OUT THEN 1
            ELSE 0
        END
    ) AS NO,
    SUM(
        CASE
            WHEN EXTRA_TYPE IN ("noballs", "NA", "byes", "legbyes", "penalty") THEN 1
            ELSE 0
        END
    ) AS BF,
    SUM(
        CASE
            WHEN BATSMAN_RUN = 4 THEN 1
            ELSE 0
        END
    ) AS FOUR,
    SUM(
        CASE
            WHEN BATSMAN_RUN = 6 THEN 1
            ELSE 0
        END
    ) AS SIX,
    CAST(SUM(BATSMAN_RUN) AS INT) AS RUNS,
    FLOOR(SUM(BATSMAN_RUN)/100) AS CS,
    FLOOR(SUM(BATSMAN_RUN)/50) AS FS, 
    SEASON
    FROM table
    GROUP BY BATTER, ID, SEASON
)

SELECT ROW_NUMBER() OVER(ORDER BY SUM(RUNS) DESC) AS POS,
BATTER as Player,
SUM(MATCHES) AS Mat, 
SUM(INNINGS) AS Inns,
SUM(NO) AS NO,
SUM(RUNS) AS Runs,
MAX(RUNS) AS HS, 
ROUND((SUM(RUNS)/(SUM(INNINGS)-SUM(NO))), 2) AS AVG,
SUM(BF) BF, 
CAST((SUM(RUNS)/SUM(BF))*100 AS DECIMAL(10, 2)) AS SR,
SUM(CS) AS `100`, 
SUM(FS) AS `50`,
SUM(FOUR) AS `4s`, 
SUM(SIX) AS `6s`, 
season
FROM DATA
GROUP BY BATTER, season
'''
result = spark.sql(query).coalesce(1)
dynamic_frame = DynamicFrame.fromDF(result, glueContext, "dynamic_frame")

# Script generated for node Amazon S3
AmazonS3_node1686306222320 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://rg-prj-1/result/Req_3/",
        "partitionKeys": ["season"],
    },
    transformation_ctx="AmazonS3_node1686306222320",
)

job.commit()
