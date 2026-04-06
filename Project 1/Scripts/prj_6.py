import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import date_format,to_date

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
AmazonS3_node1686631510296 = glueContext.create_dynamic_frame.from_catalog(
    database="rg_db",
    table_name="ipl_matches_2008_2022_csv",
    transformation_ctx="AmazonS3_node1686631510296",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "long", "id", "int"),
        ("ballnumber", "long", "ballnumber", "int"),
        ("batter", "string", "batter", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("batsman_run", "long", "batsman_run", "int"),
        ("battingteam", "string", "battingteam", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Change Schema
ChangeSchema_node1686631512464 = ApplyMapping.apply(
    frame=AmazonS3_node1686631510296,
    mappings=[
        ("id", "long", "id", "int"),
        ("date", "string", "date", "string"),
        ("season", "string", "season", "string"),
        ("team1", "string", "team1", "string"),
        ("team2", "string", "team2", "string"),
        ("venue", "string", "venue", "string"),
    ],
    transformation_ctx="ChangeSchema_node1686631512464",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1686631915743 = ApplyMapping.apply(
    frame=ChangeSchema_node1686631512464,
    mappings=[
        ("id", "int", "mid", "int"),
        ("date", "string", "date", "string"),
        ("season", "string", "season", "string"),
        ("team1", "string", "team1", "string"),
        ("team2", "string", " team2", "string"),
        ("venue", "string", "venue", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1686631915743",
)

# Script generated for node Join
Join_node1686631513688 = Join.apply(
    frame1=ApplyMapping_node2,
    frame2=RenamedkeysforJoin_node1686631915743,
    keys1=["id"],
    keys2=["mid"],
    transformation_ctx="Join_node1686631513688",
)

# Script generated for node Change Schema
ChangeSchema_node1686631512752 = ApplyMapping.apply(
    frame=Join_node1686631513688,
    mappings=[
        ("id", "int", "id", "int"),
        ("ballnumber", "int", "ballnumber", "int"),
        ("batter", "string", "batter", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("batsman_run", "int", "batsman_run", "int"),
        ("battingteam", "string", "battingteam", "string"),
        ("mid", "int", "mid", "int"),
        ("date", "string", "date", "string"),
        ("season", "string", "season", "string"),
        ("team1", "string", "team1", "string"),
        ("team2", "string", "team2", "string"),
        ("venue", "string", "venue", "string"),
    ],
    transformation_ctx="ChangeSchema_node1686631512752",
)

df = ChangeSchema_node1686631512752.toDF()
df.createOrReplaceTempView("table")

query = '''
    WITH cte1 AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY season, date, batter ORDER BY batter) AS ballcount,
            SUM(batsman_run) OVER (PARTITION BY season, date, batter ORDER BY batter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS runstillfifty
            FROM table
            WHERE extra_type NOT IN ('wides')
    ),
    cte2 AS (
        SELECT
            *,
            CASE WHEN batsman_run = 4 THEN 1 ELSE 0 END AS `4s`,
            CASE WHEN batsman_run = 6 THEN 1 ELSE 0 END AS `6s`,
            CASE WHEN runstillfifty >= 50 THEN ballcount ELSE NULL END as ballcountoverfifty,
            CASE WHEN battingteam = team1 THEN team2 ELSE team1 END AS Against
        FROM cte1
    )
        SELECT
            ROW_NUMBER() OVER (PARTITION BY season order by MIN(ballcountoverfifty), SUM(batsman_run) DESC ) AS POS,
            season,
            date AS matdate,
            batter AS Player,
            Against,
            venue,
            MIN(ballcountoverfifty) AS BF,
            SUM(`4s`) AS `4s`,
            SUM(`6s`) AS `6s`,
            SUM(batsman_run) AS Runs
        FROM cte2
        GROUP BY season, date, batter, Against, venue
        HAVING MIN(ballcountoverfifty) IS NOT NULL
        ORDER BY BF, Runs DESC
'''

result = spark.sql(query)
result = result.withColumn("MatchDate", date_format(to_date(result["matdate"],'dd-mm-yyyy'), "dd-MMM-yy"))
result = result.select("POS","Player","Runs", "BF", "4s", "6s", "Against", "venue", "MatchDate", "season")
result = result.coalesce(1)
dynamic_frame = DynamicFrame.fromDF(result, glueContext, "dynamic_frame")

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://rg-prj-1/results/Req_2/",
        "partitionKeys": ["season"],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
