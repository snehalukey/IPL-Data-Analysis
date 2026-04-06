import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import date_format, to_date


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
AmazonS3_node1686485264611 = glueContext.create_dynamic_frame.from_catalog(
    database="rg_db",
    table_name="ipl_matches_2008_2022_csv",
    transformation_ctx="AmazonS3_node1686485264611",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "long", "id", "int"),
        ("overs", "long", "overs", "int"),
        ("bowler", "string", "bowler", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("total_run", "long", "total_run", "int"),
        ("player_out", "string", "player_out", "string"),
        ("battingteam", "string", "battingteam", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Change Schema
ChangeSchema_node1686485270740 = ApplyMapping.apply(
    frame=AmazonS3_node1686485264611,
    mappings=[
        ("id", "long", "id", "int"),
        ("date", "string", "date", "string"),
        ("season", "string", "season", "string"),
        ("team1", "string", "team1", "string"),
        ("team2", "string", "team2", "string"),
        ("venue", "string", "venue", "string"),
    ],
    transformation_ctx="ChangeSchema_node1686485270740",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1686485833292 = ApplyMapping.apply(
    frame=ChangeSchema_node1686485270740,
    mappings=[
        ("id", "int", "mid", "int"),
        ("date", "string", "date", "string"),
        ("season", "string", "season", "string"),
        ("team1", "string", "team1", "string"),
        ("team2", "string", "team2", "string"),
        ("venue", "string", "venue", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1686485833292",
)

# Script generated for node Join
Join_node1686485269622 = Join.apply(
    frame1=ApplyMapping_node2,
    frame2=RenamedkeysforJoin_node1686485833292,
    keys1=["id"],
    keys2=["mid"],
    transformation_ctx="Join_node1686485269622",
)

# Script generated for node Change Schema
ChangeSchema_node1686485271910 = ApplyMapping.apply(
    frame=Join_node1686485269622,
    mappings=[
        ("id", "int", "id", "int"),
        ("overs", "int", "overs", "int"),
        ("bowler", "string", "bowler", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("total_run", "int", "total_run", "int"),
        ("player_out", "string", "player_out", "string"),
        ("battingteam", "string", "battingteam", "string"),
        ("mid", "int", "mid", "int"),
        ("date", "string", "date", "string"),
        ("season", "string", "season", "string"),
        ("team1", "string", "team1", "string"),
        ("team2", "string", "team2", "string"),
        ("venue", "string", "venue", "string"),
    ],
    transformation_ctx="ChangeSchema_node1686485271910",
)

df = ChangeSchema_node1686485271910.toDF()
df.createOrReplaceTempView("table")

query = '''
    WITH CTE1 AS(
                SELECT 
                bowler,
                id,
                COUNT(DISTINCT overs) as Ov,
                SUM(total_run) as runsgiven,
                SUM(CASE
                    WHEN player_out NOT IN ('NA') THEN 1
                    ELSE 0
                    END) as Wickets,
                CASE
                    WHEN battingteam = team1 THEN team1
                    ELSE team2
                    END as Against,
                venue,
                date,
                season
                FROM table
                WHERE extra_type NOT IN ('byes', 'legbyes', 'penalty')
                GROUP BY bowler, id, season, date, venue, Against
    )
    SELECT
        ROW_NUMBER() OVER(PARTITION BY season ORDER BY runsgiven DESC) AS POS,
        bowler as Player,
        Ov,
        runsgiven as Runs,
        Wickets as Wkts,
        CASE
        WHEN Wickets > 0 THEN (Ov*6/Wickets)
        ELSE 0
        END AS SR,
        Against,
        venue,
        date as matdate,
        season
    FROM CTE1
'''

result = spark.sql(query)
result = result.withColumn("MatchDate", date_format(to_date(result["matdate"],'dd-mm-yyyy'), "dd-MMM-yy"))
result = result.select("POS","Player","Ov","Runs", "Wkts", "SR", "Against", "venue", "MatchDate", "season")
dynamic_frame = DynamicFrame.fromDF(result, glueContext, "dynamic_frame")

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://rg-prj-1/result/Req_5/",
        "partitionKeys": ["season"],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
