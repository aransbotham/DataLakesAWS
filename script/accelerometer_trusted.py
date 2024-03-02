import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1709419631710 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://awsbucketanna/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1709419631710",
)

# Script generated for node Accelerometer
Accelerometer_node1709420458293 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://awsbucketanna/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_node1709420458293",
)

# Script generated for node SQL Query
SqlQuery0 = """
select distinct * from CustomerLanding
where shareWithResearchAsOfDate IS NOT NULL
"""
SQLQuery_node1709421434424 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"CustomerLanding": CustomerLanding_node1709419631710},
    transformation_ctx="SQLQuery_node1709421434424",
)

# Script generated for node Join to Opted In Users
JointoOptedInUsers_node1709420554980 = Join.apply(
    frame1=Accelerometer_node1709420458293,
    frame2=SQLQuery_node1709421434424,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JointoOptedInUsers_node1709420554980",
)

# Script generated for node Trusted Accelerometer
TrustedAccelerometer_node1709420602466 = glueContext.getSink(
    path="s3://awsbucketanna/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedAccelerometer_node1709420602466",
)
TrustedAccelerometer_node1709420602466.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
TrustedAccelerometer_node1709420602466.setFormat("json")
TrustedAccelerometer_node1709420602466.writeFrame(JointoOptedInUsers_node1709420554980)
job.commit()
