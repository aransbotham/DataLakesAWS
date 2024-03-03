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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1709424577806 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://awsbucketanna/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1709424577806",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1709424449803 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://awsbucketanna/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1709424449803",
)

# Script generated for node Keep Customers with Accelerometer Data
SqlQuery0 = """
select distinct
ct.*
from ct 
JOIN at 
ON ct.email = at.user
"""
KeepCustomerswithAccelerometerData_node1709424633360 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "at": AccelerometerTrusted_node1709424577806,
        "ct": CustomerTrusted_node1709424449803,
    },
    transformation_ctx="KeepCustomerswithAccelerometerData_node1709424633360",
)

# Script generated for node Customers Curated
CustomersCurated_node1709424731513 = glueContext.getSink(
    path="s3://awsbucketanna/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomersCurated_node1709424731513",
)
CustomersCurated_node1709424731513.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customers_curated"
)
CustomersCurated_node1709424731513.setFormat("glueparquet", compression="snappy")
CustomersCurated_node1709424731513.writeFrame(
    KeepCustomerswithAccelerometerData_node1709424633360
)
job.commit()
