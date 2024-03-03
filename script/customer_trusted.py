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

# Script generated for node Remove Records with NULL Research Share Date
SqlQuery0 = """
select distinct 
*
from CustomerLanding
where shareWithResearchAsOfDate IS NOT NULL
"""
RemoveRecordswithNULLResearchShareDate_node1709421271238 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"CustomerLanding": CustomerLanding_node1709419631710},
    transformation_ctx="RemoveRecordswithNULLResearchShareDate_node1709421271238",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1709419829008 = glueContext.getSink(
    path="s3://awsbucketanna/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1709419829008",
)
CustomerTrusted_node1709419829008.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1709419829008.setFormat("json")
CustomerTrusted_node1709419829008.writeFrame(
    RemoveRecordswithNULLResearchShareDate_node1709421271238
)
job.commit()
