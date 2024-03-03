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

# Script generated for node Customer Trusted
CustomerTrusted_node1709479155183 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1709479155183",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1709479205248 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1709479205248",
)

# Script generated for node Join
SqlQuery0 = """
select distinct
st.*
from stl
inner join ct 
ON st.serialNumber = st.serialNumber
"""
Join_node1709422998378 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "ct": CustomerTrusted_node1709479155183,
        "stl": StepTrainerLanding_node1709479205248,
    },
    transformation_ctx="Join_node1709422998378",
)

# Script generated for node Trusted Step Trainer
TrustedStepTrainer_node1709420602466 = glueContext.getSink(
    path="s3://awsbucketanna/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedStepTrainer_node1709420602466",
)
TrustedStepTrainer_node1709420602466.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
TrustedStepTrainer_node1709420602466.setFormat("json")
TrustedStepTrainer_node1709420602466.writeFrame(Join_node1709422998378)
job.commit()
