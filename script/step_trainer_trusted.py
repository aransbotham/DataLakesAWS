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
CustomerTrusted_node1709419631710 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://awsbucketanna/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1709419631710",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1709420458293 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://awsbucketanna/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1709420458293",
)

# Script generated for node Keep Columns Needed
SqlQuery0 = """
select distinct
st.*
from st
inner join ct 
ON st.serialNumber = st.serialNumber
"""
KeepColumnsNeeded_node1709422998378 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "ct": CustomerTrusted_node1709419631710,
        "st": StepTrainerLanding_node1709420458293,
    },
    transformation_ctx="KeepColumnsNeeded_node1709422998378",
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
TrustedStepTrainer_node1709420602466.writeFrame(KeepColumnsNeeded_node1709422998378)
job.commit()
