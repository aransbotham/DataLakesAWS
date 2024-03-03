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
AccelerometerTrusted_node1709480458906 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1709480458906",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1709480492306 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1709480492306",
)

# Script generated for node Join Tables
SqlQuery0 = """
select 
distinct
at.user,
at.x,
at.y,
at.z, 
stt.serialNumber,
stt.sensorReadingTime,
stt.distanceFromObject
from stt 
JOIN at ON stt.sensorReadingTime = at.timestamp
"""
JoinTables_node1709425337356 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "at": AccelerometerTrusted_node1709480458906,
        "stt": StepTrainerTrusted_node1709480492306,
    },
    transformation_ctx="JoinTables_node1709425337356",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1709425434722 = glueContext.getSink(
    path="s3://awsbucketanna/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1709425434722",
)
MachineLearningCurated_node1709425434722.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1709425434722.setFormat("json")
MachineLearningCurated_node1709425434722.writeFrame(JoinTables_node1709425337356)
job.commit()
