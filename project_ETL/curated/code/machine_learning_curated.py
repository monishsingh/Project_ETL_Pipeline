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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1707251575100 = glueContext.create_dynamic_frame.from_catalog(
    database="practice",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1707251575100",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1707251570105 = glueContext.create_dynamic_frame.from_catalog(
    database="practice",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1707251570105",
)

# Script generated for node join SQL Query
SqlQuery2184 = """
select * 
from steptrainer join accelerometer
on steptrainer.sensorReadingTime == accelerometer.timeStamp

"""
joinSQLQuery_node1707251578905 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2184,
    mapping={
        "steptrainer": step_trainer_trusted_node1707251575100,
        "accelerometer": accelerometer_trusted_node1707251570105,
    },
    transformation_ctx="joinSQLQuery_node1707251578905",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1707251583752 = glueContext.getSink(
    path="s3://mpractice1908/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1707251583752",
)
machine_learning_curated_node1707251583752.setCatalogInfo(
    catalogDatabase="practice", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1707251583752.setFormat("json")
machine_learning_curated_node1707251583752.writeFrame(joinSQLQuery_node1707251578905)
job.commit()
