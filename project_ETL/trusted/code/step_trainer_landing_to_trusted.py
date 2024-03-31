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

# Script generated for node customer_trusted
customer_trusted_node1707070600667 = glueContext.create_dynamic_frame.from_catalog(
    database="practice",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1707070600667",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1707070611632 = glueContext.create_dynamic_frame.from_catalog(
    database="practice",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1707070611632",
)

# Script generated for node join SQL Query
SqlQuery2156 = """
select * from 
steptrainer join customer
on steptrainer.serialnumber == customer.serialnumber

"""
joinSQLQuery_node1707070616615 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2156,
    mapping={
        "steptrainer": step_trainer_landing_node1707070611632,
        "customer": customer_trusted_node1707070600667,
    },
    transformation_ctx="joinSQLQuery_node1707070616615",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1707070623833 = glueContext.getSink(
    path="s3://mpractice1908/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1707070623833",
)
step_trainer_trusted_node1707070623833.setCatalogInfo(
    catalogDatabase="practice", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1707070623833.setFormat("json")
step_trainer_trusted_node1707070623833.writeFrame(joinSQLQuery_node1707070616615)
job.commit()
