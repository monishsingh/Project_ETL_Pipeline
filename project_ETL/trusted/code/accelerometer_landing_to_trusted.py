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

# Script generated for node accelerometer_landing
accelerometer_landing_node1707068568008 = glueContext.create_dynamic_frame.from_catalog(
    database="practice",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1707068568008",
)

# Script generated for node customer_trusted
customer_trusted_node1707068565613 = glueContext.create_dynamic_frame.from_catalog(
    database="practice",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1707068565613",
)

# Script generated for node Join SQL Query
SqlQuery2085 = """
select accelerometer.user, accelerometer.timestamp,
accelerometer.x,accelerometer.y,accelerometer.z 
from accelerometer join customer 
on accelerometer.user == customer.email; 

"""
JoinSQLQuery_node1707068590788 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2085,
    mapping={
        "accelerometer": accelerometer_landing_node1707068568008,
        "customer": customer_trusted_node1707068565613,
    },
    transformation_ctx="JoinSQLQuery_node1707068590788",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1707068597925 = glueContext.getSink(
    path="s3://mpractice1908/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1707068597925",
)
accelerometer_trusted_node1707068597925.setCatalogInfo(
    catalogDatabase="practice", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1707068597925.setFormat("json")
accelerometer_trusted_node1707068597925.writeFrame(JoinSQLQuery_node1707068590788)
job.commit()
