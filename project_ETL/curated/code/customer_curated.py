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
customer_trusted_node1707248228348 = glueContext.create_dynamic_frame.from_catalog(
    database="practice",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1707248228348",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1707248230251 = glueContext.create_dynamic_frame.from_catalog(
    database="practice",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1707248230251",
)

# Script generated for node Join SQL Query
SqlQuery2240 = """
select distinct customer.customerName,
customer.email,customer.phone,
customer.birthDay,customer.serialNumber,
customer.registrationDate,
customer.lastUpdateDate,
customer.shareWithResearchAsOfDate,
customer.shareWithPublicAsOfDate,
customer.shareWithFriendsAsOfDate
from customer join accelerometer
on accelerometer.user = customer.email
"""
JoinSQLQuery_node1707248234661 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2240,
    mapping={
        "accelerometer": accelerometer_trusted_node1707248230251,
        "customer": customer_trusted_node1707248228348,
    },
    transformation_ctx="JoinSQLQuery_node1707248234661",
)

# Script generated for node customer_curated
customer_curated_node1707248240601 = glueContext.getSink(
    path="s3://mpractice1908/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1707248240601",
)
customer_curated_node1707248240601.setCatalogInfo(
    catalogDatabase="practice", catalogTableName="customer_curated"
)
customer_curated_node1707248240601.setFormat("json")
customer_curated_node1707248240601.writeFrame(JoinSQLQuery_node1707248234661)
job.commit()
