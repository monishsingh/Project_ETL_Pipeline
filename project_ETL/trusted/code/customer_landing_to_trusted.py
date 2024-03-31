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

# Script generated for node customer_landing
customer_landing_node1707049106087 = glueContext.create_dynamic_frame.from_catalog(
    database="practice",
    table_name="customer_landing",
    transformation_ctx="customer_landing_node1707049106087",
)

# Script generated for node SQL Query
SqlQuery1974 = """
select * from myDataSource
where shareWithResearchAsOfDate is not null;

"""
SQLQuery_node1707052375781 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1974,
    mapping={"myDataSource": customer_landing_node1707049106087},
    transformation_ctx="SQLQuery_node1707052375781",
)

# Script generated for node customer_trusted zone
customer_trustedzone_node1707047704496 = glueContext.getSink(
    path="s3://mpractice1908/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trustedzone_node1707047704496",
)
customer_trustedzone_node1707047704496.setCatalogInfo(
    catalogDatabase="practice", catalogTableName="customer_trusted"
)
customer_trustedzone_node1707047704496.setFormat("json")
customer_trustedzone_node1707047704496.writeFrame(SQLQuery_node1707052375781)
job.commit()
