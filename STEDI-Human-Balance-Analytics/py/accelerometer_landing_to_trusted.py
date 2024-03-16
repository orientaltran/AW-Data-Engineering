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
customer_trusted_node1710578919575 = glueContext.create_dynamic_frame.from_catalog(
    database="pj3oriental",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1710578919575",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1710579040335 = glueContext.create_dynamic_frame.from_catalog(
    database="pj3oriental",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1710579040335",
)

# Script generated for node join query
SqlQuery477 = """
select accelerometer_landing.* from accelerometer_landing join customer_trusted on customer_trusted.email = accelerometer_landing.user;
"""
joinquery_node1710579115474 = sparkSqlQuery(
    glueContext,
    query=SqlQuery477,
    mapping={
        "customer_trusted": customer_trusted_node1710578919575,
        "accelerometer_landing": accelerometer_landing_node1710579040335,
    },
    transformation_ctx="joinquery_node1710579115474",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1710579115474 = glueContext.getSink(
    path="s3://pj3oriental/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1710579115474",
)
accelerometer_trusted_node1710579115474.setCatalogInfo(
    catalogDatabase="pj3oriental", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1710579115474.setFormat("json")
accelerometer_trusted_node1710579115474.writeFrame(joinquery_node1710579115474)
job.commit()
