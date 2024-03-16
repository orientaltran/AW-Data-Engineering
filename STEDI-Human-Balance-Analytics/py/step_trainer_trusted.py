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

# Script generated for node step_trainer_landing
step_trainer_landing_node1710579941177 = glueContext.create_dynamic_frame.from_catalog(
    database="pj3oriental",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1710579941177",
)

# Script generated for node customer_trusted_to_curated
customer_trusted_to_curated_node1710580010855 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="pj3oriental",
        table_name="customer_trusted_to_curated",
        transformation_ctx="customer_trusted_to_curated_node1710580010855",
    )
)

# Script generated for node SQL Query
SqlQuery580= """
select * 
from step_trainer_landing where serialnumber in (select serialnumber from customer_trusted_to_curated);
"""
SQLQuery_node1710580075143= sparkSqlQuery(
    glueContext,
    query=SqlQuery580,
    mapping={
        "step_trainer_landing": step_trainer_landing_node1710579941177,
        "customer_trusted_to_curated": customer_trusted_to_curated_node1710580010855,
    },
    transformation_ctx="SQLQuery_node1710580075143",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1710580194239 = glueContext.getSink(
    path="s3://pj3oriental/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1710580194239",
)
step_trainer_trusted_node1710580194239.setCatalogInfo(
    catalogDatabase="pj3oriental", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1710580194239.setFormat("json")
step_trainer_trusted_node1710580194239.writeFrame(SQLQuery_node1710580075143)
job.commit()
