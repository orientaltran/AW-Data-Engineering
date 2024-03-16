import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer loading
Customerloading_node1710562614787 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://pj3oriental/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Customerloading_node1710562614787",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1710563125613 = Filter.apply(
    frame=Customerloading_node1710562614787,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1710563125613",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1710563334772 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node1710563125613,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://pj3oriental/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedCustomerZone_node1710563334772",
)

job.commit()
