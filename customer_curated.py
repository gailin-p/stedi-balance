import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1687176636490 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AmazonS3_node1687176636490",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="customer_trusted", transformation_ctx="S3bucket_node1"
)

# Script generated for node Customer_privacy_filter
Customer_privacy_filter_node1687176700084 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1687176636490,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Customer_privacy_filter_node1687176700084",
)

# Script generated for node Drop Fields
DropFields_node1687176827206 = DropFields.apply(
    frame=Customer_privacy_filter_node1687176700084,
    paths=["user", "x", "y", "z", "timestamp"],
    transformation_ctx="DropFields_node1687176827206",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687176827206,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://gpease-dataeng-class/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
