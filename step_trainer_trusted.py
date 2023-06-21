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
    table_name="step_trainer_landing",
    transformation_ctx="AmazonS3_node1687176636490",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Customer_privacy_filter
Customer_privacy_filter_node1687176700084 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1687176636490,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Customer_privacy_filter_node1687176700084",
)

# Script generated for node Drop Fields
DropFields_node1687176827206 = DropFields.apply(
    frame=Customer_privacy_filter_node1687176700084,
    paths=["x", "y", "z", "timestamp", "user"],
    transformation_ctx="DropFields_node1687176827206",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://gpease-dataeng-class/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1687176827206)
job.commit()
