import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1765872756780 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jk-project-lake-house/accelerometer_landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1765872756780")

# Script generated for node customer_trusted
customer_trusted_node1765802004664 = glueContext.create_dynamic_frame.from_catalog(database="aws-project", table_name="customer_trusted", transformation_ctx="customer_trusted_node1765802004664")

# Script generated for node Join
Join_node1765802008072 = Join.apply(frame1=AmazonS3_node1765872756780, frame2=customer_trusted_node1765802004664, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1765802008072")

# Script generated for node SQL Query
SqlQuery3208 = '''
select * from myDataSource
'''
SQLQuery_node1765802008967 = sparkSqlQuery(glueContext, query = SqlQuery3208, mapping = {"myDataSource":Join_node1765802008072}, transformation_ctx = "SQLQuery_node1765802008967")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1765802008967, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1765801988529", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1765802018033 = glueContext.getSink(path="s3://jk-project-lake-house/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1765802018033")
AmazonS3_node1765802018033.setCatalogInfo(catalogDatabase="aws-project",catalogTableName="accelerometer_trusted")
AmazonS3_node1765802018033.setFormat("json")
AmazonS3_node1765802018033.writeFrame(SQLQuery_node1765802008967)
job.commit()