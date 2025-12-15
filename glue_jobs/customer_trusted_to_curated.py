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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1765802699830 = glueContext.create_dynamic_frame.from_catalog(database="aws-project", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1765802699830")

# Script generated for node customer_trusted
customer_trusted_node1765802700382 = glueContext.create_dynamic_frame.from_catalog(database="aws-project", table_name="customer_trusted", transformation_ctx="customer_trusted_node1765802700382")

# Script generated for node SQL Query
SqlQuery2842 = '''
SELECT DISTINCT c.*
FROM c
INNER JOIN a
  ON c.email = a.user
'''
SQLQuery_node1765802705599 = sparkSqlQuery(glueContext, query = SqlQuery2842, mapping = {"a":accelerometer_trusted_node1765802699830, "c":customer_trusted_node1765802700382}, transformation_ctx = "SQLQuery_node1765802705599")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1765802705599, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1765802656634", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1765802706831 = glueContext.getSink(path="s3://jk-project-lake-house/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1765802706831")
AmazonS3_node1765802706831.setCatalogInfo(catalogDatabase="aws-project",catalogTableName="customers_curated")
AmazonS3_node1765802706831.setFormat("json")
AmazonS3_node1765802706831.writeFrame(SQLQuery_node1765802705599)
job.commit()