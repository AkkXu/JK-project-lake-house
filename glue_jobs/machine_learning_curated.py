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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1765805275800 = glueContext.create_dynamic_frame.from_catalog(database="aws-project", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1765805275800")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1765805275548 = glueContext.create_dynamic_frame.from_catalog(database="aws-project", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1765805275548")

# Script generated for node SQL Query
SqlQuery2299 = '''
SELECT
  s.serialNumber,
  s.sensorReadingTime,
  s.distanceFromObject,
  a.user,
  a.x,
  a.y,
  a.z
FROM s
INNER JOIN a
  ON s.sensorReadingTime = a.timeStamp

'''
SQLQuery_node1765805280611 = sparkSqlQuery(glueContext, query = SqlQuery2299, mapping = {"s":step_trainer_trusted_node1765805275800, "a":accelerometer_trusted_node1765805275548}, transformation_ctx = "SQLQuery_node1765805280611")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1765805280611, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1765803688868", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1765805288099 = glueContext.getSink(path="s3://jk-project-lake-house/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1765805288099")
AmazonS3_node1765805288099.setCatalogInfo(catalogDatabase="aws-project",catalogTableName="machine_learning_curated")
AmazonS3_node1765805288099.setFormat("json")
AmazonS3_node1765805288099.writeFrame(SQLQuery_node1765805280611)
job.commit()