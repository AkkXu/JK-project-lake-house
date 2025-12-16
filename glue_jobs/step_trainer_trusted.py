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

# Script generated for node step_trainer_landing
step_trainer_landing_node1765873276839 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jk-project-lake-house/step_trainer_landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1765873276839")

# Script generated for node customers_curated
customers_curated_node1765803759025 = glueContext.create_dynamic_frame.from_catalog(database="aws-project", table_name="customers_curated", transformation_ctx="customers_curated_node1765803759025")

# Script generated for node SQL Query
SqlQuery3263 = '''
SELECT s.*
FROM s
INNER JOIN c
  ON s.serialNumber = c.serialnumber
'''
SQLQuery_node1765803768430 = sparkSqlQuery(glueContext, query = SqlQuery3263, mapping = {"s":step_trainer_landing_node1765873276839, "c":customers_curated_node1765803759025}, transformation_ctx = "SQLQuery_node1765803768430")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1765803768430, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1765803688868", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1765803771172 = glueContext.getSink(path="s3://jk-project-lake-house/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1765803771172")
AmazonS3_node1765803771172.setCatalogInfo(catalogDatabase="aws-project",catalogTableName="step_trainer_trusted")
AmazonS3_node1765803771172.setFormat("json")
AmazonS3_node1765803771172.writeFrame(SQLQuery_node1765803768430)
job.commit()