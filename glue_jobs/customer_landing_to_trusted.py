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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1765717019614 = glueContext.create_dynamic_frame.from_catalog(database="aws-project", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1765717019614")

# Script generated for node FilterConsent
SqlQuery2726 = '''
select * 
from myDataSource
WHERE shareWithResearchAsOfDate IS NOT NULL
'''
FilterConsent_node1765717374252 = sparkSqlQuery(glueContext, query = SqlQuery2726, mapping = {"myDataSource":AWSGlueDataCatalog_node1765717019614}, transformation_ctx = "FilterConsent_node1765717374252")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=FilterConsent_node1765717374252, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1765716930133", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1765717150906 = glueContext.getSink(path="s3://jk-project-lake-house/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1765717150906")
AmazonS3_node1765717150906.setCatalogInfo(catalogDatabase="aws-project",catalogTableName="customer_trusted")
AmazonS3_node1765717150906.setFormat("json")
AmazonS3_node1765717150906.writeFrame(FilterConsent_node1765717374252)
job.commit()