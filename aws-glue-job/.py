import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat_ws
from awsglue import DynamicFrame
from pyspark.sql.functions import regexp_replace

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1718995592881 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://reddit-engineer/raw/"], "recurse": True}, transformation_ctx="AmazonS3_node1718995592881")

# convert DynamicFrame to DataFrame
df = AmazonS3_node1718995592881.toDF()
df.drop_duplicates()

# remove special string in title
df = df.withColumn("title", regexp_replace(df["title"], ",", ""))

# concatenate the three columns into a single columns
df_combined = df.withColumn('ESS_updated', concat_ws('-',df['edited'],df['spoiler'],df['stickied']))
df_combined = df_combined.drop('edited', 'spoiler', 'stickied')

# convert back to DynamicFrame
S3bucket_node_combined = DynamicFrame.fromDF(df_combined, glueContext, 'S3bucket_node_combined')

# Script generated for node Amazon S3
AmazonS3_node1718995595602 = glueContext.write_dynamic_frame.from_options(frame=S3bucket_node_combined, connection_type="s3", format="csv",
    connection_options={"path": "s3://reddit-engineer/tranformed/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1718995595602")

job.commit()