import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
'''
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
'''

from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("GlueJoBDemo").getOrCreate()

def main():
    SNOWFLAKE_SOURCE_NAME = 'net.snowflake.spark.snowflake'
    snowflake_database='MANAGE_DB'
    snowflake_schema='IPHONE_DATA_ANALYSIS'
    snowflake_table_name='IPHONE_DATA'
    snowflake_options={
        "sfUrl" :"https://dyxbhjg-gc20234.snowflakecomputing.com",
        "sfUser":"avinashkumar",
        "sfPassword":"********",
        "sfDatabase":snowflake_database,
        "sfSchema":snowflake_schema,
        "sfWarehouse":"COMPUTE_WH"
        
    }
    df=spark.read\
        .format(SNOWFLAKE_SOURCE_NAME)\
        .options(**snowflake_options)\
        .option("dbtable",snowflake_database+"."+snowflake_schema+"."+snowflake_table_name)\
        .load()
    df1=df.groupBy("RAM").sum("SALE_PRICE")
    df1.write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable","Destination_table").mode("overwrite")\
        .save()
    
    
main()

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
