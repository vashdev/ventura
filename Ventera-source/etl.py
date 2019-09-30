'''
Program sets up data for first 5 challenges and executes the challenge sql

Prepares a seed value for ID column by querying target table max
reads source table and does transformation to target format
run command: C:\XXXXXX>%SPARK_HOME%\bin\spark-submit  etl.py
'''
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import boto3
import sys,os,json
import pyspark.sql.functions as func
spark = SparkSession.builder.appName("pySpark Ventera").getOrCreate()
 
# challenge 1 - 5 
                
Deptschema = StructType([
    StructField("DEPT_ID", IntegerType(), False),
    StructField("DEPARTMENT_NAME", StringType(), True),
    StructField("MANAGER", StringType(), True)
    ])
Salesmanschema = StructType([
    StructField("SALESMAN_ID", IntegerType(), False),
    StructField("SALESMAN_NAME", StringType(), True),
    StructField("DEPT_ID", IntegerType(), False)
    ])
OrdersSchema = StructType([
    StructField("ORDER_ID", IntegerType(), False),
    StructField("SALES_DATE", DateType(), True),
    StructField("ORDER_AMOUNT", DoubleType(), True),
    StructField("CUST_ID", IntegerType(), False),
    StructField("SALESMAN_ID", IntegerType(), False)
    ])
Custschema = StructType([
    StructField("CUST_ID", IntegerType(), False),
    StructField("CUST_NAME", StringType(), True),
    StructField("ADDRESS", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    ])
	
Custschema = StructType([
    StructField("CUST_ID", IntegerType(), False),
    StructField("CUST_NAME", StringType(), True),
    StructField("ADDRESS", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    ])
# load sample data` 
deptDF=spark.read.format("csv").option("header", "true").schema(Deptschema).load("../VenteraInputData/Department.csv")
deptDF.collect()
SalesmansDF=spark.read.format("csv").option("header", "true").schema(Salesmanschema).load("../VenteraInputData/Salesman.csv")
SalesmansDF.collect()
OrdersDF=spark.read.format("csv").option("header", "true").schema(OrdersSchema).load("../VenteraInputData/Orders.csv")
OrdersDF.collect()
CustDF=spark.read.format("csv").option("header", "true").schema(Custschema).load("../VenteraInputData/Customer.csv")
CustDF.collect()

# prep for  SQL 
deptDF.registerTempTable("dept")
SalesmansDF.registerTempTable("salesman")
OrdersDF.registerTempTable("orders")
CustDF.registerTempTable("cust")

#1.	Write SQL Query to list all the Sales for the state of California in the year 2018

spark.sql(" select * from orders  INNER JOIN \
                        cust  USING(CUST_ID) where STATE='CA' and year(SALES_DATE)=='2018'").show()
						
#2.	Write SQL query to list Sales amount for each department in the year 2018 sorted by the sales amount (Show all the department even if it did not make any sales)

spark.sql("select DEPT_ID , sum(totalsalesbydept) from ( select DEPT_ID,SALES_DATE, CASE  WHEN year(SALES_DATE)=='2018' THEN sum(ORDER_AMOUNT) else SUM(0) END  as totalsalesbydept  from orders  INNER JOIN \
                        salesman USING(SALESMAN_ID)  INNER JOIN dept using(DEPT_ID)  group by  DEPT_ID,SALES_DATE) t  group by t.DEPT_ID   ").show()

#3 3.	Write SQL to list all the salesman who did not make any sales in 2018

spark.sql(" select * from salesman inner join orders USING(SALESMAN_ID)  where year(SALES_DATE) <> '2018' ").show()

# 4.	Write SQL to list Top 10 Salesman in the Year 2018 based on the sales

spark.sql(" select SALESMAN_ID,  SALESMAN_NAME,totalsales, DENSE_RANK() OVER( ORDER BY totalsales desc ) ranking from (\
select SALESMAN_ID,  sum(ORDER_AMOUNT) totalsales from salesman inner join orders USING(SALESMAN_ID) where year(SALES_DATE) == '2018' group by SALESMAN_ID ) sales inner join salesman USING(SALESMAN_ID)   LIMIT 10 ").show()

#5.	Write SQL to list Top 10 Customers in the Year 2018 based on the sales
spark.sql(" select CUST_ID,  CUST_NAME,totalpurchase, DENSE_RANK() OVER( ORDER BY totalpurchase desc ) ranking from (\
select CUST_ID,  sum(ORDER_AMOUNT) totalpurchase from cust inner join orders USING(CUST_ID) where year(SALES_DATE) == '2018' group by CUST_ID ) sales inner join cust USING(CUST_ID)   LIMIT 10 ").show()


# problem 6 and 7
# set Red Shift /se credentials  
 
os.environ['aws_access_key_id']='XXXXXXXXXXXX'
os.environ['aws_secret_access_key']='XXXXXXXXXXXXXXXXXXXXX'

s3= boto3.client(
    's3',
    aws_access_key_id=os.environ['aws_access_key_id'],
    aws_secret_access_key=os.environ['aws_secret_access_key']
)
#get  Cluster credentials
obj = s3.get_object(Bucket="vashdevl", Key="config/redshiftdb.json")
body = obj['Body'].read().decode('utf-8')
crednts=json.loads(body)

url=crednts["jdbcurl"]+"?user="+crednts["user"]+"&password="+crednts["password"]
spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['aws_access_key_id'])
spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['aws_secret_access_key'])

# unload from Redshift
srcdf = spark.read.format("com.databricks.spark.redshift").option("url", url).option("dbtable", "TEST_MSR_SOURCE").option("tempdir", "s3n://vashdevl/data/work").option("forward_spark_s3_credentials", True).load()
tgtdf=spark.read.format("com.databricks.spark.redshift").option("url", url).option("dbtable", "TEST_MSR_TARGET").option("tempdir", "s3n://vashdevl/data/work").option("forward_spark_s3_credentials", True).load()

# SQL prep 
srcdf.registerTempTable("srctbl")
tgtdf.registerTempTable("tgtbl")
srcdf.printSchema()
maxrdd=spark.sql(" select MAX(TEST_MSR_TARGET_ID) from tgtbl").collect()
maxid=maxrdd[0]["max(TEST_MSR_TARGET_ID)"]
spark.sql(" select * from srctbl").show()
 
# Always fwd  ...
if maxid is None or maxid <1:
   maxid=1
print( " max id = %i" % maxid )

# prepare for loading ...

transformdata=spark.sql(" select monotonically_increasing_id() +"+str(maxid)+" AS TEST_MSR_TARGET_ID ,rpt_grp_cd ,lctn_typ_cd ,clctn_prd_txt ,msr_cd ,to_date(clcltn_date ,'MM/dd/yyyy') as clcltn_date ,CAST(grp_rate_nmrtr as INT) as grp_rate_nmrtr,CAST(grp_rate_dnmntr as INT) as grp_rate_dnmntr,file_name , NVL(to_timestamp(creat_ts,'MM/dd/yyyy'), current_timestamp() ) as creat_ts,NVL(creat_user_id ,'NA') as creat_user_id,submsn_cmplt_cd   from srctbl")
transformdata.show()
transformdata.write \
  .format("com.databricks.spark.redshift") \
  .option("url", url) \
  .option("dbtable", "TEST_MSR_TARGET") \
  .option("tempdir", "s3n://vashdevl/data/work") \
  .mode("append") \
  .save()
  
print(" Success  ..")
spark.stop()