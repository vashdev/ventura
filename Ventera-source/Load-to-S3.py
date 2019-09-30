'''
simple script to load csv to redshift 
credentials are stored and pulled from  s3
'''
import psycopg2  
import logging
import boto3
import sys,os,json

#s3 
s3= boto3.client(
    's3',
    aws_access_key_id=os.environ['aws_access_key_id'],
    aws_secret_access_key=os.environ['aws_secret_access_key']
)

obj = s3.get_object(Bucket="vashdevl", Key="config/redshiftdb.json")
body = obj['Body'].read().decode('utf-8')
crednts=json.loads(body)
print(json.dumps(crednts))
print(" current workign dir")
print(os.getcwd())

# load file to s3 from relative path
s3response = s3.upload_file("..\VenteraInputData\python_test.csv", "BKT", "data/python_test.csv")

#db connection
def getconn(dbName,user,pswd,port,enpoint):
    conn=psycopg2.connect(dbname=dbName,user=user,password=pswd,port=port,host=enpoint)
    conn.set_session(autocommit=True)
    return conn
conn=getconn(crednts["db"],crednts["user"],crednts["password"],crednts["port"],crednts["endpoint"])
cur=conn.cursor()

# COPY DATA TO RedShift from staging S3
ldsql="""copy {}.{} from '{}'\
        credentials \
        'aws_access_key_id={};aws_secret_access_key={}' \
        DELIMITER ',' ACCEPTINVCHARS EMPTYASNULL ESCAPE COMPUPDATE OFF  IGNOREHEADER 1;commit;"""\
        .format("public", "TEST_MSR_SOURCE", "s3://BKT/data/python_test.csv", os.environ['aws_access_key_id'], os.environ['aws_secret_access_key'])

try:
    cur.execute(ldsql)
except:
      print("Failed to execute copy command")

    
if conn is not None:
      conn.close()

