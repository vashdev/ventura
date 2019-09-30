Environment: python 3.6 / Pyspark 2.1 Redshift and S3  

Load-to-S3 : this script copies the input csv to table created with the provided DDL  
redshiftdb.json: Stores cluster related information along with tmp dir location used by copy command in s3
etl.py : takes mock data for the first 5 challenge questions , loads and executes these SQLs. 
         Next two questions are addressed by reading source table and target table for existing max id. This is used to seed   next set of record loads.
		 The data is Xformed to target via spark sql and loaded back using copy command.

For question 8:
We are already using copy command to push dat to redshift  via spark sql  which uses DF partitioned based on  number of cores locally or worker nodes in cluster.
We dont have to change much , i was able to load it 4 way partitioned for 4 cores  on my laptop . May be if data is in very very huge then landing data from source in parquet and then processing might help


