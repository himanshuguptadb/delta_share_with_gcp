import delta_sharing
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, TimestampType, IntegerType
from datetime import datetime
from google.cloud import bigquery

#######Initialization############

#Big query client for python
client = bigquery.Client()

#Spark session
spark = SparkSession.builder.appName('share').getOrCreate()

#Parameters from dataproc job
bucket = spark.sparkContext.getConf().get('spark.executorEnv.bucket')
share_file_path = spark.sparkContext.getConf().get('spark.executorEnv.share_file_path')
share_table_name = spark.sparkContext.getConf().get('spark.executorEnv.share_table_name')
gcp_project_dataset = spark.sparkContext.getConf().get('spark.executorEnv.gcp_project_dataset')

#bucket = "himanshu_bigquery_bucket"
spark.conf.set('temporaryGcsBucket', bucket)

#share_file_path = 'gs://himanshu-fe-bucket/config.share'
# Default first time load settings
first_time_load_version = 0
first_time_load = "N"

#Dataframe structure for job reference table
job_reference_struct = StructType([
    StructField('share_name', StringType(), True),
    StructField('date_loaded', TimestampType(), True),
    StructField('status', StringType(), True),
    StructField('version_number', IntegerType(), True)
    ])

######Check if this is first time load or incremental#####

#create temp spark job reference table
job_reference_table = spark.read.format('bigquery') \
  .option('table', f'{gcp_project_dataset}.job_reference_table') \
  .load()
job_reference_table.createOrReplaceTempView('job_reference_table')

#Load the lagtest delta table version from last successful run
version_loaded = spark.sql(f"SELECT version_number FROM job_reference_table where share_name = '{share_table_name}' and status = 'SUCCESS' ORDER BY date_loaded desc Limit 1")
print("Last Version Loaded")
version_loaded.show()

if version_loaded.isEmpty():
  starting_version = first_time_load_version
  first_time_load = "Y"
else:
  starting_version = version_loaded.first()['version_number']

table_url = f"{share_file_path}#{share_table_name}"

if first_time_load=="Y":
  print("First time load")
  shared_df = delta_sharing.load_as_spark(table_url, version=starting_version)
  shared_df.write.format('bigquery').option('table', f'{gcp_project_dataset}.churn_customers').mode("overwrite").save()
  job_reference_DF = spark.createDataFrame(data=[(share_table_name, datetime.now(), "SUCCESS", starting_version)], schema=job_reference_struct)
  job_reference_DF.write.format('bigquery').option('table', f'{gcp_project_dataset}.job_reference_table').mode("append").save()
else:
  print("Incremental Load")
  shared_df = delta_sharing.load_table_changes_as_spark(table_url, starting_version=starting_version)
  shared_df = shared_df.withColumnRenamed("_CHANGE_TYPE","share_change_type")
  shared_df = shared_df.withColumnRenamed("_commit_version", "share_commit_version")
  shared_df = shared_df.withColumnRenamed("_commit_timestamp", "share_commit_timestamp")
  shared_df = shared_df.filter(shared_df["share_change_type"] != 'update_preimage')
  incr_version = shared_df.agg({"share_commit_version": "max"}).collect()[0]['max(share_commit_version)']
  if incr_version == starting_version:
      print("Nothing New to Load")
      job_reference_DF = spark.createDataFrame(data=[(share_table_name, datetime.now(), "Nothing New to Load", incr_version)],
                                               schema=job_reference_struct)
      job_reference_DF.write.format('bigquery').option('table',
                                                       f'{gcp_project_dataset}.job_reference_table').mode(
          "append").save()
  else:
      #truncating temp table"
      truncate_query = f"truncate table {gcp_project_dataset}.churn_customers_incr"
      truncate_table = client.query_and_wait(truncate_query)
      #removing the min inclusive version number
      shared_df = shared_df.filter(shared_df["share_commit_version"] != starting_version)
      #writing incremental data to temp table
      shared_df.write.format('bigquery').option('table', f'{gcp_project_dataset}.churn_customers_incr').mode("overwrite").save()
      #merge query to identify the latest record for each key across versions
      merge_query = f"""
      MERGE {gcp_project_dataset}.churn_customers T
      USING (SELECT *  FROM 
(SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY share_commit_version desc) as rn 
FROM {gcp_project_dataset}.churn_customers_incr)  where rn=1 and share_change_type in ("update_postimage", "insert")) S
      ON T.customer_id = S.customer_id
      WHEN NOT MATCHED THEN
      INSERT(customer_id, gender, senior_citizen, partner, dependents, tenure, phone_service, multiple_lines, internet_service, online_security, online_backup,device_protection,tech_support, streaming_tv, streaming_movies, contract, paperless_billing, payment_method, monthly_charges, total_charges, churn   )
      VALUES(customer_id, gender, senior_citizen, partner, dependents, tenure, phone_service, multiple_lines, internet_service, online_security, online_backup,device_protection,tech_support, streaming_tv, streaming_movies, contract, paperless_billing, payment_method, monthly_charges, total_charges, churn)
      WHEN MATCHED THEN
      UPDATE SET 
      gender = S.gender,
      senior_citizen = S.senior_citizen,
      partner = S.partner,
      dependents = S.dependents,
      tenure = S.tenure,
      phone_service = S.phone_service,
      multiple_lines = S.multiple_lines,
      internet_service = S.internet_service,
      online_security = S.online_security,
      online_backup = S.online_backup,
      device_protection = S.device_protection,
      tech_support = S.tech_support,
      streaming_tv = S.streaming_tv,
      streaming_movies = S.streaming_movies,
      contract = S.contract,
      paperless_billing = S.paperless_billing,
      payment_method = S.payment_method,
      monthly_charges = S.monthly_charges,
      total_charges = S.total_charges,
      churn = S.churn
      """
      #query to delete records based on cdc type
      delete_query = f"""
            delete from {gcp_project_dataset}.churn_customers T where T.customer_id in 
             (SELECT customer_id  FROM 
      (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY share_commit_version desc) as rn 
      FROM {gcp_project_dataset}.churn_customers_incr)  where rn=1 and share_change_type in ("delete"))
            """
      #executing merge and delte queries
      merge_rows = client.query_and_wait(merge_query)  # Make an API request.
      delete_rows = client.query_and_wait(delete_query)  # Make an API request.

      #inserting job status in reference table
      job_reference_DF = spark.createDataFrame(data=[(share_table_name, datetime.now(), "SUCCESS", incr_version)],
                                           schema=job_reference_struct)
      job_reference_DF.write.format('bigquery').option('table', f'{gcp_project_dataset}.job_reference_table').mode(
      "append").save()
