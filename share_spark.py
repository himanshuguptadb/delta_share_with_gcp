import delta_sharing
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, TimestampType, IntegerType
from datetime import datetime
from google.cloud import bigquery

#initialinzing bigquery client for python
client = bigquery.Client()

#initializing spark session
spark = SparkSession.builder.appName('share').getOrCreate()

#setting up temp bucket to query bigquery
bucket = "himanshu_bigquery_bucket"
spark.conf.set('temporaryGcsBucket', bucket)

share_file_path = 'gs://himanshu-fe-bucket/config.share'

first_time_load_version = 0
first_time_load = "N"
share_table_name = 'hg_churn.default.churn_customers'

job_reference_struct = StructType([
    StructField('share_name', StringType(), True),
    StructField('date_loaded', TimestampType(), True),
    StructField('status', StringType(), True),
    StructField('version_number', IntegerType(), True)
    ])

#Check if this is first time load or incremental

job_reference_table = spark.read.format('bigquery').option('table', 'fe-dev-sandbox.demos_himanshug.job_reference_table').load()
job_reference_table.createOrReplaceTempView('job_reference_table')


version_loaded = spark.sql(f"SELECT version_number FROM job_reference_table where share_name = '{share_table_name}' and status = 'SUCCESS' ORDER BY date_loaded desc Limit 1")
version_loaded.show()

if version_loaded.isEmpty():
  starting_version = first_time_load_version
  first_time_load = "Y"
else:
  starting_version = version_loaded.first()['version_number']

print(starting_version)

table_url = f"{share_file_path}#hg_churn.default.churn_customers"

if first_time_load=="Y":
  shared_df = delta_sharing.load_as_spark(table_url, version=starting_version)
  print(shared_df.count())
  shared_df.write.format('bigquery').option('table', 'fe-dev-sandbox.demos_himanshug.churn_customers').mode("overwrite").save()
  job_reference_DF = spark.createDataFrame(data=[(share_table_name, datetime.now(), "SUCCESS", starting_version)], schema=job_reference_struct)
  job_reference_DF.write.format('bigquery').option('table', 'fe-dev-sandbox.demos_himanshug.job_reference_table').mode("append").save()
else:
  print("Time for incremental")
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
                                                       'fe-dev-sandbox.demos_himanshug.job_reference_table').mode(
          "append").save()
  else:
      truncate_query = "truncate table fe-dev-sandbox.demos_himanshug.churn_customers_incr"
      truncate_table = client.query_and_wait(truncate_query)
      shared_df = shared_df.filter(shared_df["share_commit_version"] != starting_version)
      shared_df.write.format('bigquery').option('table', 'fe-dev-sandbox.demos_himanshug.churn_customers_incr').mode("overwrite").save()
      #select * from fe-dev-sandbox.demos_himanshug.churn_customers_incr order by share_commit_version, share_commit_timestamp asc
      merge_query = """
      MERGE fe-dev-sandbox.demos_himanshug.churn_customers T
      USING (SELECT *  FROM 
(SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY share_commit_version desc) as rn 
FROM fe-dev-sandbox.demos_himanshug.churn_customers_incr)  where rn=1 and share_change_type in ("update_postimage", "insert")) S
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
      delete_query = """
            delete from fe-dev-sandbox.demos_himanshug.churn_customers T where T.customer_id in 
             (SELECT customer_id  FROM 
      (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY share_commit_version desc) as rn 
      FROM fe-dev-sandbox.demos_himanshug.churn_customers_incr)  where rn=1 and share_change_type in ("delete"))
            """
      truncate_query = "truncate table fe-dev-sandbox.demos_himanshug.churn_customers_incr"
      merge_rows = client.query_and_wait(merge_query)  # Make an API request.
      delete_rows = client.query_and_wait(delete_query)  # Make an API request.

      job_reference_DF = spark.createDataFrame(data=[(share_table_name, datetime.now(), "SUCCESS", incr_version)],
                                           schema=job_reference_struct)
      job_reference_DF.write.format('bigquery').option('table', 'fe-dev-sandbox.demos_himanshug.job_reference_table').mode(
      "append").save()
