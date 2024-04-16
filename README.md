# Delta Share with GCP

Purpose of this solution is demostrate incremental data ingestion from delta share into Google cloud Big Query using Dataproc.

## Prerequisite / Setup
- Setup a delta share. In this solution, I have used customer churn dataset.
    - Create the table, enable CDC, insert data and create the share
- Setup a recepient and generate the credentials file.
- Create a single node Dataproc cluster and specify python package dependencies by setting up the properties as below
    - dataproc:pip.packages = delta-sharing==1.0.3,google-cloud-bigquery==3.20.1
- Create a bigquery dataset (equivalent to a database or schema)
- Create the "job_reference_table" in bigquery dataset. This table will be used as a watermark table to maintain status of lastest delta table version loaded
- Create a Google storage bucket to store credential file generated above and store the python program
- Create a Google Storage bucket required for query bigquery using spark or pandas API.

## First time load logic
![alt text](https://github.com/himanshuguptadb/delta_share_with_gcp/blob/main/First_Time_Load.png?raw=true)

## Incremental load logic
![alt text](https://github.com/himanshuguptadb/delta_share_with_gcp/blob/main/Incremental_Load.png?raw=true)
