CREATE TABLE `job_reference_table`
(
  share_name STRING, -- name of the share sharename.schema.table
  date_loaded TIMESTAMP, -- Date timestamp when the dataproc job loaded data into the bigquery table
  status STRING, -- status of the job
  version_number INT64 -- latest version of delta table processed
);
