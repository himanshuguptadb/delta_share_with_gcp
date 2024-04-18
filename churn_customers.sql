CREATE TABLE churn_customers (
  customer_id STRING,
  gender STRING,
  senior_citizen BIGINT,
  partner STRING,
  dependents STRING,
  tenure BIGINT,
  phone_service STRING,
  multiple_lines STRING,
  internet_service STRING,
  online_security STRING,
  online_backup STRING,
  device_protection STRING,
  tech_support STRING,
  streaming_tv STRING,
  streaming_movies STRING,
  contract STRING,
  paperless_billing STRING,
  payment_method STRING,
  monthly_charges DOUBLE,
  total_charges STRING,
  churn STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableChangeDataFeed' = 'true')
