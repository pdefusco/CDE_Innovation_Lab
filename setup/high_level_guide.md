# HOL High Level Guide

This is a temporary High Level Guide summarizing each Lab in the HOL

## Summary

There are three parts to the HOL:

* Part 1: Spark with CDE Sessions and CDE Jobs
* Part 2: Iceberg with CDE Sessions
* Part 3: Running a Pipeline of CDE Jobs with the Airflow Editor

## Detailed Steps

#### Part 1: Spark with CDE Sessions and CDE Jobs

##### Spark with CDE Sessions

1. Launch a CDE Session with PySpark and default resource configurations
2. Copy and Paste the code located in the "part_01_spark" document in the step by step guides.
  - Make sure to update the username variable in the first cell to your assigned username e.g. "user010"
3. Run every cell and observe outputs

##### Spark with CDE Jobs

1. Create a CDE Files resource named after your user e.g. if you are user "user010" call it "CDE_Files_user010"
2. Open parameters.conf file located in cde_spark_jobs folder and update the storage location value for the mkt env storage location (provided to HOL participants by Workshop lead on day of event)
3. Upload all files located in the cde_spark_jobs folder to your CDE Files Resources
4. Create a CDE Python resource named after your user e.g. if you are user "user010" call it "CDE_Python_user010"
5. Uplaod the requirements.txt file to the CDE Python resource. The upload can take a few minutes but you can navigate away from the page and start building the next jobs if needed.
6. Create a CDE Spark Job with the following parameters:
  - Name: name this after your user e.g. if you are user "user010" call it "01_fraud_report_user010"
  - Application File: Select "01_fraud_report.py" from your CDE Files resource
  - Arguments: enter your username here, without quotes (just text) e.g. if you are user "user010" enter "user010" without quotes
  - Configurations: enter "spark.sql.autoBroadcastJoinThreshold" in the key field and "11M" without quotes in the value field
  - Python Environment: choose your CDE Python resource from the dropdown
  - Files & Resources: choose your CDE Files resource from the dropdown (this should have already been prefilled for you)
  - Compute Options: update Executor Cores toggle bar from 1 to 2; update Executor Memory toggle bar from 1 to 2 GB
  - Leave all other settings to default values and run the job.
7. Navigate to the CDE Job Runs UI and valudate Job runs. Explore advantages of CDE built-in Observability

#### Part 2: Iceberg with CDE Sessions

1. Launch a new CDE Session with PySpark and default resource configurations
2. Copy and Paste the code located in the "part_02_iceberg" document in the step by step guides.
  - Make sure to update the username variable in the first cell to your assigned username e.g. "user010"
  - Make sure to update the storageLocation varialbe in the first cell to the mkt env storage var e.g. "s3a://go01-demo/"
3. Run every cell and observe outputs

#### Part 3: Running a Pipeline of CDE Jobs with the Airflow Editor

Create but DO NOT RUN the following CDE Spark Jobs:
1. Data Validation:
  - Name: name this after your user e.g. if you are user "user010" call it "02_data_val_user010"
  - Application File: "02_data_validation.py" located in cde_spark_jobs folder
  - Arguments: enter your username here, without quotes (just text) e.g. if you are user "user010" enter "user010" without quotes
  - Python Environment: choose your CDE Python resource from the dropdown
  - Files & Resources: choose your CDE Files resource from the dropdown (this should have already been prefilled for you)
  - Leave all other settings to default values and run the job.
2. Customer Data Load:
  - Name: name this after your user e.g. if you are user "user010" call it "03_cust_data_user010"
  - Application File: "03_cust_data.py" located in cde_spark_jobs folder
  - Arguments: enter your username here, without quotes (just text) e.g. if you are user "user010" enter "user010" without quotes
  - Python Environment: choose your CDE Python resource from the dropdown
  - Files & Resources: choose your CDE Files resource from the dropdown (this should have already been prefilled for you)
  - Leave all other settings to default values and run the job.
3. Merge Transactions:
  - Name: name this after your user e.g. if you are user "user010" call it "04_merge_trx_user010"
  - Application File: "04_merge_trx.py" located in cde_spark_jobs folder
  - Arguments: enter your username here, without quotes (just text) e.g. if you are user "user010" enter "user010" without quotes
  - Files & Resources: choose your CDE Files resource from the dropdown (this should have already been prefilled for you)
  - Leave all other settings to default values and run the job.  
4. Incremental Report:
  - Name: name this after your user e.g. if you are user "user010" call it "05_inc_report_user010"
  - Application File: "04_incremental_report.py" located in cde_spark_jobs folder
  - Arguments: enter your username here, without quotes (just text) e.g. if you are user "user010" enter "user010" without quotes
  - Files & Resources: choose your CDE Files resource from the dropdown (this should have already been prefilled for you)
  - Leave all other settings to default values and run the job.  

Finally, create a CDE Airflow Job with the DAG provided in cde_airflow_jobs.
