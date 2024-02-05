# CDE Demo Auto Deploy

## Objective

This git repository hosts the automation for a CDE Demo that includes Spark, Airflow and Iceberg. The Demo is deployed and removed in your Virtual Cluster within minutes.

## Table of Contents

* [Requirements]
* [Deployment Instructions]
  * [1. Important Information]
  * [2. autodeploy.sh]
* [Summary]

## Requirements

To deploy the demo via this automation you need:

* An All-Purpose CDE Virtual Cluster in CDP Public Cloud version 1.19 or above.
* A working installation of Docker on your local machine and a Dockerhub account. Please have your Dockerhub user and password ready.
* Basic knowledge of CDE, Python, Airflow, Iceberg and PySpark is recommended but not required. No code changes are required.

## Deployment Instructions

The automation is provided is a set of CDE CLI commands which are run as a shell script. The shell script and accompanying resources are located in the setup folder.

#### 1. Important Information

The Demo includes three parts. Part 1 includes a CDE Session and a CDE Spark Job. Part 2 consists of another CDE Session with Iceberg Spark and SQL commands. Part 3 executes four Spark Iceberg jobs in an Airflow DAG.

The Spark Job in part 1 can be run as many times as needed. It wipes out the previous data saved by jobs 2-5. If an error is made in the workshop, rerunning job 1 will allow you to start from scratch.

In order to run part 3, each participant must complete part 1 and part 2. In part 1 two Spark tables are created. In part 2 the fact table (transactions) is migrated to iceberg.

#### 2. autodeploy.sh

Run the autodeploy script with:

```
./auto_deploy_hol.sh pauldefusco pauldefusco 3 s3a://cde-innovation-buk-9e384927/data
```

Before running this be prepared to enter your Docker credentials in the terminal. Then, you can follow progress in the terminal output. The pipeline should deploy within three minutes. When setup is complete navigate to the CDE UI and validate that the demo has been deployed. By now the setup_job should have completed and the airflow_orchestration job should already be in process.

#### 3. autodestroy.sh

When you are done run this script to tear down the data in the Catalog but not in S3. That step will be handles by the GOES teardown scripts.

```
./auto_destroy.sh cdpworkloaduser
```

## Summary

You can deploy an end to end CDE Demo with the provided automation. The demo executes a small ETL pipeline including Iceberg, Spark and Airflow.
