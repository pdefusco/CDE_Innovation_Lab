# CDE Demo Auto Deploy

## Objective

This git repository hosts the automation for a CDE Demo that includes Spark, Airflow and Iceberg. The Demo is deployed and removed in your Virtual Cluster within minutes.


## Table of Contents

* [Requirements](https://github.com/pdefusco/CDE_Demo_Auto_Deploy#requirements)
* [Demo Content](https://github.com/pdefusco/CDE_Demo_Auto_Deploy#demo-content)
* [Deployment Instructions](https://github.com/pdefusco/CDE_Demo_Auto_Deploy#deployment-instructions)
  * [1. Important Information](https://github.com/pdefusco/CDE_Demo_Auto_Deploy#1-important-information)
  * [2. autodeploy.sh](https://github.com/pdefusco/CDE_Demo_Auto_Deploy#2-autodeploysh)
  * [3. autodestroy.sh](https://github.com/pdefusco/CDE_Demo_Auto_Deploy#3-autodestroysh)
* [Summary](https://github.com/pdefusco/CDE_Demo_Auto_Deploy#summary)
* [CDE Relevant Projects](https://github.com/pdefusco/CDE_Demo_Auto_Deploy#cde-relevant-projects)


## Requirements

To deploy the demo via this automation you need:

* A CDE Virtual Cluster in CDP Public Cloud AWS (Azure coming soon).
* A working installation of Docker on your local machine and a Dockerhub account. Please have your Dockerhub user and password ready.
* Basic knowledge of CDE, Python, Airflow, Iceberg and PySpark is recommended but not required. No code changes are required.


## Demo Content

The Demo includes an Airflow DAG orchestrating three Spark Jobs in CDE. The first Spark Job loads fresh data into a staging table. The second Spark Job executes an Iceberg Merge Into into the target table. Finally, the third job performs some basic monitoring queries on Iceberg Metadata.

The Airflow Job is designed to run every five minutes independently. At each run new random data is generated, then added to the staging table and finally loaded into the target table. The same target table is used so if left running the demo can show what happens to Iceberg Metadata overtime.

Before the pipeline is executed, a setup job is launched upon triggering the deployment script. However, this job is not part of the demo track and is automatically removed when the setup is complete.

When the demo is deployed you will have the following in your CDE Virtual Cluster:

* Three CDE Spark Jobs: ```staging_table```, ```iceberg_mergeinto```, ```iceberg_metadata_queries```.
* One CDE Airflow Job: ```airflow_orchestration```.
* One CDE Files Resource: ```cde_demo_files```.
* One CDE Docker Runtime Resource: ```dex-spark-runtime-dbldatagen```.
* The CDE CLI is pre-installed in the Docker container.


## Deployment Instructions

The automation is provided in a Docker container. First, pull the docker container and run it:

```
docker pull pauldefusco/cde_demo_auto_deploy
docker run -it pauldefusco/cde_demo_auto_deploy
```

You will be automatically logged into the container as ```cdeuser```. Run the remaining commands from the container.

Add your CDE Virtual Cluster to the CDE CLI configuration file. To do so, paste your CDE Virtual Cluster's Jobs API URL at line 2.

```
vi ~/.cde/config.yaml
```

Next open the Airflow DAG and edit the username at line 50 by adding your CDP Workload Username.

```
vi ~/CDE_Demo/airflow/airflow_DAG.py
```

<pre>
# Airflow DAG
from datetime import datetime, timedelta
from dateutil import parser
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

<b>username = "cdpworkloaduser"</b> # Enter your workload username here
</pre>

#### 1. Important Information

* Each generated Iceberg Table, CDE Job and Resource will be prefixed with your CDP Workload Username.
* CDP Workload Users with dots, hyphens and other symbols are not currently supported. Please reach out to the Field Specialist team for a workaround.
* Multiple users can deploy the demo in the same CDE Virtual Cluster as long as they use different credentials.
* Each user can deploy the demo at most once in the same CDE Virtual Cluster.
* All Iceberg tables, CDE Jobs and Resources are deleted from the CDE Virtual Cluster upon execution of the "autodestroy.sh" script.
* Currently Deployment is limited to AWS CDE Services but Azure and Private Cloud will be added soon.
* The entire pipeline is executed upon deployment. No jobs need to be manually triggered upon deployment.
* **Known limitation**: when the pipeline is deployed for the first time the DAG may run twice. Therefore, in the very first run you may see a duplicate job in the Job Runs page.

#### 2. autodeploy.sh

Run the autodeploy script with:

```
./auto_deploy_hol.sh dockerusername cdpworkloaduser maxparticipants
```

Before running this be prepared to enter your Docker credentials in the terminal. Then, you can follow progress in the terminal output. The pipeline should deploy within three minutes. When setup is complete navigate to the CDE UI and validate that the demo has been deployed. By now the setup_job should have completed and the airflow_orchestration job should already be in process.

#### 3. autodestroy.sh

When you are done run this script to tear down the pipeline:

```
./auto_destroy.sh cdpworkloaduser
```


## Summary

You can deploy an end to end CDE Demo with the provided automation. The demo executes a small ETL pipeline including Iceberg, Spark and Airflow.

## CDE Relevant Projects

If you are exploring or using CDE today you may find the following tutorials relevant:

* [CDE 1.19 Workshop HOL](https://github.com/pdefusco/CDE119_ACE_WORKSHOP): The HOL is typically a three to four-hour event organized by Cloudera for CDP customers and prospects, where a small technical team from Cloudera Solutions Engineering provides cloud infrastructure for all participants and guides them through the completion of the labs with the help of presentations and open discussions.

* [Spark 3 & Iceberg](https://github.com/pdefusco/Spark3_Iceberg_CML): A quick intro of Time Travel Capabilities with Spark 3.

* [Simple Intro to the CDE CLI](https://github.com/pdefusco/CDE_CLI_Simple): An introduction to the CDE CLI for the CDE beginner.

* [CDE CLI Demo](https://github.com/pdefusco/CDE_CLI_demo): A more advanced CDE CLI reference with additional details for the CDE user who wants to move beyond the basics.

* [CDE Resource 2 ADLS](https://github.com/pdefusco/CDEResource2ADLS): An example integration between ADLS and CDE Resource. This pattern is applicable to AWS S3 as well and can be used to pass execution scripts, dependencies, and virtually any file from CDE to 3rd party systems and viceversa.

* [Using CDE Airflow](https://github.com/pdefusco/Using_CDE_Airflow): A guide to Airflow in CDE including examples to integrate with 3rd party systems via Airflow Operators such as BashOperator, HttpOperator, PythonOperator, and more.

* [GitLab2CDE](https://github.com/pdefusco/Gitlab2CDE): a CI/CD pipeline to orchestrate Cross-Cluster Workflows for Hybrid/Multicloud Data Engineering.

* [CML2CDE](https://github.com/pdefusco/cml2cde_api_example): an API to create and orchestrate CDE Jobs from any Python based environment including CML. Relevant for ML Ops or any Python Users who want to leverage the power of Spark in CDE via Python requests.

* [Postman2CDE](https://github.com/pdefusco/Postman2CDE): An example of the Postman API to bootstrap CDE Services with the CDE API.

* [Oozie2CDEAirflow API](https://github.com/pdefusco/Oozie2CDE_Migration): An API to programmatically convert Oozie workflows and dependencies into CDE Airflow and CDE Jobs. This API is designed to easily migrate from Oozie to CDE Airflow and not just Open Source Airflow.

For more information on the Cloudera Data Platform and its form factors please visit [this site](https://docs.cloudera.com/).

For more information on migrating Spark jobs to CDE, please reference [this guide](https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/cdppvc-data-migration-spark/topics/cdp-migration-spark-cdp-cde.html).
