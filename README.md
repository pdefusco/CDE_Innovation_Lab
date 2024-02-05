# CDE 1.19 Hands-On-Lab - Cloudera Marketing

## About the Hands On Lab Workshops

The Hands-On Lab (HOL) Workshops are an initiative by Cloudera Solutions Engineering aimed at familiarizing CDP users with each Data Service. The content consists of a series of guides and exercises to quickly implement sample end-to-end use cases in the realm of Machine Learning, Datawarehousing, Data Engineering, Data Streaming and Operational Database.

The Marketing Event HOL is typically a two-hour event organized by Cloudera for CDP customers and prospects, where a small technical team from Cloudera Solutions Engineering provides cloud infrastructure for all participants and guides them through the completion of the labs with the help of presentations and open discussions.

The content is primarily designed for developers, cloud administrators and big data software architects. However, little to no code changes are typically required and non-technical stakeholders such as project managers and analysts are encouraged to actively take part.

HOL events are open to all CDP users and customers. If you would like Cloudera to host an event for you and your colleagues please contact your local Cloudera Representative or submit your information [through this portal](https://www.cloudera.com/contact-sales.html). Finally, if you have access to a CDE Virtual Cluster you are welcome to use this guide and go through the same concepts in your own time.

## About the Cloudera Data Engineering (CDE) Service

CDE is the Cloudera Data Engineering Service, a containerized managed service for Cloudera Data Platform designed for Large Scale Batch Pipelines with Spark, Airflow and Iceberg. It allows you to submit batch jobs to auto-scaling virtual clusters. As a Cloud-Native service, CDE enables you to spend more time on your applications, and less time on infrastructure.

CDE allows you to create, manage, and schedule Apache Spark jobs without the overhead of creating and maintaining Spark clusters. With CDE, you define virtual clusters with a range of CPU and memory resources, and the cluster scales up and down as needed to run your Spark workloads, helping to control your cloud costs.

## About the Labs

This Hands On Lab is designed to walk you through the Services's main capabilities. Throughout the exercises you will:

1. Deploy an Ingestion, Transformation and Reporting pipeline with Spark 3.2.
2. Learn about Iceberg's most popular features.
3. Orchestrate pipelines with Airflow.

Throughout these labs, you are going to deploy an ELT (Extract, Load, Transform) data pipeline that extracts data stored on AWS S3 (or Azure ADLS) object storage, loads it into the Cloudera Data Lakehouse and transforms it for reporting purposes.

## Step by Step Instructions

Detailed instructions are provided in the [step_by_step_guides](https://github.com/pdefusco/CDE_Banking_HOL_MKT/tree/main/step_by_step_guides/english) folder.

* [Link to the English Guide](https://github.com/pdefusco/CDE_Banking_HOL_MKT/tree/main/step_by_step_guides/english).

## Other CDP Hands On Lab Workshops

CDP Data Services include Cloudera Machine Learning (CML), Cloudera Operational Database (COD), Cloudera Data Flow (CDF) and Cloudera Data Warehouse (CDW). HOL Workshops are available for each of these CDP Data Services.

* [CML Workshop](https://github.com/cloudera/CML_AMP_Churn_Prediction): Prototype and deploy a Churn Prediction classifier, apply an explainability model to it and deploy a Flask Application to share insights with your project stakeholders. This project uses SciKit-Lean, PySpark, and Local Interpretable Model-agnostic Explanations (LIME).
* [CDF Workshop](https://github.com/cloudera-labs/edge2ai-workshop): Build a full OT to IT workflow for an IoT Predictive Maintenance use case with: Edge Flow Management with MQTT and MiNiFi for data collection; Data Flow management was handled by NiFi and Kafka, and Spark Streaming with Cloudera Data Science Workbench (CDSW) model to process data. The lab also includes content focused on Kudu, Impala and Hue.
* [CDW Workshop](https://github.com/pdefusco/cdw-workshop): As a Big Data Engineer and Analyst for an Aeronautics corporation, build a Data Warehouse & Data Lakehouse to gain an advantage over your competition. Interactively explore data at scale. Create ongoing reports. Finally move to real-time analysis to anticipate engine failures. All using Apache Impala and Iceberg.
