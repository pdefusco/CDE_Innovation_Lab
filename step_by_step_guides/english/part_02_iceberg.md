# Part 2: Apache Iceberg in CDE

### A Brief Introduction to Apache Iceberg

Apache Iceberg is a cloud-native, high-performance open table format for organizing petabyte-scale analytic datasets on a file system or object store. Combined with Cloudera Data Platform (CDP), users can build an open data lakehouse architecture for multi-function analytics and to deploy large scale end-to-end pipelines.

Iceberg provides many advantages. First of all it is a table format that provides many advantages including the ability to standardize various data formats into a uniform data management system, a Lakehouse. In the Iceberg Lakehouse, data can be queries with different compute engines including Apache Impala, Apache Flink, Apache Spark and others.

In addition, Iceberg simplifies data analysis and data engineering pipelines with features such as partition and schema evolution, time travel, and Change Data Capture. In CDE you can use Spark to query Iceberg Tables interactively via Sessions or in batch pipelines via Jobs.

### Lab 1: Working with Iceberg in CDE Sessions

In Part 1 we used CDE Sessions to explore two datasets and prototype code for a fraud detection Spark Application. In this brief Lab we will load a new batch of transactions from Cloud Storage and leverage Iceberg Lakehouse capabilities in order to test the SQL Merge Into with Spark.

##### Migrate Spark Table to Iceberg Table

Launch a new CDE Session or reuse the Session you created in Part 1 if it is still running.

```
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

storageLocation = "s3a://cde-innovation-buk-9e384927/data"
username = "user002"
```

By default, a Spark table created in CDE is tracked in the Hive Metastore as an External table. Data is stored in Cloud Storage in one of many formats (parquet, csv, avro, etc.) and its location is tracked by the HMS.

When adopting Iceberg for the first time, tables can be copied or migrated to Iceberg format. A copy implies the recompuation of the whole datasets into an Iceberg table while a Migration only involves the creation of Iceberg metadata in the Iceberg Metadata Layer.   

```
## Migrate Table to Iceberg Table Format
spark.sql("ALTER TABLE {}.TRX_TABLE UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
spark.sql("CALL spark_catalog.system.migrate('{}.TRX_TABLE')".format(username))
```

The Iceberg Metadata Layer provides three layers of metadata files whose purpose is to track Iceberg tables as they are modified. The Metadata layer consists of Metadata Files, Manifest Lists and Manifest Files.

The Metadata Layer as a whole stores a version of each dataset by providing pointers to different versions of the data. Among other advantages, this enables Time Travel capabilities on Iceberg tables i.e. the ability to query data as of a particular snapshot or timestamp.

In the following example you will load a new batch of transactions from Cloud Storage. Then you will insert it into the historical transactions table via the SQL Merge Into statement which not available in Spark SQL unless Iceberg table format is used. Finally, we will query Iceberg Metadata tables and query the data in its PRE-INSERT version with a simple Spark SQL command.

As in Part 1, you can just copy and paste the following code snippets into CDE Sessions Notebook cells.

```
# PRE-INSERT TIMESTAMP
from datetime import datetime
now = datetime.now()
timestamp = datetime.timestamp(now)

print("PRE-INSERT TIMESTAMP: ", timestamp)
```

```
# PRE-INSERT COUNT
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}.TRX_TABLE".format(username)).show()
```
```
# LOAD NEW TRANSACTION BATCH
trxBatchDf = spark.read.json("{0}/mkthol/trans/{1}/trx_batch_1".format(storageLocation, username))
trxBatchDf = trxBatchDf.withColumn("event_ts", trxBatchDf["event_ts"].cast('timestamp'))
trxBatchDf.printSchema()
trxBatchDf.createOrReplaceTempView("BATCH_TEMP_VIEW".format(username))
```

```
ICEBERG_MERGE_INTO_SYNTAX = """MERGE INTO spark_catalog.{}.TRX_TABLE t USING (SELECT * FROM BATCH_TEMP_VIEW) s
   ON t.credit_card_number = s.credit_card_number WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""".format(username)

spark.sql(ICEBERG_MERGE_INTO_SYNTAX)
```

```
### ALTERNATIVE SYNTAX VIA ICEBERG DF API IF MERGE INTO IS JUST APPEND
### trxBatchDf.writeTo("spark_catalog.{}.TRX_TABLE".format(username)).append()
```

```
# POST-INSERT COUNT
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}.TRX_TABLE".format(username)).show()
```

```
# QUERY ICEBERG METADATA HISTORY TABLE
spark.sql("SELECT * FROM spark_catalog.{}.TRX_TABLE.history".format(username)).show(20, False)
```

```
# QUERY ICEBERG METADATA HISTORY TABLE
spark.sql("SELECT * FROM spark_catalog.{}.TRX_TABLE.snapshots".format(username)).show(20, False)
```

```
# TIME TRAVEL AS OF PREVIOUS TIMESTAMP
df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load("spark_catalog.{}.TRX_TABLE".format(username))

# POST TIME TRAVEL COUNT
print(df.count())
```

### Summary

Open data lakehouse on CDP simplifies advanced analytics on all data with a unified platform for structured and unstructured data and integrated data services to enable any analytics use case from ML, BI to stream analytics and real-time analytics. Apache Iceberg is the secret sauce of the open lakehouse.

Apache Iceberg is an open table format designed for large analytic workloads. It supports schema evolution, hidden partitioning, partition layout evolution and time travel. Every table change creates an Iceberg snapshot, this helps to resolve concurrency issues and allows readers to scan a stable table state every time.

Iceberg lends itself well to a variety of use cases including Lakehouse Analytics, Data Engineering pipelines, and regulatory compliance with specific aspects of regulations such as GDPR (General Data Protection Regulation) and CCPA (California Consumer Privacy Act) that require being able to delete customer data upon request.

CDE Virtual Clusters provide native support for Iceberg. Users can run Spark workloads and interact with their Iceberg tables via SQL statements. The Iceberg Metadata Layer tracks Iceberg table versions via Snapshots and provides Metadata Tables with snapshot and other useful information. In this Lab we used Iceberg to access the credit card transactions dataset as of a particular timestamp.

### References

If you are curious to learn more about the above features in the context of more advanced use cases, please visit the following references:

* [Apache Iceberg in the Cloudera Data Platform](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html)
* [Exploring Iceberg Architecture](https://github.com/pdefusco/Exploring_Iceberg_Architecture)
* [Using Apache Iceberg in Cloudera Data Engineering](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)
* [Importing and Migrating Iceberg Table in Spark 3](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-iceberg-import-migrate-table.html)
* [Getting Started with Iceberg and Spark](https://iceberg.apache.org/docs/latest/spark-getting-started/)
