# Understanding Iceberg

### Brief Intro to Apache Iceberg

### Working with Iceberg in CDE Sessions

Iceberg allows you to perform more advanced operations. In this Lab you will use a CDE Session to get acquainteed with Iceberg and its benefits.

##### Migrate Spark Table to Iceberg Table

```
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

storageLocation = "s3a://cde-innovation-buk-9e384927/data"
username = "user001"
```

```
transactionsDf = spark.sql("SELECT * FROM {}.TRX_TABLE".format(username))
transactionsDf.printSchema()
```

```
## Migrate Table to Iceberg Table Format
spark.sql("ALTER TABLE {}.TRX_TABLE UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
spark.sql("CALL spark_catalog.system.migrate('{}.TRX_TABLE')".format(username))
```

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
trxBatchDf = spark.read.json("{0}/mkthol/trans/{1}/trx_batch.json".format(storageLocation, username))
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
