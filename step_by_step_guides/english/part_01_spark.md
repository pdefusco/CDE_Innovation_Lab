# Build a Spark Job

### CDE Sessions

You can explore data interactively in CDE Sessions.

Launch a CDE Session and run the following commands:

```
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

storageLocation = "s3a://go01-demo"
username = "user002"
```

```
### WORKING WITH TRANSACTIONS FACT TABLE
transactionsDf = spark.read.json("{0}/mkthol/trans/{1}/transactions.json".format(storageLocation, username))
transactionsDf.printSchema()
```
```
# Takes in a StructType schema object and return a column selector that flattens the Struct
def flatten_struct(schema, prefix=""):
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(F.col(prefix + elem.name).alias(prefix + elem.name))
    return result
```

```
# FLATTEN NESTED STRUCT
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))
transactionsDf.printSchema()
```
```
# RENAME COLS
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_amount", "transaction_amount")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_currency", "transaction_currency")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_type", "transaction_type")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.latitude", "latitude")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.longitude", "longitude")
```
```
# CAST TYPES
transactionsDf = transactionsDf.withColumn("transaction_amount",  transactionsDf["transaction_amount"].cast('float'))
transactionsDf = transactionsDf.withColumn("latitude",  transactionsDf["latitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("longitude",  transactionsDf["longitude"].cast('float'))
```

```
### ANALYTICS ON TRANSACTIONS FACT TABLE ###
transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0],2)
transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0],2)

print("Transaction Amount Mean: ", transactionsAmountMean)
print("Transaction Amount Median: ", transactionsAmountMedian)
```

```
#transactionsDf.select("event_ts")
transactionsDf.createOrReplaceTempView("trx")
spark.sql("SELECT * FROM trx LIMIT 10").show()
```

```
# average transaction amount by month
spark.sql("SELECT MONTH(event_ts) AS month, \
          avg(transaction_amount) FROM trx GROUP BY month ORDER BY month").show()
```

```
# Average transaction amount by day of week
spark.sql("SELECT DAYOFWEEK(event_ts) AS DAYOFWEEK, \
          avg(transaction_amount) FROM trx GROUP BY DAYOFWEEK ORDER BY DAYOFWEEK").show()
```

```
# Number of transactions by credit card
spark.sql("SELECT CREDIT_CARD_NUMBER, COUNT(*) AS COUNT FROM trx \
            GROUP BY CREDIT_CARD_NUMBER ORDER BY COUNT DESC LIMIT 10").show()
```

```
### PII DIMENSION TABLE
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii.csv".format(storageLocation, username))
piiDf.show()
piiDf.printSchema()
```

```
### CAST LAT LON AS FLOAT
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))
piiDf.createOrReplaceTempView("cust_info")
```

```
# TOP 100 customers with multiple credit cards, sorted by highest to lowest
spark.sql("SELECT name AS name, \
          COUNT(credit_card_number) AS CC_COUNT FROM cust_info GROUP BY name ORDER BY CC_COUNT DESC \
          LIMIT 100").show()
```

```
# TOP 100 credit cards with multiple names, sorted by highest to lowest
spark.sql("SELECT COUNT(name) AS NM_COUNT, \
          credit_card_number AS CC_NUM FROM cust_info GROUP BY CC_NUM ORDER BY NM_COUNT DESC \
          LIMIT 100").show()
```

```
# TOP 25 customers with multiple addresses, sorted by highest to lowest
spark.sql("SELECT name AS name, \
          COUNT(address) AS ADD_COUNT FROM cust_info GROUP BY name ORDER BY ADD_COUNT DESC \
          LIMIT 25").show()
```

```
### OPTIONAL: ADD A MORE ADVANCED QUERY E.G. WINDOWING

### JOIN TWO DATASETS AND COMPARE COORDINATES
joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM cust_info i INNER JOIN trx r
          ON i.credit_card_number == r.credit_card_number;""")
joinDf.show()
```
```
distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())
distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude",
                                                                            "address_latitude", "address_longitude")))
```

```
# SELECT CUSTOMERS WHERE TRANSACTION OCCURRED MORE THAN 100 MILES FROM HOME
distanceDf.filter(distanceDf.trx_dist_from_home > 100).show()
```

### CDE Spark Jobs

You have used Sessions to interactively explore data, but CDE Spark Jobs provide many benefits (insert benefits here).

Before running the following script:
1. Create a CDE Files Resource
2. Upload Script and Dependencies
3. Create a Python Resource and upload requirements.txt

Run the following script to build a report.

```
#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys, random, os, json, random, configparser
from utils import *

spark = SparkSession \
    .builder \
    .appName("BANK TRANSACTIONS BATCH REPORT") \
    .getOrCreate()

config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
storageLocation=config.get("general","data_lake_name")
print("Storage Location from Config File: ", storageLocation)

username = sys.argv[1]
print("PySpark Runtime Arg: ", sys.argv[1])

### TRANSACTIONS FACT TABLE

transactionsDf = spark.read.json("{0}/mkthol/trans/{1}/transactions.json".format(storageLocation, username))
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))
transactionsDf.printSchema()

### RENAME MULTIPLE COLUMNS
cols = [col for col in transactionsDf.columns if col.startswith("transaction")]
new_cols = [col.split(".")[1] for col in cols]
transactionsDf = renameMultipleColumns(transactionsDf, cols, new_cols)

### CAST TYPES
cols = ["transaction_amount", "latitude", "longitude"]
transactionsDf = castMultipleColumns(transactionsDf, cols)
transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))

### TRX DF SCHEMA AFTER CASTING AND RENAMING
transactionsDf.printSchema()

### STORE TRANSACTIONS AS TABLE
spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(username))
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(username))
spark.sql("SHOW DATABASES LIKE '{}'".format(username)).show()
transactionsDf.write.mode("overwrite").saveAsTable('{}.TRX_TABLE'.format(username), format="parquet")

### PII DIMENSION TABLE
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii.csv".format(storageLocation, username))

### CAST LAT LON AS FLOAT
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))

### STORE CUSTOMER DATA AS TABLE
piiDf.write.mode("overwrite").saveAsTable('{}.CUST_TABLE'.format(username), format="parquet")

### JOIN TWO DATASETS AND COMPARE COORDINATES
joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM {0}.CUST_TABLE i INNER JOIN {0}.TRX_TABLE r
          ON i.credit_card_number == r.credit_card_number;""".format(username))

print("JOINDF SCHEMA")
joinDf.printSchema()

### PANDAS UDF
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType

# Method for Euclidean Distance
def euclidean_dist(x1: pd.Series, x2: pd.Series, y1: pd.Series, y2: pd.Series) -> pd.Series:
   return ((x2-x1)**2)+((y2-y1)**2).pow(1./2)

# Saving Method as Pandas UDF
eu_dist = pandas_udf(euclidean_dist, returnType=FloatType())

# Applying UDF on joinDf
eucDistDf = joinDf.withColumn("DIST_FROM_HOME", eu_dist(F.col("address_longitude"), \
                                      F.col("longitude"), F.col("address_latitude"), \
                                       F.col("latitude")))

# SELECT CUSTOMERS WHERE TRANSACTION OCCURRED MORE THAN 100 MILES FROM HOME
eucDistDf.filter(eucDistDf.DIST_FROM_HOME > 100).show()
```
