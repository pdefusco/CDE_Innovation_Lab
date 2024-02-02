# Build a Spark Job

### CDE Sessions

You can explore data interactively in CDE Sessions.

Launch a CDE Session and run the following commands:

```
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

storageLocation = "s3a://goes-se-sandbox01"
username = "user001"

### TRANSACTIONS FACT TABLE

transactionsDf = spark.read.json("{0}/mkthol/trans/{1}/transactions.json".format(storageLocation, username))

transactionsDf.printSchema()

# Takes in a StructType schema object and return a column selector that flattens the Struct
def flatten_struct(schema, prefix=""):
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(F.col(prefix + elem.name).alias(prefix + elem.name))
    return result


# FLATTEN NESTED STRUCT
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))

transactionsDf.printSchema()

# RENAME COLS
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_amount", "transaction_amount")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_currency", "transaction_currency")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_type", "transaction_type")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.latitude", "latitude")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.longitude", "longitude")

# CAST TYPES
transactionsDf = transactionsDf.withColumn("transaction_amount",  transactionsDf["transaction_amount"].cast('float'))
transactionsDf = transactionsDf.withColumn("latitude",  transactionsDf["latitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("longitude",  transactionsDf["longitude"].cast('float'))

### ANALYTICS ON TRANSACTIONS FACT TABLE ###

transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0],2)
transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0],2)

print("Transaction Amount Mean: ", transactionsAmountMean)
print("Transaction Amount Median: ", transactionsAmountMedian)

#transactionsDf.select("event_ts")
transactionsDf.createOrReplaceTempView("res")

spark.sql("SELECT * FROM res LIMIT 10").show()

# average transaction amount by month
spark.sql("SELECT MONTH(event_ts) AS month, \
          avg(transaction_amount) FROM res GROUP BY month ORDER BY month").show()

# Average transaction amount by day of week
spark.sql("SELECT DAYOFWEEK(event_ts) AS DAYOFWEEK, \
          avg(transaction_amount) FROM res GROUP BY DAYOFWEEK ORDER BY DAYOFWEEK").show()

# Number of transactions by credit card
spark.sql("SELECT CREDIT_CARD_NUMBER, COUNT(*) AS COUNT FROM res \
            GROUP BY CREDIT_CARD_NUMBER ORDER BY COUNT DESC LIMIT 10").show()

### PII DIMENSION TABLE

piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii.csv".format(storageLocation, username))

piiDf.show()

piiDf.printSchema()

### CAST LAT LON AS FLOAT

piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))

piiDf.createOrReplaceTempView("info")

# TOP 100 customers with multiple credit cards, sorted by highest to lowest
spark.sql("SELECT name AS name, \
          COUNT(credit_card_number) AS CC_COUNT FROM info GROUP BY name ORDER BY CC_COUNT DESC \
          LIMIT 100").show()

# TOP 100 credit cards with multiple names, sorted by highest to lowest
spark.sql("SELECT COUNT(name) AS NM_COUNT, \
          credit_card_number AS CC_NUM FROM info GROUP BY CC_NUM ORDER BY NM_COUNT DESC \
          LIMIT 100").show()

# TOP 25 customers with multiple addresses, sorted by highest to lowest
spark.sql("SELECT name AS name, \
          COUNT(address) AS ADD_COUNT FROM info GROUP BY name ORDER BY ADD_COUNT DESC \
          LIMIT 25").show()

### OPTIONAL: ADD A MORE ADVANCED QUERY HERE E.G. WINDOWING

### JOIN TWO DATASETS AND COMPARE COORDINATES

joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM info i INNER JOIN res r
          ON i.credit_card_number == r.credit_card_number;""")

distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())

distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude",
                                                                            "address_latitude", "address_longitude")))

# SELECT CUSTOMERS WHERE TRANSACTION OCCURRED MORE THAN 100 MILES FROM HOME
distanceDf.filter(distanceDf.trx_dist_from_home > 100)
```

### CDE Spark Jobs

You have used Sessions to interactively explore data, but CDE Spark Jobs provide many benefits (insert benefits here).

Before running the following script:
1. Create a CDE Files Resource
2. Upload Script and Dependencies
3. Create a Python Resource and upload requirements.txt

Run the following script to build a report.

```
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
import sys, random, os, json, random, configparser

spark = SparkSession \
    .builder \
    .appName("BANK TRANSACTIONS BATCH REPORT") \
    .getOrCreate()

storageLocation = "s3a://goes-se-sandbox01"
username = "user001" # Enter your username here

### TRANSACTIONS FACT TABLE

transactionsDf = spark.read.json("{0}/mkthol/trans/{1}/transactions.json".format(storageLocation, username))

transactionsDf.printSchema()

# Takes in a StructType schema object and return a column selector that flattens the Struct
def flatten_struct(schema, prefix=""):
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(F.col(prefix + elem.name).alias(prefix + elem.name))
    return result


transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))

transactionsDf.printSchema()

cols = [col for col in transactionsDf.columns if col.startswith("transaction")]
new_cols = [col.split(".")[1] for col in cols]

def renameMultipleColumns(df, cols, new_cols):
  res = {cols[i]: new_cols[i] for i in range(len(cols))}
  for key, value in res.items():
    df = df.withColumnRenamed(key,value)
  return df

cols = ["transaction_amount", "latitude", "longitude"]

def castMultipleColumns(df, cols):
  for col_name in cols:
    df = df.withColumn(col_name, F.col(col_name).cast('float'))
  return df

transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))

transactionsDf.createOrReplaceTempView("TRX_VIEW")

### PII DIMENSION TABLE

piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii.csv".format(storageLocation, username))

### CAST LAT LON AS FLOAT

piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))

piiDf.createOrReplaceTempView("CUST_VIEW")

### JOIN TWO DATASETS AND COMPARE COORDINATES

joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM CUST_VIEW i INNER JOIN TRX_VIEW r
          ON i.credit_card_number == r.credit_card_number;""")

distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())

distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude",
                                                                            "address_latitude", "address_longitude")))

# SELECT CUSTOMERS WHERE TRANSACTION OCCURRED MORE THAN 100 MILES FROM HOME
distanceDf.filter(distanceDf.trx_dist_from_home > 100).show()
```
