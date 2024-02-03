#****************************************************************************
# (C) Cloudera, Inc. 2020-2024
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
    .appName("DATA VALIDATION") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .getOrCreate()

config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
storageLocation=config.get("general","data_lake_name")
print("Storage Location from Config File: ", storageLocation)

username = sys.argv[1]
print("PySpark Runtime Arg: ", sys.argv[1])

#---------------------------------------------------
#               LOAD TRX BATCH DATA
#---------------------------------------------------

trxBatchDf = spark.read.json("{0}/mkthol/trans/{1}/trx_batch_2".format(storageLocation, username))

trxBatchDf = trxBatchDf.withColumn("transaction_amount",  trxBatchDf["transaction_amount"].cast('float'))
trxBatchDf = trxBatchDf.withColumn("latitude",  trxBatchDf["latitude"].cast('float'))
trxBatchDf = trxBatchDf.withColumn("longitude",  trxBatchDf["longitude"].cast('float'))
trxBatchDf = trxBatchDf.withColumn("event_ts", trxBatchDf["event_ts"].cast("timestamp"))

#---------------------------------------------------
#               MIGRATE TRX DATA TO ICEBERG
#---------------------------------------------------

# Already done in CDE Session Iceberg Part 2:
#spark.sql("ALTER TABLE {}.TRX_TABLE UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
#spark.sql("CALL spark_catalog.system.migrate('{}.TRX_TABLE')".format(username))

#---------------------------------------------------
#               PARTITION TRX TABLE BY MONTH
#---------------------------------------------------

spark.sql("ALTER TABLE spark_catalog.{}.TRX_TABLE ADD PARTITION FIELD MONTHS(event_ts) AS MON".format(username))

#---------------------------------------------------
#               MERGE TRX RECORDS
#---------------------------------------------------

### ALTERNATIVE SYNTAX VIA ICEBERG DF API IF MERGE INTO IS JUST APPEND
trxBatchDf.writeTo("spark_catalog.{}.TRX_TABLE".format(username)).append()

"""trxBatchDf.createOrReplaceTempView("BATCH_TEMP_VIEW".format(username))

ICEBERG_MERGE_INTO_SYNTAX = """

"""MERGE INTO spark_catalog.{}.TRX_TABLE t
                                USING (SELECT * FROM BATCH_TEMP_VIEW) s
                                ON t.credit_card_number = s.credit_card_number
                                WHEN MATCHED THEN UPDATE SET *
                                WHEN NOT MATCHED THEN INSERT *"""
""".format(username)

spark.sql(ICEBERG_MERGE_INTO_SYNTAX)"""

#-----------------------------------------------------
#               PARTITION EVOLUTION TRX TABLE BY DAY
#-----------------------------------------------------

spark.sql("ALTER TABLE spark_catalog.{}.TRX_TABLE REPLACE PARTITION FIELD MON WITH DAYS(event_ts) AS DY".format(username))
