#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
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

# Airflow DAG
from datetime import datetime, timedelta
from dateutil import parser
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.param import Param

username = "user092" # Enter your username here
dag_name = "BankFraud-Orch-"+username

print("Using DAG Name: {}".format(dag_name))

default_args = {
        'owner':username,
        'start_date': datetime(2024,1,1,1),
        'depends_on_past': False,
        'retries':0,
        'schedule_interval':None, #timedelta(minutes=5) '*/5 * * * *'
        'retry_delay': timedelta(minutes=15),
        'end_date': datetime(2025,1,1,1)
        }

airflow_dag = DAG(
        dag_name,
        default_args=default_args,
        catchup=False,
        schedule_interval=None,#'*/5 * * * *'
        is_paused_upon_creation=False
        )

start = DummyOperator(
        task_id="start",
        dag=airflow_dag
)

data_validation = CDEJobRunOperator(
        task_id='data-validation',
        dag=airflow_dag,
        job_name='02_data_val_'+username, #Must match name of CDE Spark Job in the CDE Jobs UI
        trigger_rule='all_success',
        )

customer_data = CDEJobRunOperator(
        task_id='customer-data-load',
        dag=airflow_dag,
        job_name='03_cust_data_'+username, #Must match name of CDE Spark Job in the CDE Jobs UI
        trigger_rule='all_success',
        )

merge_trx = CDEJobRunOperator(
        task_id='merge-trx',
        dag=airflow_dag,
        job_name='04_merge_trx_'+username, #Must match name of CDE Spark Job in the CDE Jobs UI
        trigger_rule='all_success',
        )

inc_report = CDEJobRunOperator(
        task_id='incremental-report',
        dag=airflow_dag,
        job_name='05_inc_report_'+username, #Must match name of CDE Spark Job in the CDE Jobs UI
        trigger_rule='all_success',
        )

end = DummyOperator(
        task_id="end",
        dag=airflow_dag
)

start >> [data_validation, customer_data] >> merge_trx >> inc_report >> end
