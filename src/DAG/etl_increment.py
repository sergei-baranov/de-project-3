from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import numpy
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from os.path import exists

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

pg_conn = BaseHook.get_connection('pg_connection')

def get_increment_request(conn_name, business_dt):
    """
    получаем increment_id по переменной etl_report_id
    и дате business_dt через /get_increment,
    сохраняем в переменную etl_increment_id,
    и далее по нему забираем файлы через s3 уже отдельной таской
    (get_files_from_s3)

    если файл уже существует - не обращаемся к серверу
    (у нас по факту один файл используется в ТЗ)
    """

    dt = business_dt
    # (datetime.strptime(business_dt, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    local_filaname = '/file_staging/' + dt + '_user_orders_log_inc.csv'
    file_exists = exists(local_filaname)
    if file_exists is True:
        return

    conn = BaseHook.get_connection(conn_name)
    req_host = conn.host
    # req_host = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/'

    report_id = Variable.get('etl_report_id')

    nickname = "sergei_baranov"
    cohort = "1"
    url = f"{req_host}/get_increment?report_id={report_id}&date={dt}T00:00:00"
    resp = requests.get(
        url,
        headers={
            "X-Nickname": nickname,
            "X-Cohort": cohort,
            "X-API-KEY": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
            "X-Project": 'True'
        }
    ).json()

    if resp['status'] != 'SUCCESS':
        raise ValueError(
            'task status is not SUCCESS (' + resp['status'] + ') for url [' + url + ']')

    increment_id = resp['data']['increment_id']
    if increment_id is None:
        raise ValueError(
                f"increment_id for report_id {report_id} and date {dt} is None")
    Variable.set(key='etl_increment_id', value=increment_id)


def get_files_from_s3(business_dt):
    """
    забираем файлы через s3 по report_id из переменной etl_report_id,
    стейджим их в директорию /file_staging

    если файл уже существует - не обращаемся к серверу
    """

    dt = business_dt
    # (datetime.strptime(business_dt, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    # https://storage.yandexcloud.net/s3-sprint3/cohort_1/sergei_baranov/project/{increment_id}/user_orders_log_inc.csv

    increment_id = Variable.get('etl_increment_id')
    session = boto3.session.Session()
    bucket_name = 's3-sprint3'
    key = f"cohort_1/sergei_baranov/project/{increment_id}/"
    # TWpBeU1pMHdOaTB5TWxRd01Eb3hOem95TkFselpYSm5aV2xmWW1GeVlXNXZkZz09
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id='YCAJEWXOyY8Bmyk2eJL-hlt2K',
        aws_secret_access_key='YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA'
    )
    dt = dt.replace('-', '')
    filebases = [
        # 'customer_research',
        # 'user_activity_log',
        # 'price_log',
        'user_orders_log'
    ]
    for s3_filebase in filebases:
        local_filaname = '/file_staging/' + dt + '_' + s3_filebase + '_inc.csv'

        file_exists = exists(local_filaname)
        if file_exists is True:
            continue

        key_i = key + s3_filebase + '_inc.csv'
        try:
            s3.download_file(
                Bucket=bucket_name,
                Key=key_i,
                Filename=local_filaname
            )
        except ClientError as e:
            print("boto3 error while downloading " + s3_filebase + " (" + e.response['Error']['Message'] + ")")


def pg_execute_query(query, conn_obj):
    conn_args = {
        'dbname': conn_obj.schema,
        'user': conn_obj.login,
        'password': conn_obj.password,
        'host': conn_obj.host,
        'port': conn_obj.port
    }
    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def load_file_to_pg(business_dt, filebase, pg_table, conn_obj):
    dt = business_dt
    # (datetime.strptime(business_dt, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    truncate_sql = f"TRUNCATE {pg_table};"

    dt = dt.replace('-', '')
    filename = '/file_staging/' + dt + '_' + filebase + '_inc.csv'

    f = pd.read_csv(filename)
    # f.drop(columns=f.columns[[0]], axis=1, inplace=True)

    cols = ','.join(list(f.columns))
    insert_stmt = f"INSERT INTO {pg_table} ({cols}) VALUES %s"

    conn_args = {
        'dbname': conn_obj.schema,
        'user': conn_obj.login,
        'password': conn_obj.password,
        'host': conn_obj.host,
        'port': conn_obj.port
    }

    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()

    # truncate
    cur.execute(truncate_sql)
    conn.commit()

    # insert
    psycopg2.extras.execute_values(cur, insert_stmt, f.values)
    conn.commit()

    cur.close()
    conn.close()

    if pg_table == 'staging.user_order_log':
        migrations_path = '/migrations/'

        with open(migrations_path + '/increment_mart_d_item.sql') as file:
            sql_query = file.read()
            pg_execute_query(query=sql_query, conn_obj=conn_obj)

        with open(migrations_path + '/increment_mart_d_customer.sql') as file:
            sql_query = file.read()
            pg_execute_query(query=sql_query, conn_obj=conn_obj)

        with open(migrations_path + '/increment_mart_d_city.sql') as file:
            sql_query = file.read()
            pg_execute_query(query=sql_query, conn_obj=conn_obj)

        with open(migrations_path + '/increment_mart_d_calendar.sql') as file:
            sql_query = file.read()
            pg_execute_query(query=sql_query, conn_obj=conn_obj)

        with open(migrations_path + '/increment_mart_f_sales.sql') as file:
            sql_query = file.read()
            pg_execute_query(query=sql_query, conn_obj=conn_obj)


# вот это дата, за которую были последние данные в backfill
# '2022-06-14 00:00:00'
date_time_str = Variable.get('etl_backfill_end_date')
dt_start_obj = datetime.strptime(date_time_str + ' +0300', '%Y-%m-%d %H:%M:%S %z')
# добавим два дня
dt_start_obj2 = dt_start_obj + timedelta(days=2)
# a вот это - завтра
end_date_obj = datetime.today() + timedelta(days=1)


with DAG(
        'etl_increment',
        default_args={
            'owner': 'student',
            'email': ['student@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'depends_on_past': True,
            'wait_for_downstream': True
        },
        description='Increment S3 to Postgres',
        schedule_interval='@daily',
        catchup=True,
        start_date=dt_start_obj2,
        # {{ ds }}: The DAG run’s logical date as YYYY-MM-DD.
        # Same as {{ dag_run.logical_date | ds }}.
        # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
        end_date=end_date_obj,
        params={'business_dt': '{{ ds }}'}
) as dag:
    get_increment_task = PythonOperator(
        task_id='get_increment_task',
        python_callable=get_increment_request,
        op_kwargs={
            'conn_name': 'create_files_api',
            'business_dt': dag.params['business_dt']
        },
        dag=dag)

    get_files_task = PythonOperator(
        task_id='get_files_task',
        python_callable=get_files_from_s3,
        op_kwargs={
            'business_dt': dag.params['business_dt']
        },
        dag=dag)

    load_user_order_log = PythonOperator(
        task_id='load_user_order_log',
        python_callable=load_file_to_pg,
        op_kwargs={
            'business_dt': dag.params['business_dt'],
            'filebase': 'user_orders_log',  # _orderS_
            'pg_table': 'staging.user_order_log',
            'conn_obj': pg_conn
        },
        dag=dag)

    (
        get_increment_task
        >> get_files_task
        >> [load_user_order_log]
        # >> [
        #     load_customer_research,
        #     load_user_order_log,
        #     load_user_activity_log,
        #     load_price_log
        # ]
    )