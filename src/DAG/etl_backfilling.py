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


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

pg_conn = BaseHook.get_connection('pg_connection')

def create_files_request(conn_name, business_dt):
    """
    даём задание на /generate_report,
    task_id кладём в переменные;
    далее делей-таска, а потом взять report_id по task_id,
    и по report_id забрать файлы - это сделает get_files_from_s3
    """

    conn = BaseHook.get_connection(conn_name)
    req_host = conn.host
    # req_host = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/'
    nickname = "sergei_baranov"
    cohort = "1"
    headers = {
        "X-API-KEY": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
        "X-Nickname": nickname,
        "X-Cohort": cohort
    }
    method_url = '/generate_report'

    r = requests.post(
        req_host + method_url,
        headers=headers,
        data={
            'business_dt': business_dt
        })

    response_dict = json.loads(r.content)
    task_id = response_dict['task_id']

    Variable.set(key='etl_task_id', value=task_id)


def get_report_request(conn_name):
    """
    получаем report_id по переменной etl_task_id через /get_report,
    сохраняем в переменную etl_report_id,
    и далее по нему забираем файлы через s3 уже отдельной таской
    (get_files_from_s3)
    """

    conn = BaseHook.get_connection(conn_name)
    req_host = conn.host
    # req_host = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/'

    task_id = Variable.get('etl_task_id')

    nickname = "sergei_baranov"
    cohort = "1"
    resp = requests.get(
        f"{req_host}/get_report?task_id={task_id}",
        headers={
            "X-API-KEY": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
            "X-Nickname": nickname,
            "X-Cohort": cohort
        }
    ).json()

    if resp['status'] != 'SUCCESS':
        raise ValueError('task status is not SUCCESS (' + resp['status'] + ')')

    report_id = resp['data']['report_id']
    if report_id is None:
        raise ValueError('task report_id is None')
    Variable.set(key='etl_report_id', value=report_id)

    end_date = resp['data']['end_date']
    if end_date is None:
        raise ValueError('task end_date is None')
    Variable.set(key='etl_backfill_end_date', value=end_date)

    


def get_files_from_s3(business_dt):
    """
    забираем файлы через s3 по report_id из переменной etl_report_id,
    стейджим их в директорию /file_staging
    """

    report_id = Variable.get('etl_report_id')
    session = boto3.session.Session()
    bucket_name = 's3-sprint3'
    key = f"cohort_1/sergei_baranov/{report_id}/"
    # TWpBeU1pMHdOUzB5TkZReU1Ub3pNam95TXdselpYSm5aV2xmWW1GeVlXNXZkZz09
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id='YCAJEWXOyY8Bmyk2eJL-hlt2K',
        aws_secret_access_key='YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA'
    )
    dt = business_dt.replace('-', '')
    filenames_OLD = [
        'customer_research.csv',  # 'custom_research.csv',
        'user_orders_log.csv',  # 'user_order_log.csv',
        'user_activity_log.csv'  # ,
        # 'price_log.csv'
    ]
    filenames = [
        'user_orders_log.csv'
    ]
    for s3_filename in filenames:
        local_filaname = '/file_staging/' + dt + '_' + s3_filename
        key_i = key + s3_filename
        try:
            s3.download_file(
                Bucket=bucket_name,
                Key=key_i,
                Filename=local_filaname
            )
        except ClientError as e:
            print("boto3 error while downloading " + s3_filename + " (" + e.response['Error']['Message'] + ")")


def load_file_to_pg(business_dt, filebase, pg_table, conn_obj):
    truncate_sql = f"TRUNCATE {pg_table};"

    filename = '/file_staging/' + business_dt.replace('-', '') + '_' + filebase + '.csv'

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


with DAG(
        'etl_backfilling',
        default_args={
            'owner': 'student',
            'email': ['student@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Move user data from files on S3 to Postgres',
        schedule_interval='@once',
        catchup=False,
        start_date=datetime.today() - timedelta(days=9),
        end_date=datetime.today() - timedelta(days=8),
        # {{ ds }}: The DAG run’s logical date as YYYY-MM-DD.
        # Same as {{ dag_run.logical_date | ds }}.
        # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
        params={'business_dt': '{{ ds }}'}
) as dag:
    set_gen_report_task = PythonOperator(
        task_id='set_gen_report_task',
        python_callable=create_files_request,
        op_kwargs={
            'conn_name': 'create_files_api',
            'business_dt': dag.params['business_dt']
        },
        dag=dag)

    delay_task = BashOperator(
        task_id='delay_task',
        bash_command='sleep 120s',
        dag=dag)

    get_report_task = PythonOperator(
        task_id='get_report_task',
        python_callable=get_report_request,
        op_kwargs={
            'conn_name': 'create_files_api'
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

    migrations_path = '/migrations/'

    with open(migrations_path + '/backfill_mart_truncate.sql') as file:
        truncate_mart_sql_query = file.read()
    truncate_mart = PythonOperator(
        task_id='truncate_mart',
        python_callable=pg_execute_query,
        op_kwargs={
            'query': truncate_mart_sql_query,
            'conn_obj': pg_conn
        },
        dag=dag)

    with open(migrations_path + '/backfill_mart_dimensions.sql') as file:
        dim_upd_sql_query = file.read()
    update_dimensions = PythonOperator(
        task_id='update_dimensions',
        python_callable=pg_execute_query,
        op_kwargs={
            'query': dim_upd_sql_query,
            'conn_obj': pg_conn
        },
        dag=dag)

    with open(migrations_path + '/backfill_mart_facts.sql') as file:
        facts_upd_sql_query = file.read()
    update_facts = PythonOperator(
        task_id='update_facts',
        python_callable=pg_execute_query,
        op_kwargs={
            'query': facts_upd_sql_query,
            'conn_obj': pg_conn
        },
        dag=dag)

    (
        set_gen_report_task
        >> delay_task
        >> get_report_task
        >> get_files_task
        >> [load_user_order_log]
        # >> [
        #     load_customer_research,
        #     load_user_order_log,
        #     load_user_activity_log,
        #     load_price_log
        # ]
        >> truncate_mart
        >> update_dimensions
        >> update_facts
    )

if __name__ == 'main':
    pass
