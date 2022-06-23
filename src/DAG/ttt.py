import sys
import requests

req_host = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/'
report_id = 'TWpBeU1pMHdOaTB5TWxRd01Eb3hOem95TkFselpYSm5aV2xmWW1GeVlXNXZkZz09'
nickname = "sergei_baranov"
cohort = "1"
business_dt = '2022-06-15 00:00:00'
business_dt = business_dt.strip().replace(' ', 'T')
resp = requests.get(
    f"{req_host}/get_increment?report_id={report_id}&date={business_dt}",
    headers={
        "X-Nickname": nickname,
        "X-Cohort": cohort,
        "X-API-KEY": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
        "X-Project": 'True'
    }
)

print(resp.content)

sys.exit()

# dir_path = os.path.dirname(os.path.abspath(__file__))
# migrations_path = os.path.dirname(
#     dir_path + '/../../migrations/'
# )

# with open(migrations_path + '/backfill_mart_truncate.sql') as file:
#     truncate_mart_sql_query = file.read()

# print(truncate_mart_sql_query)

# sys.exit()

# import json
# import requests


# url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'

# nickname = "sergei_baranov"
# cohort = "1"

# headers = {
#    "X-API-KEY": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
#    "X-Nickname": nickname,
#    "X-Cohort": cohort
# }

# task_id = 'MjAyMi0wNS0yNFQyMTozMjoyMwlzZXJnZWlfYmFyYW5vdg=='
# report_method_url = '/get_report'
# payload = {'task_id': task_id}
# r = requests.get(url + report_method_url, params=payload, headers=headers)
# response_dict = json.loads(r.content)
# print(response_dict)
# report_id = response_dict['data']['report_id']
# print(report_id)

# TWpBeU1pMHdOUzB5TkZReU1Ub3pNam95TXdselpYSm5aV2xmWW1GeVlXNXZkZz09

# ----

import time
import requests
import json
from airflow.hooks.base import BaseHook
import boto3
from botocore.exceptions import ClientError

# boto3.set_stream_logger(name='botocore')


etl_task_id = ''
etl_report_id = ''


def create_files_request(conn_name, business_dt):
    """
    даём задание на /generate_report,
    task_id кладём в переменные;
    далее делей-таска, а потом взять report_id по task_id,
    и по report_id забрать файлы - это сделает get_files_from_s3
    """

    global etl_task_id

    # conn = BaseHook.get_connection(conn_name)
    # req_host = conn.host
    req_host = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/'
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
    print(r.content)
    response_dict = json.loads(r.content)
    print(response_dict)
    task_id = response_dict['task_id']
    etl_task_id = task_id


def get_report_request(conn_name):
    """
    получаем report_id по переменной etl_task_id через /get_report,
    сохраняем в переменную etl_report_id,
    и далее по нему забираем файлы через s3 уже отдельной таской
    (get_files_from_s3)
    """

    # conn = BaseHook.get_connection(conn_name)
    # req_host = conn.host
    req_host = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/'

    global etl_task_id
    global etl_report_id

    print("etl_task_id: " + etl_task_id)

    task_id = etl_task_id
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
    print(resp)
    if resp['status'] != 'SUCCESS':
        raise ValueError('task status is not SUCCESS (' + resp['status'] + ')')
    report_id = resp['data']['report_id']
    if report_id is None:
        raise ValueError('task report_id is None')
    etl_report_id = report_id


def get_files_from_s3(business_dt):
    """
    забираем файлы через s3 по report_id из переменной etl_report_id,
    стейджим их в директорию /file_staging
    """

    global etl_report_id

    report_id = etl_report_id
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
    filenames = [
        'customer_research.csv', # 'custom_research.csv',
        'user_orders_log.csv', # 'user_order_log.csv',
        'user_activity_log.csv'# ,
        # 'price_log.csv'
    ]
    for s3_filename in filenames:
        local_filaname = '/home/s_baranov/temp/' + dt + '_' + s3_filename
        key_i = key + s3_filename
        # try:
        s3.download_file(
            Bucket=bucket_name,
            Key=key_i,
            Filename=local_filaname
        )
        # except ClientError as e:
        #     print("boto3 error while downloading " + s3_filename + " (" + e.response['Error']['Message'] + ")")


ds = '2022-06-05'
create_files_request('create_files_api', ds)
time.sleep(100)
try:
    get_report_request('create_files_api')
except ValueError:
    time.sleep(100)
    get_report_request('create_files_api')
get_files_from_s3(ds)
