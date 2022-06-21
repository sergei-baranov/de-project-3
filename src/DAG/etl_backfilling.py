import os
from datetime import datetime, timedelta
import psycopg2
import boto3
import pandas as pd
import requests
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import numpy
from psycopg2.extensions import register_adapter, AsIs

