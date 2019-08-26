import beatbox
import psycopg2
from python_code.utils import fetch_json_data
import boto3
from botocore.client import Config
import logging


logger = logging.getLogger(__name__)

def bi_connection(env):
    """
        Method to establish a connection with database and return connection object
    :return: Database connection object here..
    """
    if env == 'prod':
        data = fetch_json_data('bi_prod')
    elif env == 'dev':
        data = fetch_json_data('bi_dev')

    bi_conn = psycopg2.connect(
        host=data['hostname'],
        dbname=data['dbname'],
        user=data['user'],
        password=data['password']
    )
    return bi_conn


def sfdc_connection():

    sfdc_conn = beatbox.PythonClient()
    data = fetch_json_data('sfdc_prod')
    sfdc_conn.login(data['username'], data['password'])
    return sfdc_conn

def front_connection():

    data = fetch_json_data('front_prod')
    front_conn = psycopg2.connect(
        host=data['hostname'],
        dbname=data['dbname'],
        user=data['user'],
        password=data['password']
    )
    return front_conn

def get_s3_resource(env):

    if env == 'prod':
        s3_bucket = fetch_json_data('s3_bucket_prod')
    elif env == 'dev':
        s3_bucket = fetch_json_data('s3_bucket_dev')

    s3_resource = boto3.resource\
    (
        's3',
        aws_access_key_id=s3_bucket['ACCESS_KEY_ID'],
        aws_secret_access_key=s3_bucket['ACCESS_SECRET_KEY'],
        config=Config(signature_version='s3v4')
    )
    return s3_resource

def get_s3_client(env):

    if env == 'prod':
        s3_bucket = fetch_json_data('s3_bucket_prod')
    elif env == 'dev':
        s3_bucket = fetch_json_data('s3_bucket_dev')

    s3_client = boto3.client\
    (
        's3', aws_access_key_id=s3_bucket['ACCESS_KEY_ID'],
        aws_secret_access_key=s3_bucket['ACCESS_SECRET_KEY'],
        config=Config(signature_version='s3v4')
    )
    return s3_client
