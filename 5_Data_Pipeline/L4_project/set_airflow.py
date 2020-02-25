# Automation script to set the environment settings in Airflow
# according https://knowledge.udacity.com/questions/68955

from airflow import settings
from airflow.models import (Connection, Variable)
aws_credentials = Connection(
        conn_id='aws_credentials',
        conn_type='Amazon Web Services',
        host='',
        login='XXXXXXXXXXXXXXXXXXXXXX',
        password='YYYYYYYYYYYYYYYYYYYYYYY'
        #port=''
)
redshift = Connection(
        conn_id='redshift',
        conn_type='Postgres',
        host='redshift-cluster-1.AAAAAAAAAAAAAA.us-west-2.redshift.amazonaws.com',
        login='awsuser',
        password='ZZZZZZZZZZZZZZZZZZZ',
        port='5439',
        schema='dev'
)
s3_bucket = Variable(
        key = 's3_bucket',
        val = 'udacity-dend'
)
json_path = Variable(
        key = 'json_path',
        val = 's3://udacity-dend/log_json_path.json'
)

session = settings.Session()
session.add(aws_credentials)
session.add(redshift)
session.add(s3_bucket)
session.add(json_path)
session.commit()