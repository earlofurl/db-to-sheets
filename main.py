import os
import boto3
import botocore
import pygsheets
import psycopg2 as pg
import pandas as pd
import json  # currently only used to verify that file is being downloaded from S3
# logging functionality added
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info('got event{}'.format(event))
    logger.error('something went wrong')

    # establish s3 interaction client. provide variables for later actions
    s3 = boto3.client('s3')
    BUCKET_NAME = 'motocloudstor'
    KEY = 'db-sync-234405-fa350d5692fd.json'
    LOCAL_FILENAME = '/tmp/{}'.format(os.path.basename(KEY))

    # try statement contains additional JSON code for verifying in logs that file was downloaded.
    try:
        s3.download_file(Bucket=BUCKET_NAME, Key=KEY, Filename=LOCAL_FILENAME)
        with open(LOCAL_FILENAME, 'r') as handle:
            parsed = json.load(handle)
            print(json.dumps(parsed, indent=4, sort_keys=True))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    # psycopg2 used as db engine for pandas
    conn = pg.connect(host='motocloudbase.ctiujpo8nyuy.us-west-2.rds.amazonaws.com',
                      dbname='motocloudbase1',
                      user='motomaster',
                      password='ZXRGJfpMRe3S',
                      port='5432')
    # pandas grabs all data from masterlabresults
    df = pd.read_sql_query('select * from masterlabresults', con=conn)

    # authorization into gsheets. Opens target sheet. Then sets data from query grabbed above into the sheet.
    gc = pygsheets.authorize(service_file=LOCAL_FILENAME)
    wks = gc.open("New 2019 Moto Lab Results").sheet1
    wks.set_dataframe(df, (1, 1))
