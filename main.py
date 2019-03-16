#!usr/bin/env python3

import pygsheets
import psycopg2 as pg
import pandas as pd

gc = pygsheets.authorize(service_file='db-sync-234405-fa350d5692fd.json')

wks = gc.open("New 2019 Moto Lab Results").sheet1

conn = pg.connect("host='motocloudbase.ctiujpo8nyuy.us-west-2.rds.amazonaws.com' dbname='motocloudbase1' user='motomaster' password='ZXRGJfpMRe3S' port='5432'")
df = pd.read_sql_query('select * from masterlabresults', con=conn)

wks.set_dataframe(df, (1, 1))