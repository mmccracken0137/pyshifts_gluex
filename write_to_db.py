import datetime as dt
import mysql.connector
import pandas as pd
import os
import csv
from rich import print

today_str = dt.datetime.today().strftime('%Y-%m-%d')
leader_df, worker_df = '', ''
if os.path.exists('temp_leader_' + today_str + '.tsv'):
    leader_df = pd.read_csv('temp_leader_' + today_str + '.tsv', sep='\t')
else:
    print('Error: tentative leader schedule file not found.')
    exit()
    
if os.path.exists('temp_worker_' + today_str + '.tsv'):
    worker_df = pd.read_csv('temp_worker_' + today_str + '.tsv', sep='\t')
else:
    print('Error: tentative worker schedule file not found.')
    exit()
    
pwd = input('Enter gluexshifts db password:')

shifts_db = mysql.connector.connect(host='halldweb', user='gluexshiftbot',
                                    password=pwd, database='GlueX_Shifts')

for l_row in leader_df.iterrows():
    # print(l_row)
    scmd = "INSERT INTO expert (shift_date, owl, day, eve, shiftdate) VALUES (%s, %s, %s, %s, %s)"
    vals = (l_row['shift_date'], l_row['owl'], l_row['day'], l_row['eve'], l_row['shiftdate'])
    print(scmd)
    # mycursor.execute(sql, val)
