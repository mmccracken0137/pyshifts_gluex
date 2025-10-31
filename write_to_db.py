import datetime as dt
import mysql.connector
import pandas as pd
import os
import csv
from rich import print
from rich.progress import track
from time import sleep

today_str = dt.datetime.today().strftime('%Y-%m-%d')
leader_df, worker_df = '', ''
if os.path.exists('temp_leader_' + today_str + '.csv'):
    leader_df = pd.read_csv('temp_leader_' + today_str + '.csv')
else:
    print('Error: tentative leader schedule file not found.')
    exit()
    
if os.path.exists('temp_worker_' + today_str + '.csv'):
    worker_df = pd.read_csv('temp_worker_' + today_str + '.csv')
else:
    print('Error: tentative worker schedule file not found.')
    exit()

print('Shift assignments files found:')
print('\tleader shifts:', 'temp_leader_' + today_str + '.csv')
print('\tworker shifts:', 'temp_leader_' + today_str + '.csv')

print()
write = input('Do you wish to write these shift assignments to the db [y/N]? ')
if write != 'y':
    exit()

print()
pwd = input('Enter gluexshifts db password: ')

shifts_db = mysql.connector.connect(host='halldweb', user='gluexshiftbot',
                                    password=pwd, database='gluexshifts')

shifts_cursor = shifts_db.cursor()

for i, l_row in leader_df.iterrows():
    scmd = "INSERT INTO expert (shift_date, owl, day, eve, shiftdate) VALUES (%s, %s, %s, %s, %s)"
    vals = (l_row['shift_date'], l_row['owl'], l_row['day'], l_row['eve'], l_row['shiftdate'])
    print(scmd % vals)
    shifts_cursor.execute(scmd, vals)

for i, w_row in worker_df.iterrows():
    scmd = "INSERT INTO novice (shift_date, owl, day, eve, shiftdate) VALUES (%s, %s, %s, %s, %s)"
    vals = (l_row['shift_date'], l_row['owl'], l_row['day'], l_row['eve'], l_row['shiftdate'])
    print(scmd % vals)
    shifts_cursor.execute(scmd, vals)

print()
print('The assignments above have been inserted into the db.')
commit = input('Do you wish to commit the assignments to the db [y/N]? ')
if commit != 'y':
    exit()

commit = input('Are you REALLY sure you wish to commit the assignments to the db [y/N]? ')
if commit != 'y':
    exit()

shifts_db.commit()
