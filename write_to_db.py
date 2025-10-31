import datetime as dt
import mysql.connector
import pandas as pd
import os
import csv
from rich import print
from rich.progress import track
from getpass import getpass

today_str = dt.datetime.today().strftime('%Y-%m-%d')
expert_df, novice_df = '', ''
if os.path.exists('temp_expert_' + today_str + '.csv'):
    expert_df = pd.read_csv('temp_expert_' + today_str + '.csv')
else:
    print('Error: tentative expert schedule file not found.')
    exit()
    
if os.path.exists('temp_novice_' + today_str + '.csv'):
    novice_df = pd.read_csv('temp_novice_' + today_str + '.csv')
else:
    print('Error: tentative novice schedule file not found.')
    exit()

if os.path.exists('temp_acc_sched_' + today_str + '.csv'):
    acc_sched_df = pd.read_csv('temp_acc_sched_' + today_str + '.csv')
else:
    print('Error: tentative novice schedule file not found.')
    exit()

print()
print('Accelerator schedule file found:', '\n\ttemp_acc_sched_' + today_str + '.csv')

print()
print('Shift assignments files found:')
print('\texpert shifts:', 'temp_expert_' + today_str + '.csv')
print('\tnovice shifts:', 'temp_expert_' + today_str + '.csv')

print()
write = input('Do you wish to write the schedule and shift assignments to the db [y/N]? ')
if write != 'y':
    exit()

print()
pwd = getpass('Enter gluexshifts db password: ')

shifts_db = mysql.connector.connect(host='halldweb', user='gluexshiftbot',
                                    password=pwd, database='gluexshifts')

shifts_cursor = shifts_db.cursor()

print('Writing accelerator schedule to db...')
for i, a_row in track(acc_sched_df.iterrows()):
    scmd = "INSERT INTO acc_sched (acc_date, exp, energy, acc_day, shiftdate) VALUES (%s, %s, %s, %s, %s)"
    vals = (a_row['acc_date'], a_row['exp'], a_row['energy'], a_row['acc_day'], a_row['shiftdate'])
    # print(scmd % vals)
    shifts_cursor.execute(scmd, vals)

for i, e_row in expert_df.iterrows():
    scmd = "INSERT INTO expert (shift_date, owl, day, eve, shiftdate) VALUES (%s, %s, %s, %s, %s)"
    vals = (e_row['shift_date'], e_row['owl'], e_row['day'], e_row['eve'], e_row['shiftdate'])
    print(scmd % vals)
    shifts_cursor.execute(scmd, vals)

for i, n_row in novice_df.iterrows():
    scmd = "INSERT INTO novice (shift_date, owl, day, eve, shiftdate) VALUES (%s, %s, %s, %s, %s)"
    vals = (n_row['shift_date'], n_row['owl'], n_row['day'], n_row['eve'], n_row['shiftdate'])
    print(scmd % vals)
    shifts_cursor.execute(scmd, vals)

print()
print('The assignments above have been inserted into the db.')
commit = input('Do you wish to commit the assignments to the db [y/N]? ')
if commit != 'y':
    exit()

print()
print('Are you REALLY sure you wish to commit the assignments to the db?')
print('This would take a LOT of time and effort to clean up...')
commit = input('Commit [y/N]? ')
if commit != 'y':
    exit()

shifts_db.commit()
