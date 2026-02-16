import datetime as dt
import mysql.connector
import pandas as pd
import os
import csv
from rich import print
from rich.progress import track
from getpass import getpass
from time import sleep


t_date = dt.datetime.today().strftime('%Y-%m-%d')

print()
# s_date = input('Enter start date in YYYY-MM-DD format: ')
# e_date = input('Enter end date in YYYY-MM-DD format:   ')
# n_date = input('Enter the new start date in YYYY-MM-DD format:')
s_date = '2026-01-01'
e_date = '2026-12-25'

start_date = dt.datetime.strptime(s_date, '%Y-%m-%d')
end_date = dt.datetime.strptime(e_date, '%Y-%m-%d')

# connect to db
print()
pwd = getpass('Enter gluexshifts db password: ')
shifts_db = mysql.connector.connect(host='halldweb', user='gluexshiftbot',
                                    password=pwd, database='gluexshifts')
shifts_cursor = shifts_db.cursor()

# creat output files
exp_out_file = open(f'./temp_files/check_expert_{s_date}_{e_date}_{t_date}.csv', 'w')
exp_out_file.write('shift_date,owl,day,eve,shiftdate\n')
nov_out_file = open(f'./temp_files/check_novice_{s_date}_{e_date}_{t_date}.csv', 'w')
nov_out_file.write('shift_date,owl,day,eve,shiftdate\n')

iter_date = start_date # datetime objects are immutable :^)
print('Reading expert and novice schedules from db...')

while iter_date <= end_date:
    print()
    print('Reading entries from', iter_date.strftime('%Y-%m-%d'), '...')

    # expert sched
    scmd = "SELECT shift_date, owl, day, eve, shiftdate FROM expert WHERE shiftdate = %s"
    print(scmd % iter_date.strftime('%Y-%m-%d'))
    shifts_cursor.execute(scmd, [iter_date.strftime('%Y-%m-%d')])
    for (shift_date, owl, day, eve, shiftdate) in shifts_cursor:
        print(f'{shift_date},{owl},{day},{eve},{shiftdate}')
        exp_out_file.write(f'{shift_date},{owl},{day},{eve},{shiftdate}')

    # novice sched
    scmd = "SELECT shift_date, owl, day, eve, shiftdate FROM novice WHERE shiftdate = %s"
    print(scmd % iter_date.strftime('%Y-%m-%d'))
    shifts_cursor.execute(scmd, [iter_date.strftime('%Y-%m-%d')])
    for (shift_date, owl, day, eve, shiftdate) in shifts_cursor:
        print(f'{shift_date},{owl},{day},{eve},{shiftdate}')
        nov_out_file.write(f'{shift_date},{owl},{day},{eve},{shiftdate}')

    sleep(0.3)
    iter_date += dt.timedelta(days=1)
    
exp_out_file.close()
nov_out_file.close()

print()

