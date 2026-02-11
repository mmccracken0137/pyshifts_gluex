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
s_date = input('Enter start date in YYYY-MM-DD format: ')
e_date = input('Enter end date in YYYY-MM-DD format:   ')
# s_date = '2025-08-01'
# e_date = '2025-08-03'

start_date = dt.datetime.strptime(s_date, '%Y-%m-%d')
end_date = dt.datetime.strptime(e_date, '%Y-%m-%d')
n_days = (end_date - start_date).days

print('Reading shifts schedule entries for', start_date.strftime('%Y-%m-%d'),
      'to', end_date.strftime('%Y-%m-%d'), f'({n_days} days)...')


# connect to db
print()
pwd = getpass('Enter gluexshifts db password: ')
shifts_db = mysql.connector.connect(host='halldweb', user='gluexshiftbot',
                                    password=pwd, database='gluexshifts')
shifts_cursor = shifts_db.cursor()

iter_date = start_date # datetime objects are immutable :^)
print('Reading accelerator, expert, and novice schedules from db...')

# creat output files
acc_out_file = open(f'./temp_files/read_acc_{s_date}_{e_date}_{t_date}.csv', 'w')
acc_out_file.write('acc_date,exp,energy,acc_day,shiftdate\n')
exp_out_file = open(f'./temp_files/read_expert_{s_date}_{e_date}_{t_date}.csv', 'w')
exp_out_file.write('shift_date,owl,day,eve,shiftdate\n')
nov_out_file = open(f'./temp_files/read_novice_{s_date}_{e_date}_{t_date}.csv', 'w')
nov_out_file.write('shift_date,owl,day,eve,shiftdate\n')


while iter_date <= end_date:
    # accelerator sched
    scmd = "SELECT acc_date, exp, energy, acc_day, shiftdate FROM acc_sched WHERE shiftdate = %s"
    print(scmd % iter_date.strftime('%Y-%m-%d'))
    shifts_cursor.execute(scmd, [iter_date.strftime('%Y-%m-%d')])
    for (acc_date, exp, energy, acc_day, shiftdate) in shifts_cursor:
        print(f'{acc_date},{exp},{energy},{acc_day},{shiftdate}')
        acc_out_file.write(f'{acc_date},{exp},{energy},{acc_day},{shiftdate}\n')

    # expert sched
    scmd = "SELECT shift_date, owl, day, eve, shiftdate FROM expert WHERE shiftdate = %s"
    print(scmd % iter_date.strftime('%Y-%m-%d'))
    shifts_cursor.execute(scmd, [iter_date.strftime('%Y-%m-%d')])
    for (shift_date, owl, day, eve, shiftdate) in shifts_cursor:
        print(f'{shift_date},{owl},{day},{eve},{shiftdate}')
        exp_out_file.write(f'{shift_date},{owl},{day},{eve},{shiftdate}\n')

    # novice sched
    scmd = "SELECT shift_date, owl, day, eve, shiftdate FROM novice WHERE shiftdate = %s"
    print(scmd % iter_date.strftime('%Y-%m-%d'))
    shifts_cursor.execute(scmd, [iter_date.strftime('%Y-%m-%d')])
    for (shift_date, owl, day, eve, shiftdate) in shifts_cursor:
        print(f'{shift_date},{owl},{day},{eve},{shiftdate}')
        nov_out_file.write(f'{shift_date},{owl},{day},{eve},{shiftdate}\n')


    sleep(0.1)
    iter_date += dt.timedelta(days=1)
    
acc_out_file.close()
exp_out_file.close()
nov_out_file.close()

print()

