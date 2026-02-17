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

def date_from_shift(level, shift, date):
    s_date = shiftdate.replace(hour=0, minute=0, second=0)
    e_date = shiftdate.replace(hour=8, minute=0, second=0)
    if level == 'exp':
        if shift == 'day': 
            s_date += dt.timedelta(hour=8)
            e_date += dt.timedelta(hour=8)
        if shift == 'eve': 
            s_date += dt.timedelta(hour=8)
            e_date += dt.timedelta(hour=8)
    elif level == 'nov':
        s_date += dt.timedelta(hour=4)
        e_date += dt.timedelta(hour=4)
        if shift == 'day': 
            s_date += dt.timedelta(hour=8)
            e_date += dt.timedelta(hour=8)
        if shift == 'eve': 
            s_date += dt.timedelta(hour=8)
            e_date += dt.timedelta(hour=8)
    return s_date, e_date

#'''
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
        exp_out_file.write(f'{shift_date},{owl},{day},{eve},{shiftdate}\n')

    # novice sched
    scmd = "SELECT shift_date, owl, day, eve, shiftdate FROM novice WHERE shiftdate = %s"
    print(scmd % iter_date.strftime('%Y-%m-%d'))
    shifts_cursor.execute(scmd, [iter_date.strftime('%Y-%m-%d')])
    for (shift_date, owl, day, eve, shiftdate) in shifts_cursor:
        print(f'{shift_date},{owl},{day},{eve},{shiftdate}')
        nov_out_file.write(f'{shift_date},{owl},{day},{eve},{shiftdate}\n')

    sleep(0.3)
    iter_date += dt.timedelta(days=1)
    
exp_out_file.close()
nov_out_file.close()
#'''

'''
exp_file = open('../check_expert_{s_date}_{e_date}_{t_date}.csv', 'r')
nov_file = open('../check_novice_{s_date}_{e_date}_{t_date}.csv', 'r')
exp_lin = exp_file.readline()
exp_lin = exp_file.readline()
nov_lin = nov_file.readline()
nov_lin = nov_file.readline()

shifters = {}
shift_types = ['owl, 'day', 'eve']
while len(exp_lin) > 1 and len(nov_lin) > 1:
    exp_arr = exp_lin.split(',')
    date = dt.datetime.strptime(exp_arr[-1], '%Y-%m-%d')
    for i, sh_type in enumerate(shift_types):
        person = exp_arr[i+1]
        if person not in shifters.keys():
            shifters[person] = {'shifts': []}
        shifters[person]['shifts'].append(date_from_shift('exp', sh_type, date))

    nov_arr = exp_lin.split(',')
    date = dt.datetime.strptime(nov_arr[-1], '%Y-%m-%d')
    for i, sh_type in enumerate(shift_types):
        person = nov_arr[i+1]
        if person not in shifters.keys():
            shifters[person] = {'shifts': []}
        shifters[person]['shifts'].append(date_from_shift('exp', sh_type, date))

    exp_lin = exp_file.readline()
    nov_lin = nov_file.readline()
    
exp_file.close()
nov_file.close()
'''
