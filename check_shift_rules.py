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
    s_date = date.replace(hour=0, minute=0, second=0)
    e_date = date.replace(hour=8, minute=0, second=0)
    if level == 'exp':
        if shift == 'day': 
            s_date += dt.timedelta(hours=8)
            e_date += dt.timedelta(hours=8)
        if shift == 'eve': 
            s_date += dt.timedelta(hours=8)
            e_date += dt.timedelta(hours=8)
    elif level == 'nov':
        s_date += dt.timedelta(hours=4)
        e_date += dt.timedelta(hours=4)
        if shift == 'day': 
            s_date += dt.timedelta(hours=8)
            e_date += dt.timedelta(hours=8)
        if shift == 'eve': 
            s_date += dt.timedelta(hours=8)
            e_date += dt.timedelta(hours=8)
    return s_date, e_date

def get_shortest_down(shifts):
    shortest_hrs = 1e10
    for i in range(len(shifts)-1):
        diff = (shifts[i+1][0] - shifts[i][1]).total_seconds() / 3600
        print(shifts[i+1][0], shifts[i][1], diff)
        shortest_hrs = min(diff, shortest_hrs)
        # if diff < shortest_hrs:
        #     shortest_hrs = diff
    return shortest_hrs

def get_longest_stint(shifts):
    stint, longest_stint = 1, 1
    for i in range(len(shifts)-1):
        diff = (shifts[i+1][0] - shifts[i][1]).total_seconds() / 3600
        if diff <= 24:
            stint += 1
            longest_stint = max(stint, longest_stint)
        else: 
            stint = 0
    return longest_stint

'''
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
'''


exp_file = open(f'./check_expert_{s_date}_{e_date}_{t_date}.csv', 'r')
nov_file = open(f'./check_novice_{s_date}_{e_date}_{t_date}.csv', 'r')
exp_lin = exp_file.readline()
exp_lin = exp_file.readline()
nov_lin = nov_file.readline()
nov_lin = nov_file.readline()

shifters = {}
shift_types = ['owl', 'day', 'eve']
n_days = 0
while len(exp_lin) > 1 and len(nov_lin) > 1:
    exp_arr = exp_lin.split(',')
    date = dt.datetime.strptime(exp_arr[-1].strip(), '%Y-%m-%d')
    for i, sh_type in enumerate(shift_types):
        person = exp_arr[i+1]
        if person not in shifters.keys():
            shifters[person] = {'shifts': []}
        shifters[person]['shifts'].append(date_from_shift('exp', sh_type, date))

    nov_arr = nov_lin.split(',')
    date = dt.datetime.strptime(nov_arr[-1].strip(), '%Y-%m-%d')
    for i, sh_type in enumerate(shift_types):
        person = nov_arr[i+1]
        if person not in shifters.keys():
            shifters[person] = {'shifts': []}
        shifters[person]['shifts'].append(date_from_shift('nov', sh_type, date))

    exp_lin = exp_file.readline()
    nov_lin = nov_file.readline()
    n_days += 1
    
exp_file.close()
nov_file.close()

shifters = {k: v for k, v in sorted(shifters.items())}
print(shifters)
print()
print(f'read shifts for {n_days} accelerator days')
print(f'found {len(shifters)} unique shifters')
print('checking rules for individual shifters')

shifters_summary = {}
for pers in shifters.keys():
    print()
    print(f'checking rules for {pers}')
    shifters_summary[pers] = {}
    assigns = sorted(shifters[pers]['shifts'])
    shifters_summary[pers]['n_shifts'] = len(shifters[pers]['shifts'])
    brk = get_shortest_down(assigns)
    shifters_summary[pers]['shortest_break_hrs'] = brk
    stint = get_longest_stint(assigns)
    shifters_summary[pers]['longest_stint_days'] = stint

# print(shifters_summary)

summ_df = pd.DataFrame(shifters_summary).T
# summ_df = summ_df.reset_index()
summ_df.to_csv(f'./temp_files/shifts_rules_summary_{t_date}.csv')
print(summ_df)