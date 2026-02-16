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
s_date = '2026-03-13'
e_date = '2026-03-28'
n_date = '2026-08-20'

start_date = dt.datetime.strptime(s_date, '%Y-%m-%d')
end_date = dt.datetime.strptime(e_date, '%Y-%m-%d')
new_start = dt.datetime.strptime(n_date, '%Y-%m-%d')

n_days = (end_date - start_date).days
new_end = new_start + dt.timedelta(days=n_days)

print('Reading shifts schedule entries for', start_date.strftime('%Y-%m-%d'),
      'to', end_date.strftime('%Y-%m-%d'), f'({n_days + 1} days)...')
print('Transposing dates to begin on', new_start.strftime('%Y-%m-%d'),
      'and end on', new_end.strftime('%Y-%m-%d'))

# connect to db
print()
pwd = getpass('Enter gluexshifts db password: ')
shifts_db = mysql.connector.connect(host='halldweb', user='gluexshiftbot',
                                    password=pwd, database='gluexshifts')
shifts_cursor = shifts_db.cursor()

# creat output files
acc_out_file = open(f'./temp_files/copied_acc_{s_date}_{e_date}_{t_date}.csv', 'w')
acc_out_file.write('acc_date,exp,energy,acc_day,shiftdate\n')
exp_out_file = open(f'./temp_files/copied_expert_{s_date}_{e_date}_{t_date}.csv', 'w')
exp_out_file.write('shift_date,owl,day,eve,shiftdate\n')
nov_out_file = open(f'./temp_files/copied_novice_{s_date}_{e_date}_{t_date}.csv', 'w')
nov_out_file.write('shift_date,owl,day,eve,shiftdate\n')

iter_date = start_date # datetime objects are immutable :^)
iter_new_date = new_start
print('Reading accelerator, expert, and novice schedules from db...')

acc_energy = '11.8 GeV'
exp_status = 'GlueX2/JEF'

while iter_date <= end_date:
    print()
    print('Reading entries from', iter_date.strftime('%Y-%m-%d'),
          'and transposing to', iter_new_date.strftime('%Y-%m-%d'))
    # accelerator sched
    scmd = "SELECT acc_date, exp, energy, acc_day, shiftdate FROM acc_sched WHERE shiftdate = %s"
    print(scmd % iter_date.strftime('%Y-%m-%d'))
    shifts_cursor.execute(scmd, [iter_date.strftime('%Y-%m-%d')])
    for (acc_date, exp, energy, acc_day, shiftdate) in shifts_cursor:
        print(f'{acc_date},{exp_status},{acc_energy},{iter_new_date.strftime("%A")},{iter_new_date.strftime("%Y-%m-%d")}')
        acc_out_file.write(f'{iter_new_date.strftime("%-d-%b-%Y")},{exp_status},{acc_energy},{iter_new_date.strftime("%A")},{iter_new_date.strftime("%Y-%m-%d")}\n')

    # expert sched
    scmd = "SELECT shift_date, owl, day, eve, shiftdate FROM expert WHERE shiftdate = %s"
    print(scmd % iter_date.strftime('%Y-%m-%d'))
    shifts_cursor.execute(scmd, [iter_date.strftime('%Y-%m-%d')])
    for (shift_date, owl, day, eve, shiftdate) in shifts_cursor:
        print(f'{shift_date},{owl},{day},{eve},{iter_new_date.strftime("%Y-%m-%d")}')
        exp_out_file.write(f'{iter_new_date.strftime("%-d-%b-%Y")},{owl},{day},{eve},{iter_new_date.strftime("%Y-%m-%d")}\n')

    # novice sched
    scmd = "SELECT shift_date, owl, day, eve, shiftdate FROM novice WHERE shiftdate = %s"
    print(scmd % iter_date.strftime('%Y-%m-%d'))
    shifts_cursor.execute(scmd, [iter_date.strftime('%Y-%m-%d')])
    for (shift_date, owl, day, eve, shiftdate) in shifts_cursor:
        print(f'{shift_date},{owl},{day},{eve},{iter_new_date.strftime("%Y-%m-%d")}')
        nov_out_file.write(f'{iter_new_date.strftime("%-d-%b-%Y")},{owl},{day},{eve},{iter_new_date.strftime("%Y-%m-%d")}\n')

    sleep(0.5)
    iter_date += dt.timedelta(days=1)
    iter_new_date += dt.timedelta(days=1)
    
acc_out_file.close()
exp_out_file.close()
nov_out_file.close()

print()

