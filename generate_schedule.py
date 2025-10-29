import datetime as dt
import mysql.connector
import pandas as pd
import csv
from rich import print

def round_to_block(s, block_size=4):
    m = int(s / block_size)
    return int(m * block_size)
    
# load personpower data and calculate total effective shift takers

# treat inst data as dataframe:
power_df = pd.read_csv('personpower_2026run_TEST.csv')
power_df['total'] = power_df['leaders'] + power_df['workers']
# line below applies JLAB's 0.5 weighting (and others if applicable)
power_df['eff_N'] = (power_df['total'] * power_df['effect_frac']).astype(int)
calc_T = power_df['eff_N'].sum()
print('total effective shift takers:', calc_T)
total_RC_vols = power_df['rc'].sum()
print('total RC shifts volunteered:', total_RC_vols)

# calculate features of shift schedule
start_date = dt.datetime(2026, 2, 23)
end_date = dt.datetime(2026, 8, 1)
print('run start date:', start_date)
print('run end date:  ',end_date)
n_days = (end_date - start_date).days + 1
print('length of run: ', n_days, 'days')
if n_days / 4 != int(n_days / 4):
    print('ERROR: length of run not divisble by 4 days')
    exit()

n_leader = n_days * 3
n_worker = n_days * 3
n_RC = n_days
n_RC_weeks = n_days / 8
print('number of leader shifts: ', n_leader)
print('number of worker shifts: ', n_worker)
print('number of RC days:       ', n_RC)
print('number of RC weeks:      ', n_RC_weeks)
if n_RC > total_RC_vols:
    print('ERROR: not enough RCs!')
    exit()

# calculate number of leader and worker shifts for each institution
#worker shifts:  W*N/T 
#leader shifts:   (L+R)*N/T  minus the count of RC shifts that they provided 
power_df['n_worker_alloc'] = n_worker * power_df['eff_N'] / calc_T
power_df['n_leader_alloc'] = (n_leader + n_RC) * power_df['eff_N'] / calc_T - power_df['rc']
power_df['n_worker_avail'] = power_df['n_worker_alloc']
power_df['n_leader_avail'] = power_df['n_leader_alloc']

print(power_df)
print('total calculated leader shifts:', power_df['n_leader_alloc'].sum())
print('total calculated worker shifts:', power_df['n_worker_alloc'].sum())
summary_df = power_df.copy()

# now generate shifts schedule
iter_date = start_date
date_list, text_date_list = [], []
leader_assign = {'owl': [], 'day': [], 'eve': []}
worker_assign = {'owl': ['JLAB', 'JLAB'], 'day': ['JLAB', 'JLAB'], 'eve': ['JLAB', 'JLAB']}

# create copies of person power df for leader and worker assignments
leader_alloc_df = power_df.drop(columns=['n_worker_alloc', 'n_worker_avail']).copy()
worker_alloc_df = power_df.drop(columns=['n_leader_alloc', 'n_leader_avail']).copy()

#for i in range(n_days):
while iter_date <= end_date:
    # print(i, iter_date.strftime('%Y-%m-%d'))
    for t in ['owl', 'day', 'eve']:
        # leaders first
        # remove insts with zero or fewer shifts remaining
        leader_alloc_df = leader_alloc_df[leader_alloc_df['n_leader_avail'] > 0]    
        total_avail = leader_alloc_df['n_leader_avail'].sum()
        # calculate the sampling weights based on number of available shifts remaining
        leader_alloc_df['sample_weights'] = leader_alloc_df['n_leader_avail'] / total_avail
        leader_select_inst = leader_alloc_df.sample(1, weights='sample_weights')['inst'].values[0]
        # append the institution to the df for four shifts
        [leader_assign[t].append(leader_select_inst) for _ in range(4)]
        # decrement the allocation of the chosen institution
        leader_alloc_df.loc[leader_alloc_df['inst'] == leader_select_inst, 'n_leader_avail'] -= 4 
        
        # workers next... same steps as above
        worker_alloc_df = worker_alloc_df[worker_alloc_df['n_worker_avail'] > 0]    
        total_avail = worker_alloc_df['n_worker_avail'].sum()
        worker_alloc_df['sample_weights'] = worker_alloc_df['n_worker_avail'] / total_avail
        worker_select_inst = worker_alloc_df.sample(1, weights='sample_weights')['inst'].values[0]
        [worker_assign[t].append(worker_select_inst) for _ in range(4)]
        worker_alloc_df.loc[worker_alloc_df['inst'] == worker_select_inst, 'n_worker_avail'] -= 4 
    
    # append dates in blocks of four
    for _ in range(4):
        date_list.append(iter_date.strftime('%Y-%m-%d'))
        text_date_list.append(iter_date.strftime('%-d-%b-%Y'))
        iter_date = iter_date + dt.timedelta(days=1)

# remove last block from worker and replace with two days assigned to JLAB
for t in ['owl', 'day', 'eve']:
    [worker_assign[t].pop() for _ in range(4)]
    [worker_assign[t].append('JLAB') for _ in range(2)]

print('Inspect these data frames to see whether any institutions have too many available remaining:')
print(leader_alloc_df)
print(worker_alloc_df)

leader_df = pd.DataFrame({'shiftdate': date_list, 'shift_date': text_date_list, 
                          'owl': leader_assign['owl'],
                          'day': leader_assign['day'],
                          'eve': leader_assign['eve']})
worker_df = pd.DataFrame({'shiftdate': date_list, 'shift_date': text_date_list, 
                          'owl': worker_assign['owl'],
                          'day': worker_assign['day'],
                          'eve': worker_assign['eve']})
# print(leader_df)
# print(worker_df)

# write tentative shift assignments to tsv files for the user to review
leader_df.to_csv('temp_leader_' + dt.datetime.today().strftime('%Y-%m-%d') + '.tsv', sep='\t')
worker_df.to_csv('temp_worker_' + dt.datetime.today().strftime('%Y-%m-%d') + '.tsv', sep='\t')

# now let's do some summarizing of the assignments. get a copy of earlier power df
for t in ['owl', 'day', 'eve']:
    summary_df['leader_' + t] = len(leader_df[leader_df[t] == summary_df['inst']])
    

print()
print('Tentative shift assignments have been written to file(s).')
print('Please review these before writing to the db.')

# TO DO
# Write the mysql code to write these to the db
# How to run this on jlabl5?  Or can I run this locally and access the db from offsite?
