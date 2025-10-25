import datetime as dt
import mysql.connector
import pandas as pd
import csv
from rich import print

def round_to_block(s, block_size=4):
    m = int(s / block_size)
    return int(m * block_size)
    
# load personpower data and calculate total effective shift takers
# if done with dictionary, easier to modify individual values
# inst_dict = {}
# with open('personpower_2026run_20251025.csv', mode='r', encoding='utf-8') as csv_file:
#     line = csv_file.readline().strip()
#     keys = line.split(',')
#     line = csv_file.readline().strip()
#     while len(line)>2:
#         larr = line.split(',')
#         idict = {}
#         for i in range(len(keys)-1):
#             idict[keys[i+1]] = float(larr[i+1])
#         inst_dict[larr[0]] = idict
#         inst_dict[larr[0]]['total'] = inst_dict[larr[0]]['leaders'] + inst_dict[larr[0]]['workers']
#         inst_dict[larr[0]]['eff_N'] = inst_dict[larr[0]]['total'] * inst_dict[larr[0]]['effect_frac']
#         line = csv_file.readline().strip()
# print(inst_dict)

# treat inst data as dataframe:
personp_df = pd.read_csv('personpower_2026run_TEST.csv')
personp_df['total'] = personp_df['leaders'] + personp_df['workers']
# line below applies JLAB's 0.5 weighting (and others if applicable)
personp_df['eff_N'] = (personp_df['total'] * personp_df['effect_frac']).astype(int)
calc_T = personp_df['eff_N'].sum()
print('total effective shift takers:', calc_T)
total_RC_vols = personp_df['rc'].sum()
print('total RC shifts volunteered:', total_RC_vols)

# calculate features of shift schedule
start_date = dt.datetime(2026, 2, 23)
end_date = dt.datetime(2026, 8, 1)
print('run start date:', start_date)
print('run end date:  ',end_date)
n_days = (end_date - start_date).days + 1
n_leader = n_days * 3
n_worker = n_days * 3
n_RC = n_days
n_RC_weeks = n_days / 8
print('length of run: ', n_days, 'days')
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
personp_df['n_worker_alloc'] = n_worker * personp_df['eff_N'] / calc_T
personp_df['n_leader_alloc'] = (n_leader + n_RC) * personp_df['eff_N'] / calc_T - personp_df['rc']
personp_df['n_worker_avail'] = personp_df['n_worker_alloc']
personp_df['n_leader_avail'] = personp_df['n_leader_alloc']

print(personp_df)
print('total calculated leader shifts:', personp_df['n_leader_alloc'].sum())
print('total calculated worker shifts:', personp_df['n_worker_alloc'].sum())

# now generate shifts schedule
iter_date = start_date
date_list, text_date_list = [], []
leader_assign = {'owl': [], 'day': [], 'eve': []}
worker_assign = {'owl': ['JLAB', 'JLAB'], 'day': ['JLAB', 'JLAB'], 'eve': ['JLAB', 'JLAB']}

# create copies of person power df for leader and worker assignments
leader_alloc_df = personp_df.drop(columns=['n_worker_alloc', 'n_worker_avail']).copy()
worker_alloc_df = personp_df.drop(columns=['n_leader_alloc', 'n_leader_avail']).copy()

#for i in range(n_days):
while iter_date <= end_date:
    # print(i, iter_date.strftime('%Y-%m-%d'))
    for type in ['owl', 'day', 'eve']:
        # leaders first
        # remove insts with zero or fewer shifts remaining
        leader_alloc_df = leader_alloc_df[leader_alloc_df['n_leader_avail'] > 0]    
        total_avail = leader_alloc_df['n_leader_avail'].sum()
        # calculate the sampling weights based on number of available shifts remaining
        leader_alloc_df['sample_weights'] = leader_alloc_df['n_leader_avail'] / total_avail
        leader_select_inst = leader_alloc_df.sample(1, weights='sample_weights')['inst'].values[0]
        # append the institution to the df for four shifts
        [leader_assign[type].append(leader_select_inst) for _ in range(4)]
        # decrement the allocation of the chosen institution
        leader_alloc_df.loc[leader_alloc_df['inst'] == leader_select_inst, 'n_leader_avail'] -= 4 
        
        # workers next... same steps as above
        worker_alloc_df = worker_alloc_df[worker_alloc_df['n_worker_avail'] > 0]    
        total_avail = worker_alloc_df['n_worker_avail'].sum()
        worker_alloc_df['sample_weights'] = worker_alloc_df['n_worker_avail'] / total_avail
        worker_select_inst = worker_alloc_df.sample(1, weights='sample_weights')['inst'].values[0]
        [worker_assign[type].append(worker_select_inst) for _ in range(4)]
        worker_alloc_df.loc[worker_alloc_df['inst'] == worker_select_inst, 'n_worker_avail'] -= 4 
    
    # append dates in blocks of four
    for _ in range(4):
        date_list.append(iter_date.strftime('%Y-%m-%d'))
        text_date_list.append(iter_date.strftime('%-d-%b-%Y'))
        iter_date = iter_date + dt.timedelta(days=1)

# remove last block from worker and replace with two days assigned to JLAB
for type in ['owl', 'day', 'eve']:
    [worker_assign[type].pop() for _ in range(4)]
    [worker_assign[type].append('JLAB') for _ in range(2)]

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
print(leader_df)
print(worker_df)

# TO DO
# Write the mysql code to write these to the db
# How to run this on jlabl5?  Or can I run this locally and access the db from offsite?
