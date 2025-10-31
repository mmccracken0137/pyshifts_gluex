import datetime as dt
import pandas as pd
from rich import print

def round_to_block(s, block_size=4):
    m = int(s / block_size)
    return int(m * block_size)
    
# load personpower data and calculate total effective shift takers

# treat inst data as dataframe:
power_df = pd.read_csv('personpower_2026run_TEST.csv')
power_df['total'] = power_df['experts'] + power_df['novices']
# line below applies JLAB's 0.5 weighting (and others if applicable)
power_df['eff_N'] = (power_df['total'] * power_df['effect_frac']).astype(int)
calc_T = power_df['eff_N'].sum()
print('total effective shift takers:', calc_T)
total_RC_vols = power_df['rc'].sum()
print('total RC shifts volunteered:', total_RC_vols)

# set start/end of run
start_date = dt.datetime(2026, 2, 23)
end_date = dt.datetime(2026, 8, 1)
# end_date = dt.datetime(2026, 2, 26)
print('run start date: ', start_date)
print('run end date:   ',end_date)

# make a dictionary of start dates/experiments for the acc_sched table
acc_sched_dates = {start_date.strftime('%Y-%m-%d'): 'TBD', '2026-05-01': 'TBD2'}

# calculate features of shift schedule
n_days = (end_date - start_date).days + 1
print('length of run: ', n_days, 'days')
if n_days / 4 != int(n_days / 4):
    print('ERROR: length of run not divisble by 4 days')
    exit()
    
n_expert = n_days * 3
n_novice = n_days * 3
n_RC = n_days
n_RC_weeks = n_days / 8
print('number of expert shifts: ', n_expert)
print('number of novice shifts: ', n_novice)
print('number of RC days:       ', n_RC)
print('number of RC weeks:      ', n_RC_weeks)
if n_RC > total_RC_vols:
    print('ERROR: not enough RCs!')
    exit()

# calculate number of expert and novice shifts for each institution
#novice shifts:  W*N/T 
#expert shifts:   (L+R)*N/T  minus the count of RC shifts that they provided 
power_df['n_novice_alloc'] = n_novice * power_df['eff_N'] / calc_T
power_df['n_expert_alloc'] = (n_expert + n_RC) * power_df['eff_N'] / calc_T - power_df['rc']
power_df['n_novice_avail'] = power_df['n_novice_alloc']
power_df['n_expert_avail'] = power_df['n_expert_alloc']

print(power_df)
print('total calculated expert shifts:', power_df['n_expert_alloc'].sum())
print('total calculated novice shifts:', power_df['n_novice_alloc'].sum())
summary_df = power_df.copy()

# now generate shifts schedule
iter_date = start_date
date_list, text_date_list = [], []
expert_assign = {'owl': [], 'day': [], 'eve': []}
novice_assign = {'owl': ['JLAB', 'JLAB'], 'day': ['JLAB', 'JLAB'], 'eve': ['JLAB', 'JLAB']}
acc_sched = {'exp': [], 'energy': [], 'acc_day': []}

# create copies of person power df for expert and novice assignments
expert_alloc_df = power_df.drop(columns=['n_novice_alloc', 'n_novice_avail']).copy()
novice_alloc_df = power_df.drop(columns=['n_expert_alloc', 'n_expert_avail']).copy()

#for i in range(n_days):
while iter_date <= end_date:
    # print(i, iter_date.strftime('%Y-%m-%d'))
    for t in ['owl', 'day', 'eve']:
        # experts first
        # remove insts with zero or fewer shifts remaining
        expert_alloc_df = expert_alloc_df[expert_alloc_df['n_expert_avail'] > 0]    
        total_avail = expert_alloc_df['n_expert_avail'].sum()
        # calculate the sampling weights based on number of available shifts remaining
        expert_alloc_df['sample_weights'] = expert_alloc_df['n_expert_avail'] / total_avail
        expert_select_inst = expert_alloc_df.sample(1, weights='sample_weights')['inst'].values[0]
        # append the institution to the df for four shifts
        [expert_assign[t].append(expert_select_inst) for _ in range(4)]
        # decrement the allocation of the chosen institution
        expert_alloc_df.loc[expert_alloc_df['inst'] == expert_select_inst, 'n_expert_avail'] -= 4 
        
        # novices next... same steps as above
        novice_alloc_df = novice_alloc_df[novice_alloc_df['n_novice_avail'] > 0]    
        total_avail = novice_alloc_df['n_novice_avail'].sum()
        novice_alloc_df['sample_weights'] = novice_alloc_df['n_novice_avail'] / total_avail
        novice_select_inst = novice_alloc_df.sample(1, weights='sample_weights')['inst'].values[0]
        [novice_assign[t].append(novice_select_inst) for _ in range(4)]
        novice_alloc_df.loc[novice_alloc_df['inst'] == novice_select_inst, 'n_novice_avail'] -= 4 
    
    # append dates in blocks of four
    for _ in range(4):
        date_list.append(iter_date.strftime('%Y-%m-%d'))
        text_date_list.append(iter_date.strftime('%-d-%b-%Y'))

        # add info to the acc_sched dictionary
        exp = ''
        for d, e in acc_sched_dates.items():
            if iter_date >= dt.datetime(int(d.split('-')[0]), 
                                        int(d.split('-')[1]), 
                                        int(d.split('-')[2])):
                exp = e
        acc_sched['exp'].append(exp)
        acc_sched['acc_day'].append(iter_date.strftime('%A'))
        acc_sched['energy'].append('TBD')

        # increment the date
        iter_date = iter_date + dt.timedelta(days=1)
        

# remove last block from novice and replace with two days assigned to JLAB
for t in ['owl', 'day', 'eve']:
    [novice_assign[t].pop() for _ in range(4)]
    [novice_assign[t].append('JLAB') for _ in range(2)]

print('Inspect these data frames to see whether any institutions have too many available remaining:')
print(expert_alloc_df)
print(novice_alloc_df)

expert_df = pd.DataFrame({'shiftdate': date_list, 'shift_date': text_date_list, 
                          'owl': expert_assign['owl'],
                          'day': expert_assign['day'],
                          'eve': expert_assign['eve']})
novice_df = pd.DataFrame({'shiftdate': date_list, 'shift_date': text_date_list, 
                          'owl': novice_assign['owl'],
                          'day': novice_assign['day'],
                          'eve': novice_assign['eve']})
acc_sched_df = pd.DataFrame({'shiftdate': date_list, 'acc_date': text_date_list, 
                             'exp': acc_sched['exp'],
                             'acc_day': acc_sched['acc_day'],
                             'energy': acc_sched['energy']})
# print(expert_df)
# print(novice_df)

# write tentative shift assignments to tsv files for the user to review
expert_df.to_csv('temp_expert_' + dt.datetime.today().strftime('%Y-%m-%d') + '.csv', index=False)
novice_df.to_csv('temp_novice_' + dt.datetime.today().strftime('%Y-%m-%d') + '.csv', index=False)
acc_sched_df.to_csv('temp_acc_sched_' + dt.datetime.today().strftime('%Y-%m-%d') + '.csv', index=False)

# now let's do some summarizing of the assignments. get a copy of earlier power df
summary = {}
for t in ['owl', 'day', 'eve']:
    summary['expert_' + t] = {}
    summary['novice_' + t] = {}
    for i in summary_df['inst']:
        summary['expert_' + t][i] = len(expert_df[expert_df[t] == i])
        summary['novice_' + t][i] = len(novice_df[novice_df[t] == i])
    summary_df['expert_' + t] = summary_df['inst'].map(summary['expert_' + t])
    summary_df['novice_' + t] = summary_df['inst'].map(summary['novice_' + t])

summary_df['total_expert'] = summary_df['expert_owl'] + summary_df['expert_day'] + summary_df['expert_eve']
summary_df['total_novice'] = summary_df['novice_owl'] + summary_df['novice_day'] + summary_df['novice_eve']

summary_df = summary_df.drop(columns=['effect_frac', 'n_novice_avail', 'n_expert_avail'])
summary_df.loc['totals'] = summary_df.sum(numeric_only=True)

summary_df.to_csv('summary_' + dt.datetime.today().strftime('%Y-%m-%d') + '.csv')    

print()
print('Tentative shift assignments have been written to file(s).')
print('Please review these before writing to the db.')

# TO DO
# Write the mysql code to write these to the db
# How to run this on jlabl5?  Or can I run this locally and access the db from offsite?
