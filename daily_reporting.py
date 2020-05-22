# -*- coding: utf-8 -*-
"""
Created on Wed May 13 10:32:46 2020

@author: Harmony Liu
"""

import os
import numpy as np
import pandas as pd
import datetime

os.getcwd()
os.chdir('C:\\Users\\innov8ag\\Documents\\Walla2COVID19')
# Read in Data
dat = pd.read_excel('WW County COVID 19 Dashboard Inputs.xlsx', sheet_name = 'Sheet1')

# clean data
raw = clean_data(dat)

# specify date
start_date = datetime.date(2020, 3, 22)
end_date = datetime.date(2020, 5, 15)

# generate report
daily_summary_through_time(start_date, end_date, 'Burbank', raw)
daily_summary_through_time(start_date, end_date, 'Walla Walla', raw)
daily_summary_through_time(start_date, end_date, 'College Place', raw)
daily_summary_through_time(start_date, end_date, 'Wallula ', raw)
daily_summary_through_time(start_date, end_date, 'Prescott', raw)
daily_summary_through_time(start_date, end_date, 'Touchet', raw)
daily_summary_through_time(start_date, end_date, 'Walla Walla County', raw)










def clean_data(data):
    
    # Select columns of interest
    raw = dat[['WW Case #', 'test date', 'Date Recovered', 'Recovered','Sex', 'Age', 'City', 'Source of Infection']].copy()
    
    # Rename the columns
    raw.columns = ['WW Case #', 'Test date', 'Recovery/Death Date', 'Recovered','Sex', 'Age', 'City', 'Source of Infection']
    
    # change all the dates into datetime.date object for easier comparison 
    raw['Test date'] = [raw.loc[k, 'Test date'].date() for k in range(raw.shape[0])]
    raw['Recovery/Death Date'] = [raw.loc[k, 'Recovery/Death Date'].date() if raw.loc[k, 'Recovery/Death Date'] else None for k in range(raw.shape[0])]
    # =============================================================================
    #  Fill in missing dates for Recovery/Death date
    #  Protocols: Recovery date = Test Date + two weeks
    # =============================================================================
    # raw['Recovery/Death Date'] = [raw.loc[k, 'Test date']+datetime.timedelta(days=14) if pd.isnull(raw.loc[k, 'Recovery/Death Date']) and pd.notnull(raw.loc[k, 'Recovered']) else raw.loc[k, 'Recovery/Death Date'] for k in range(raw.shape[0])]
    
    # Categorized the age group
    raw['Age'] = raw['Age'].apply(age_placement)
    
    return raw
# Required Format from ArcGIS
def get_esri_format():
    formatted = pd.read_csv('CoronavirusCaseSource.csv')
    columns_with_data = ['name', 'reportdt', 'positive','casesmale', 'casesfemale', 'casesagerange1', 'casesagerange2', 'casesagerange3',
           'casesagerange4', 'casesagerange5', 'casesagerange6', 'casesagerange7',
           'casesagerange8', 'casesagerange9', 'casesagerange10', 'casesageother','sourceclosecontact', 'sourcepossibletravel',
           'sourceunderinvestigation', 'sourceTravel', 'sourcecommunity',
           'sourceother']
    num_columns = ['positive','casesmale', 'casesfemale', 'casesagerange1', 'casesagerange2', 'casesagerange3',
           'casesagerange4', 'casesagerange5', 'casesagerange6', 'casesagerange7',
           'casesagerange8', 'casesagerange9', 'casesagerange10', 'casesageother','sourceclosecontact', 'sourcepossibletravel',
           'sourceunderinvestigation', 'sourceTravel', 'sourcecommunity',
           'sourceother']
    return formatted


# Categorize age group
def age_placement(age):
    n = age//10
    age_group = ['0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79', '80+', 'N/A']

    if n >= 90:
        group = age_group[8]
    elif n == np.nan:
        group = age_group[9]
    else:    
        group = age_group[n]
    return group




def daily_summary_through_time(start_date, end_date, city_name, data):
    
    
    def daterange(date1, date2):
        for n in range(int ((date2 - date1).days)+1):
            yield date1 + datetime.timedelta(n)
        
        
    if city_name == 'Walla Walla County':
        dat = data.copy()
    else:
        dat = data[data['City'] == city_name].copy()   
    records = pd.DataFrame(columns = get_esri_format().columns)
    
    for k, today in enumerate(daterange(start_date, end_date)):
        record = daily_summary(today, dat, city_name)
        records = records.append(record)
        
    fname = city_name.replace(' ', '')+'.csv'
    records.to_csv('Reporting\\'+fname, index = False)
    
    return records

def daily_summary(today, raw_county, city_name):    
    # # Define date
    # today = datetime.date(2020, 5, 1)
    
    # Find total positive cases uptodate
    uptodate_total_records  = raw_county.loc[raw_county['Test date'] <= today, :]
    
    # Divide Total into Active and Inactive
    uptodate_active_records = uptodate_total_records[(uptodate_total_records['Recovery/Death Date']> today) | uptodate_total_records['Recovery/Death Date'].isnull()]
    uptodate_inactive_records =uptodate_total_records[uptodate_total_records['Recovery/Death Date']<= today]
                                                      
    # Divide Inactive into Died and Recovered
    uptodate_recovered_records = uptodate_inactive_records.loc[uptodate_inactive_records['Recovered']== 'Yes',:]
    uptodate_death_records = uptodate_inactive_records.loc[uptodate_inactive_records['Recovered']!= 'Yes',:]
    
    
    # Find total cases up to yesterday
    yesterday_total_records  = raw_county.loc[raw_county['Test date'] <= today - datetime.timedelta(days = 1), :]
    yesterday_inactive_records = yesterday_total_records[yesterday_total_records['Recovery/Death Date']<= today]
    yesterday_death_records = yesterday_inactive_records.loc[yesterday_inactive_records['Recovered']!= 'Yes',:]
    print(' ----- '+str(today)+ '    ' + city_name)
    print('Total diagnosed: %d, Recovered: %d, Deceased: %d, Active: %d'%(uptodate_total_records.shape[0], uptodate_recovered_records.shape[0], uptodate_death_records.shape[0], uptodate_active_records.shape[0]))
    k = 0
    daily_summary = pd.DataFrame(columns = get_esri_format().columns)
    daily_summary.loc[k, 'name'] = city_name
    daily_summary.loc[k, 'reportdt'] = today
    daily_summary.loc[k, 'positive'] = uptodate_total_records.shape[0]
    daily_summary.loc[k, 'deaths'] = uptodate_death_records.shape[0]
    daily_summary.loc[k, 'recovered'] = uptodate_recovered_records.shape[0]
    daily_summary.loc[k, 'active'] = uptodate_active_records.shape[0]
    daily_summary.loc[k, 'casesmale'] = uptodate_total_records[uptodate_total_records['Sex']=='M'].shape[0]
    daily_summary.loc[k, 'casesfemale'] = uptodate_total_records[uptodate_total_records['Sex']=='F'].shape[0]
    daily_summary.loc[k, 'casesagerange1'] = uptodate_total_records[uptodate_total_records['Age']=='0-9'].shape[0]
    daily_summary.loc[k, 'casesagerange2'] = uptodate_total_records[uptodate_total_records['Age']=='10-19'].shape[0]
    daily_summary.loc[k, 'casesagerange3'] = uptodate_total_records[uptodate_total_records['Age']=='20-29'].shape[0]
    daily_summary.loc[k, 'casesagerange4'] = uptodate_total_records[uptodate_total_records['Age']=='30-39'].shape[0]
    daily_summary.loc[k, 'casesagerange5'] = uptodate_total_records[uptodate_total_records['Age']=='40-49'].shape[0]
    daily_summary.loc[k, 'casesagerange6'] = uptodate_total_records[uptodate_total_records['Age']=='50-59'].shape[0]
    daily_summary.loc[k, 'casesagerange7'] = uptodate_total_records[uptodate_total_records['Age']=='60-69'].shape[0]
    daily_summary.loc[k, 'casesagerange8'] = uptodate_total_records[uptodate_total_records['Age']=='70-79'].shape[0] 
    daily_summary.loc[k, 'casesagerange9'] = uptodate_total_records[uptodate_total_records['Age']=='80+'].shape[0]
    # daily_summary.loc[k, 'casesagerange10'] = uptodate_total_records[uptodate_total_records['Age']=='0-9'].shape[0]
    daily_summary.loc[k, 'casesageother'] = uptodate_total_records[uptodate_total_records['Age'].isnull()].shape[0]
    
    
    daily_summary.loc[k, 'deathsmale'] = uptodate_death_records[uptodate_death_records['Sex']=='M'].shape[0]
    daily_summary.loc[k, 'deathsfemale'] = uptodate_death_records[uptodate_death_records['Sex']=='F'].shape[0]
    daily_summary.loc[k, 'deathsagerange1'] = uptodate_death_records[uptodate_death_records['Age']=='0-9'].shape[0]
    daily_summary.loc[k, 'deathsagerange2'] = uptodate_death_records[uptodate_death_records['Age']=='10-19'].shape[0]
    daily_summary.loc[k, 'deathsagerange3'] = uptodate_death_records[uptodate_death_records['Age']=='20-29'].shape[0]
    daily_summary.loc[k, 'deathsagerange4'] = uptodate_death_records[uptodate_death_records['Age']=='30-39'].shape[0]
    daily_summary.loc[k, 'deathsagerange5'] = uptodate_death_records[uptodate_death_records['Age']=='40-49'].shape[0]
    daily_summary.loc[k, 'deathsagerange6'] = uptodate_death_records[uptodate_death_records['Age']=='50-59'].shape[0]
    daily_summary.loc[k, 'deathsagerange7'] = uptodate_death_records[uptodate_death_records['Age']=='60-69'].shape[0]
    daily_summary.loc[k, 'deathsagerange8'] = uptodate_death_records[uptodate_death_records['Age']=='70-79'].shape[0] 
    daily_summary.loc[k, 'deathsagerange9'] = uptodate_death_records[uptodate_death_records['Age']=='80+'].shape[0]
    daily_summary.loc[k, 'deathsagerange10'] = uptodate_death_records[uptodate_death_records['Age']=='0-9'].shape[0]
    daily_summary.loc[k, 'deathsageother'] = uptodate_death_records[uptodate_death_records['Age'].isnull()].shape[0]
    
    daily_summary.loc[k, 'positiveincrease'] = daily_summary.loc[k, 'positive'] - yesterday_total_records.shape[0]
    daily_summary.loc[k, 'deathincrease'] = daily_summary.loc[k, 'deaths'] - yesterday_death_records.shape[0]
    
    
    
    daily_summary.loc[k, 'sourceclosecontact'] = uptodate_total_records[uptodate_total_records['Source of Infection']=='close contact'].shape[0]
    daily_summary.loc[k, 'sourceTravel'] = uptodate_total_records[uptodate_total_records['Source of Infection']=='travel'].shape[0]
    daily_summary.loc[k, 'sourcecommunity'] = uptodate_total_records[uptodate_total_records['Source of Infection']=='other'].shape[0]
    daily_summary.loc[k, 'sourceother'] = uptodate_total_records[uptodate_total_records['Source of Infection']=='community'].shape[0]

    return daily_summary










