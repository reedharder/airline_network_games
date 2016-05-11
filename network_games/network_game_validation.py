# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 10:44:26 2016

@author: d29905p
"""

'''
visualize and set up data for validation analysis of southewest network
(will call stage one pipeline)
'''

import pandas as pd

import os
import numpy as np
import pandas as pd
from itertools import product
import ast
import pickle
import matplotlib.pyplot as plt
bts_datadir = "C:/Users/d29905p/documents/airline_competition_paper/code/network_games/bts_data/"
fig_dir = "C:/Users/d29905p/documents/airline_competition_paper/aotp_temporal/"

major_carriers_2014 = ['DL','WN','UA','US','AA','B6','NK','AS','F9','VX']
major_carriers_west_2014 = ['WN','UA','US','AS','VX']
ope35 = ['ATL', 'BOS', 'BWI', 'CLE', 'CLT', 'CVG', 'DCA', 'DEN', 'DFW', 'DTW', 'EWR', 'FLL', 'IAD', 'IAH', 'JFK', 'LAS', 'LAX', 'LGA', 'MCO', 'MDW', 'MEM', 'MIA', 'MSP', 'ORD', 'PDX', 'PHL', 'PHX', 'PIT', 'SAN', 'SEA', 'SFO', 'SLC', 'STL', 'TPA']
western= ['SEA','PDX','SFO','SAN','LAX','LAS','PHX','OAK','ONT','SMF','SJC']

'''
date tools

'''
def create_date_index_dict(year=2007):
    
    common_year_days_month = {1 :31,
    2:	28,
    3:	31,
    4:	30,
    5:	31,
    6:	30,
    7:	31,
    8:	31,
    9:	30,
    10:	31,
    11:	30,
    12:	31}
    
    leap_year_days_month = {1 :31,
    2:	29,
    3:	31,
    4:	30,
    5:	31,
    6:	30,
    7:	31,
    8:	31,
    9:	30,
    10:	31,
    11:	30,
    12:	31}
    
    # is this a leap year?
    if year % 4 !=0:
        leap =False
    elif year % 100 !=0:
        leap=True
    elif year % 400 !=0:
        leap =False
    else: 
        leap = True
    # select day to month dict
    days_month_dict = leap_year_days_month if leap else common_year_days_month
    #create index
    date_index = {}
    i = 0 
    #func to zero pad dates
    def zeropad(datestr):
        if len(datestr) <2:
            datestr = "0" + datestr
        return datestr
    # fill date dict
    for month in range(1,13):    
        for day  in range(1,days_month_dict[month]+1):
            date_index['-'.join([str(year),zeropad(str(month)),zeropad(str(day))])] = i
            i += 1                
    return date_index




def AOTP_viz():
    year=2007
    marketset = western
    include_CX_DIV = True
    #rolling average lag in days
    smoothing = 9
    aotp = pd.read_csv(bts_datadir + "aotp%s.csv" % year, usecols = ['YEAR','QUARTER','MONTH','DAY_OF_MONTH','FLIGHT_DATE','ORIGIN','DESTINATION','UNIQUE_CARRIER','CANCELLED','DIVERTED','NUMBER_FLIGHTS','TAIL_NUMBER' ])    
    #select relevant markets
    aotp = aotp[aotp['ORIGIN'].isin(marketset) & aotp['DESTINATION'].isin(marketset)]
    aotp_perday = aotp[['YEAR','QUARTER','MONTH','DAY_OF_MONTH','FLIGHT_DATE','ORIGIN','DESTINATION','UNIQUE_CARRIER','NUMBER_FLIGHTS']].groupby(['YEAR','QUARTER','MONTH','DAY_OF_MONTH','FLIGHT_DATE','ORIGIN','DESTINATION','UNIQUE_CARRIER']).aggregate(np.sum).reset_index()
    #create date index for this year    
    date_index = create_date_index_dict(year)
    #create reverse map
    reverse_date_index = {value:key for key, value in date_index.items()}
    #add date index for dataframe
    aotp_perday['DATE_INDEX'] =aotp_perday.apply(lambda row: date_index[row['FLIGHT_DATE']],1)
    #function to create zero flight padded full year index from numpy arrays of flights and dates
    num_days = len(date_index)
    def yearly_flight_count(DATE_INDEX,NUM_FLIGHTS,num_days):
        num_flights_padded = np.zeros(num_days)
        num_flights_padded[DATE_INDEX]=NUM_FLIGHTS
        return num_flights_padded
    #reshape matrix by collapasing days into vector
    market_dynamics_rows = []
    aotp_perday_gb = aotp_perday[['ORIGIN','DESTINATION','UNIQUE_CARRIER','NUMBER_FLIGHTS','DATE_INDEX']].groupby(['ORIGIN','DESTINATION','UNIQUE_CARRIER'])
    for group in aotp_perday_gb.groups:
        DATE_INDEX = aotp_perday_gb.get_group(group)['DATE_INDEX']
        NUM_FLIGHTS = aotp_perday_gb.get_group(group)['NUMBER_FLIGHTS']
        flights_vec = yearly_flight_count(DATE_INDEX,NUM_FLIGHTS,num_days)
        market_dynamics_rows.append({'UNIQUE_CARRIER': group[2],'ORIGIN':group[0],'DESTINATION':group[1],'FLIGHTS_VEC':flights_vec})
    market_dynamics_df = pd.DataFrame(market_dynamics_rows)
    #add rolling avg of flights
    rolling = []
    for vec in market_dynamics_df['FLIGHTS_VEC'].tolist():        
        rolling.append(pd.rolling_mean(vec,smoothing) )
        
    market_dynamics_df['ROLLING'] =rolling
    market_dynamics_df['ROLLING_MAX']=market_dynamics_df.apply(lambda row: np.nanmax(row['ROLLING']),1)
    
    market_dynamics_groups = market_dynamics_df.groupby(['ORIGIN','DESTINATION'])
    #plots for unidrectional markets (check discripency, eliminate max <1?)
    #create date index and month labels
    xvec =np.array(range(0,num_days))
    month_labs=[]
    lab_locs = []
    month_prev = 'NULL'
    loc= 0
    for date_ind in xvec.tolist():
        month = reverse_date_index[date_ind].split('-')[1]
        if month!=month_prev:
            month_labs.append(month)
            lab_locs.append(loc)
        #else:
           # month_labs.append('')
        month_prev=month      
        loc += 1
    
    for group in market_dynamics_groups.groups:
        market = market_dynamics_groups.get_group(group)
        plt.subplot(2,1,1)
        
        plt.ylabel('Flights per Day')
             
        plt.xticks(lab_locs, month_labs,rotation=45) 
        for carrier in market['UNIQUE_CARRIER'].tolist():
            row = market[market['UNIQUE_CARRIER']==carrier]
            plt.plot(row['FLIGHTS_VEC'].item(), label=carrier)
        plt.xlabel('Month')
       
        plt.title('Frequency Competition in %s Market, %s' % (group[0]+'_'+group[1], year))
        plt.subplot(2,1,2)
        plt.ylabel('%s day rolling avg' % smoothing)
        plt.xlabel('Month')
        plt.xticks(lab_locs, month_labs,rotation=45) 
        for carrier in market['UNIQUE_CARRIER'].tolist():
            row = market[market['UNIQUE_CARRIER']==carrier]
            plt.plot(np.nan_to_num(row['ROLLING'].item()), label=carrier) 
            plt.legend(shadow=True, loc=3,fancybox=True)   
        plt.savefig(fig_dir+'aotp_fig_%s_%s.jpg' % ('-'.join(sorted([group[0],group[1]]))+'_'+group[0]+'_'+group[1],year))
        plt.clf()
        
    
    
