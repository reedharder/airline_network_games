# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 14:02:00 2015

@author: d29905p
"""

'''
functions to construct route demands and pax delay tables for arbitrary quarters and years
'''

'''
fields needed:
YEAR, QUARTER, NUM_FLIGHTS, ORIGIN, CONNECTION, DESTINATION, FIRST_OPERATING_CARRIER, SECOND_OPERATING_CARRIER, PASSENGERS

'''

import numpy as np 
import pandas as pd
import time

#function proportion of sampled markets with less than or equal to given number of connecting flights  
def connecting_flight_diagnostic(connections_max=2,  db1b_file='DB1B_MARKET_2007_Q1.csv',data_dir = 'C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'):
     db1bmkts = pd.read_csv(data_dir + db1b_file)     
     #count coupon numbers throughout database
     coup_counts=pd.DataFrame(db1bmkts['MARKET_COUPONS'].value_counts())
     coup_counts['mkt_count']=coup_counts[0]
     coup_counts['index']=coup_counts.index
     coup_counts=coup_counts.sort(columns=['index'])
     #sum total samples and samples with number of connections>= connctions max
     legs_sum = coup_counts.mkt_count.sum()
     legs_under_n_sum = coup_counts[:connections_max].mkt_count.sum()
     #return proportion
     return  legs_under_n_sum/legs_sum
     

def create_route_demands_quarter(year =2007, quarter=1, filter_null_fares=False, filter_fare_bounds = [None, None], base_coup_fn= 'DB1B_COUPON_{year}_Q{quarter}.csv', base_mkt_fn = 'DB1B_MARKET_{year}_Q{quarter}.csv',data_dir = 'C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'):
    #load data from DB1B markets and coupons into dataframes  
    print('loading data...')
    mkt_df = pd.read_csv(data_dir + base_mkt_fn.format(year=year, quarter=quarter),usecols=['MKT_ID','YEAR','QUARTER','MARKET_COUPONS','ORIGIN','DEST','OPERATING_CARRIER','MARKET_FARE','PASSENGERS'])    
    coup_df = pd.read_csv(data_dir + base_coup_fn.format(year=year, quarter=quarter),usecols=['MKT_ID','SEQ_NUM','ORIGIN','DEST','OPERATING_CARRIER'])
    merge_df = coup_df.merge(mkt_df,'left',on='MKT_ID')
    del mkt_df
    del coup_df
    merge_df=merge_df.sort(columns=['MKT_ID','SEQ_NUM'])
    t0=time.time()
    merge_df = merge_df.groupby('MKT_ID').aggregate()
    t1=time.time()-t0
    print(t1)
    print('filtering data...')
    #group coupon table by mkt id
    coup_df = coup_df.groupby('MKT_ID')
    #filter out markets with more than one connection
    mkt_df =mkt_df[mkt_df['MARKET_COUPONS']<=2]    
    #optional price filtering: null/0 prices 
    if filter_null_fares:
         mkt_df =mkt_df[mkt_df['MARKET_FARE'].notnull() and mkt_df['MARKET_FARE'] > 0.0 ]      
    #optional price filering: bounds
    if filter_fare_bounds[0]: #lower bound
         mkt_df =mkt_df[mkt_df['MARKET_FARE']>=filter_fare_bounds[0]]   
    if filter_fare_bounds[1]: #upper bound
         mkt_df =mkt_df[mkt_df['MARKET_FARE']<=filter_fare_bounds[1]]       
    mkt_df = mkt_df.reset_index()
    mkt_df['index'] =mkt_df.index
    #function for pulling relevant market data from coupon df mkt id groups
    #for each row of mkt 
    print('merging data...')
    new_rows=[]
    t0 = time.time()
    #TRY WRITING THIS AS A MERGE FUNCTION FIRST, THEN SORT, THEN CUSTOM AGGREGATE FUNCTION
    def find_route(row,coup_df):
        '''
        if int(row['index'])%10000==0:
            print( row['index'])
            '''
        #if flight market is nonstop, simply use data DB1B markets for route demands
        if row['MARKET_COUPONS'] ==1:
            return pd.Series({'NUM_FLIGHTS':1,'ORIGIN':row['ORIGIN'],'CONNECTION':'' ,'DESTINATION':row['DEST'],'FIRST_OPERATING_CARRIER':row['OPERATING_CARRIER'],'SECOND_OPERATING_CARRIER':''})
        else: #otherwise, construct connection 
            #get market from coupons table 
            mkt_coupons = coup_df.get_group(row['MKT_ID'])
            #order coupons by sequence number
            mkt_coupons = mkt_coupons.sort(columns=['SEQ_NUM'])
            #get route data
            origin = mkt_coupons.iloc[0]['ORIGIN']
            connection = mkt_coupons.iloc[0]['DEST']
            destination = mkt_coupons.iloc[1]['DEST']
            first_op = mkt_coupons.iloc[0]['OPERATING_CARRIER']
            second_op =mkt_coupons.iloc[1]['OPERATING_CARRIER']
            return pd.Series({'NUM_FLIGHTS':2,'ORIGIN':origin,'CONNECTION':connection, 'DESTINATION':destination, 'FIRST_OPERATING_CARRIER':first_op,'SECOND_OPERATING_CARRIER':second_op})
    #apply above function to each market id, to create columns listed below
    for i in range(0,mkt_df.shape[0]):
        if i%10000==0:
            print(i)
        new_rows.append(find_route(mkt_df.iloc[i],coup_df))
        
    
    mkt_df[['NUM_FLIGHTS','ORIGIN','CONNECTION','DESTINATION','FIRST_OPERATING_CARRIER','SECOND_OPERATING_CARRIER']] = mkt_df.apply(find_route,axis=1,coup_df=coup_df)
    #remove coupons from memory    
    del coup_df    
    print(t0-time.time())
    #aggregate same routes by same carriers     
    print('aggregating data...')    
    mkt_df  =  mkt_df['ORIGIN','CONNECTION','DESTINATION','FIRST_OPERATING_CARRIER','SECOND_OPERATING_CARRIER','YEAR','QUARTER','NUM_FLIGHTS','PASSENGERS','MARKET_FARE'].groupby(['ORIGIN','CONNECTION','DESTINATION','FIRST_OPERATING_CARRIER','SECOND_OPERATING_CARRIER']).aggregate({'YEAR': lambda x: int(x['YEAR'].iloc[0]),'QUARTER': lambda x: int(x['QUARTER'].iloc[0]),'NUM_FLIGHTS': lambda x: int(x['NUM_FLIGHTS'].iloc[0]),'PASSENGERS': lambda x: np.sum(x['PASSENGERS'])*10,'MARKET_FARE': lambda x: np.average(x['MARKET_FARE'], weights=x['PASSENGERS'])}).reset_index()     
    return mkt_df