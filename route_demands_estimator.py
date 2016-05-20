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
import gc
import sys
import logging
import traceback


#function proportion of sampled markets with less than or equal to given number of connecting flights  
def connecting_flight_diagnostic(connections_max=2,  db1b_file='DB1B_MARKET_2014_Q1.csv',data_dir = 'C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'):
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
     


    

def create_route_demands_quarter(year =2014, quarter=1, filter_null_fares=False, filter_fare_bounds = [None, None], base_coup_fn= 'DB1B_COUPON_{year}_Q{quarter}.csv', base_mkt_fn = 'DB1B_MARKET_{year}_Q{quarter}.csv',data_dir = 'C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/', outfile = 'route_demand_{year}_Q{quarter}.csv'):
    
    #year =2014
    #quarter=1
    #filter_null_fares=False
    #filter_fare_bounds = [None, None]
    #base_coup_fn= 'DB1B_COUPON_{year}_Q{quarter}.csv'
    #base_mkt_fn = 'DB1B_MARKET_{year}_Q{quarter}.csv'
    outfile  = outfile.format(year=year, quarter=quarter)
   # data_dir = 'C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'
    #load data from DB1B markets and coupons into dataframes  
    print('loading data...')
    mkt_df = pd.read_csv(data_dir + base_mkt_fn.format(year=year, quarter=quarter),usecols=['MKT_ID','YEAR','QUARTER','MARKET_COUPONS','MARKET_FARE','PASSENGERS','BULK_FARE'])    
    coup_df = pd.read_csv(data_dir + base_coup_fn.format(year=year, quarter=quarter),usecols=['MKT_ID','SEQ_NUM','ORIGIN','DEST','OPERATING_CARRIER'])
    merge_df = coup_df.merge(mkt_df,'left',on='MKT_ID')
    del mkt_df
    del coup_df
    merge_df=merge_df.sort(columns=['MKT_ID','SEQ_NUM'])
    t0=time.time()
    ##merge_df = merge_df.groupby('MKT_ID').aggregate()
    merge_df =merge_df[merge_df['MARKET_COUPONS']<=2] 
    t1=time.time()-t0
    print(t1)
    print('filtering data...')
    
    def num_flights(x):
        if x['MARKET_COUPONS'].iloc[0] == 1:
            return '1'
        else:
            return '2'
            
    def origin(x):
        if x['MARKET_COUPONS'].iloc[0] == 1:
            return x['ORIGIN'].iloc[0]
        else:           
            return x.iloc[0]['ORIGIN']
            
    def dest(x):
        if x['MARKET_COUPONS'].iloc[0] == 1:
            return x['DEST'].iloc[0]
        else:            
            return x.iloc[1]['DEST']
    
    def connection(x):
        if x['MARKET_COUPONS'].iloc[0] == 1:
            return ''
        else:            
            return x.iloc[0]['DEST']
            
    def first_op(x):
        if x['MARKET_COUPONS'].iloc[0] == 1:
            return x['OPERATING_CARRIER'].iloc[0]
        else:           
            return x.iloc[0]['OPERATING_CARRIER']
             
    def second_op(x):
        if x['MARKET_COUPONS'].iloc[0] == 1:
            return ''
        else:           
            return x.iloc[1]['OPERATING_CARRIER']
    
    leg_categories = ['MKT_ID', 'ORIGIN','DEST','MARKET_COUPONS','OPERATING_CARRIER','YEAR','QUARTER','MARKET_FARE','PASSENGERS','BULK_FARE']
    
   
    
    merge_df = merge_df[leg_categories].groupby(['MKT_ID'])
    
    t0=time.time()
    with open(data_dir + "leg_file_" + outfile,'a') as outfile:
        outfile.write(",".join(['YEAR','QUARTER','NUM_FLIGHTS','PASSENGERS','ORIGIN','CONNECTION','DESTINATION','FIRST_OPERATING_CARRIER','SECOND_OPERATING_CARRIER','MARKET_FARE','BULK_FARE']) +"\n")
        for i, (g, x) in enumerate(merge_df):
            if i % 10000 == 0:
                print("checkpoint %s, %s sec" % (i,time.time()-t0))     
                gc.collect()
            new_line= [str(x['YEAR'].iloc[0]),str(x['QUARTER'].iloc[0]), num_flights(x),str(10*x['PASSENGERS'].iloc[0]),\
            origin(x), connection(x),dest(x), first_op(x),
            second_op(x),str(x['MARKET_FARE'].iloc[0]),str(x['BULK_FARE'].iloc[0])]
        
            try: 
                outfile.write(",".join(new_line) +'\n')
            except:
                #remove any nan
                try: 
                    new_line = [i if type(i)==str else '' for i in new_line]
                    outfile.write(",".join(new_line) +'\n')
                except:
                    print(new_line)
                    logging.exception('Got exception on main handler')
                    raise
                    
    #REVISE LEG FILE, NEEDS TO BE DELLED OR NAME CHANGED AFTER USE, make this a user option. THEN RESCUE LEG FILE FOR 2007, BETTER PARAMETERIZE THIS FUNCTION
    try:
        merge_df = pd.read_csv(data_dir + "leg_file_" + outfile)
        try:
            merge_df.drop('BULK_FARE',1)    
        except ValueError:
            pass 
        merge_df[['YEAR','QUARTER', 'NUM_FLIGHTS']] = merge_df[['YEAR','QUARTER', 'NUM_FLIGHTS']].astype(int) 
        merge_df[['MARKET_FARE','PASSENGERS']] =  merge_df[['MARKET_FARE','PASSENGERS']].astype(float)        
        def wavg(group):
            d = group['MARKET_FARE']
            w = group['PASSENGERS']
            return pd.DataFrame([{'PASSENGERS': w.sum(), 'MARKET_FARE': (d * w).sum() / w.sum()}])
        merge_df = merge_df.fillna('NotANumber')
        merge_df = merge_df.groupby(['YEAR','QUARTER','NUM_FLIGHTS','ORIGIN','CONNECTION','DESTINATION','FIRST_OPERATING_CARRIER','SECOND_OPERATING_CARRIER']).apply(wavg).reset_index()
        #FOR NOW...
        merge_df = merge_df.drop('level_8',1)
        ##merge_df['PASSENGERS']=merge_df['PASSENGERS']*10 
        merge_df = merge_df.replace('NotANumber','')
        merge_df.to_csv(data_dir + outfile)
    except:
        with open(data_dir + "errorlog_route_demands",'a') as logfile:
            logfile.write(str(sys.exc_info()[0])+'\n')
            logfile.write(str(sys.exc_info()[1])+'\n')
            traceback.print_tb(sys.last_traceback, file=open(data_dir + "errorlog_route_demands",'a'))
            
        return merge_df
   
    return merge_df
    
##CLEAN UP THIS FILE, REMOVE FIRST FUCTION, FIGURE OUT WHY MERGING FAILS
def run():
    data_dir = 'C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'
    LOG_FILENAME = data_dir + "errorlog_route_demands.txt"
    logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
    logging.debug('This message should go to the log file')
    for year in [2011]:
        for quarter in [1,2,3,4]:    
           m=create_route_demands_quarter(year=year,quarter=quarter)
    
    #WHY DOES THIS ONLY WORK OUTSIDE FUNCTION?
    for year in [2012]:
        for quarter in [1]:
            t0=time.time()
            data_dir = 'C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'
            outfile = 'route_demand_{year}_Q{quarter}.csv'
            outfile  = outfile.format(year=year, quarter=quarter)
            merge_df = pd.read_csv(data_dir + "leg_file_" + outfile)# dtype={'YEAR':int,'QUARTER':int,'PASSENGERS':int,'NUM_FLIGHTS':int, 'MARKET_FARE':float})
            try:
                merge_df.drop('BULK_FARE',1)    
            except ValueError:
                pass
            merge_df[['YEAR','QUARTER', 'NUM_FLIGHTS']] = merge_df[['YEAR','QUARTER', 'NUM_FLIGHTS']].astype(int) 
            merge_df[['MARKET_FARE','PASSENGERS']] =  merge_df[['MARKET_FARE','PASSENGERS']].astype(float)
            
            def wavg(group):
                    d = group['MARKET_FARE']
                    w = group['PASSENGERS']
                    return pd.DataFrame([{'PASSENGERS': w.sum(), 'MARKET_FARE': (d * w).sum() / w.sum()}])
            merge_df = merge_df.fillna('NotANumber')
            merge_df = merge_df.groupby(['YEAR','QUARTER','NUM_FLIGHTS','ORIGIN','CONNECTION','DESTINATION','FIRST_OPERATING_CARRIER','SECOND_OPERATING_CARRIER']).apply(wavg).reset_index()
            #FOR NOW...
            merge_df = merge_df.drop('level_8',1)
            ##merge_df['PASSENGERS']=merge_df['PASSENGERS']*10 
            merge_df = merge_df.replace('NotANumber','')
            merge_df.to_csv(data_dir + outfile)
            ###m = create_route_demands_quarter2(year =2014, quarter=q)
            print("checkpoint %s sec" % (time.time()-t0)) 
    quarter =1
    year =2014
    data_dir = 'C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'
    outfile = 'route_demand_{year}_Q{quarter}.csv'
    outfile  = outfile.format(year=year, quarter=quarter)
    merge_df1 = pd.read_csv(data_dir + outfile)
    quarter = 2
    outfile = 'route_demand_{year}_Q{quarter}.csv'
    outfile  = outfile.format(year=year, quarter=quarter)
    merge_df2 = pd.read_csv(data_dir + outfile)
    quarter = 3
    outfile = 'route_demand_{year}_Q{quarter}.csv'
    outfile  = outfile.format(year=year, quarter=quarter)
    merge_df3 = pd.read_csv(data_dir + outfile)
    quarter = 4
    outfile = 'route_demand_{year}_Q{quarter}.csv'
    outfile  = outfile.format(year=year, quarter=quarter)
    merge_df4 = pd.read_csv(data_dir + outfile)
    merge_df = pd.concat([merge_df1,merge_df2,merge_df3, merge_df4])
    
    
    
    
    
    rd2007  =pd.read_csv(data_dir + 'route_demand_2007_Q1.csv').drop(['Unnamed: 0','MARKET_FARE'],axis=1)
    #rd2007['PASSENGERS'] = rd2007['PASSENGERS']*10
    rd2007old  =pd.read_csv(data_dir + 'route_demand_2007_Q1_old.csv').sort(columns=['ORIGIN','CONNECTION','DESTINATION','FIRST_OPERATING_CARRIER','SECOND_OPERATING_CARRIER'])
    df = pd.concat([rd2007, rd2007old])
    df = df.reset_index(drop=True)
    df_gpby = df.groupby(list(df.columns))
    idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
    df =df.reindex(idx)
    
    #write a concatenated list of quarters
    dflist = []
    for q in range(1,5):
        file = pd.read_csv(data_dir + 'route_demand_2014_Q%s.csv' % q)
        file = file.fillna(value='--')
        file = file.drop('Unnamed: 0',axis=1)
        dflist.append(file) 
    df = pd.concat(dflist)
    df.MARKET_FARE = df.MARKET_FARE.round(decimals=2)
    df.PASSENGERS = df.PASSENGERS.astype(int)
    df['AVG_MARKET_FARE'] = df.MARKET_FARE
    df = df.drop('MARKET_FARE',axis=1)
    df.to_csv(data_dir + 'route_demand_2014.csv',index=False)