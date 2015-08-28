# -*- coding: utf-8 -*-
"""
Created on Sun May 17 23:07:54 2015

@author: Reed
"""


import os
import numpy as np
import pandas as pd
from itertools import product
import ast
os.chdir("C:/Users/d29905p/documents/airline_competition_paper/code/network_games")
#LOAD NEW KIND OF T100 file 

'''
STEP ONE: CREATE NETWORK PROFILE TABLE, MAJOR CARRIERS IN MAJOR MARKETS IN SELECT AIRPORTS

'''
'''
function creates file consisting of nonstop market/carrier combinations for relevant markets and carriers, including empirical costs, frequencies and prices
qaurters is a list of yearly quarters to include in the analysis
t100_fn is T100 segments file from BTS
p52_fn is form P.52 file from BTS
ac_type_fn is aircraft type file from BTS
b43_fn is Schedule B4 file from BTS
airports is a list of airports in the network
freq_cuttoff is minimum daily frequency to consider
ms_cuttoff is cumulative market share to consider in each market
merge_HP: True if carrier HP is to be merged with UA
only_big_carriers: if True, only carriers in list carriers_of_interest will be counted in constructing market type and market size, otherwise any that meet the frequency and marketshare cuttoffs will be included
carriers_of_interest: list of carriers to be counted in categorizing a market size
'''
def nonstop_market_profile(output_file = "processed_data/nonstop_competitive_markets_reg1.csv",aotp_fn = 'bts_data/aotp_march.csv',directory="C:/Users/Reed/Desktop/vaze_competition_paper", quarters=[1,2,3,4], \
    t100_fn="bts_data/T100_2007.csv",p52_fn="bts_data/P52_2007.csv", t100_avgd_fn="processed_data/t100_avgd_reg1.csv", merge_HP=True, \
    t100_summed_fn = 'processed_data/t100_summed_reg1.csv', t100_craft_avg_fn='processed_data/t100_craft_avg_reg1.csv',\
    ignore_mkts = ['PDX_SJC','PDX_SFO','OAK_PDX'],\
    freq_cuttoff = .5, ms_cuttoff=.1, only_big_carriers=False, carriers_of_interest = ['AS','UA','US','WN'],airports = ['SEA','PDX','SFO','SAN','LAX','LAS','PHX','OAK','ONT','SMF','SJC']):
        
    #read in revelant bts files and supplementary data files 
    
    ##os.chdir(directory)
    t100 = pd.read_csv(t100_fn)
    p52 = pd.read_csv(p52_fn)

    #create bidrectional market pairs
    pairs =[sorted([pair[0],pair[1]]) for pair in product(airports,airports) if pair[0]!=pair[1] ]
    txtpairs = list(set(["_".join(pair) for pair in pairs]))
    txtpairs = [pair for pair in txtpairs if pair not in ignore_mkts]
    
    #leave out fare finding for now, may add later
    #get relevant segments within network for all market pairs
    t100 = t100[t100['QUARTER'].isin(quarters)]
    print("creating markets...")
    t100['BI_MARKET']=t100.apply(create_market,1) #first, create bidriectional market indicator   
    print("done")
    relevant_t100= t100.set_index('BI_MARKET').loc[txtpairs].reset_index() #then, select markets
    #merge carrier HP under UA if this is called for.
    if merge_HP:
        relevant_t100['UNIQUE_CARRIER']=relevant_t100['UNIQUE_CARRIER'].replace('HP','US')

    #get relevant data from schedule P-5.2
    relevant_p52_d = p52[p52['REGION']=='D']
    relevant_p52=relevant_p52_d[relevant_p52_d['QUARTER'].isin(quarters)]
    
    #get aotp to get flight times
    aotp_mar = pd.read_csv(aotp_fn)
    aotp_mar['BI_MARKET']=aotp_mar.apply(create_market,1) 
    #DISSAGREGGATE BY AIRCRAFT TYPE LATER, NOTE: POSSIBILITY OF CANCELED ZERO HOUR FLIGHTS?
    aotp_mar_times = aotp_mar[['UNIQUE_CARRIER','BI_MARKET','AIR_TIME']].groupby(['UNIQUE_CARRIER','BI_MARKET']).aggregate(lambda x: np.mean(x)/60)
    aotp_mar_times = aotp_mar_times.reset_index().groupby(['UNIQUE_CARRIER','BI_MARKET'])    
    
    #average quarterly costs if necessary 
    if len(quarters) > 1:
        expenses_by_type=relevant_p52[['AIRCRAFT_TYPE','UNIQUE_CARRIER','TOT_AIR_OP_EXPENSES', 'TOTAL_AIR_HOURS']].groupby(['AIRCRAFT_TYPE','UNIQUE_CARRIER']).aggregate(np.sum).reset_index().dropna()
    else:
        expenses_by_type = relevant_p52.dropna()   
    #calculate expenses per air hour for each type for each airline
    expenses_by_type['EXP_PER_HOUR'] = expenses_by_type['TOT_AIR_OP_EXPENSES'] / expenses_by_type['TOTAL_AIR_HOURS']

    #average relevant monthly frequencie to get daily freqencies
    t100fields =['BI_MARKET','UNIQUE_CARRIER','ORIGIN', 'DEST','AIRCRAFT_TYPE','DEPARTURES_SCHEDULED','DEPARTURES_PERFORMED','SEATS','PASSENGERS','DISTANCE','AIR_TIME']
    #daily departures, seats, passengers, avg distance, total airtime
    t100_summed = relevant_t100[t100fields].groupby(['UNIQUE_CARRIER','BI_MARKET','ORIGIN','DEST','AIRCRAFT_TYPE']).aggregate({'DEPARTURES_SCHEDULED':lambda x: np.sum(x),'DEPARTURES_PERFORMED':lambda x: np.sum(x),'SEATS':lambda x: np.sum(x)/(365/(4/len(quarters))),'PASSENGERS':lambda x: np.sum(x)/(365/(4/len(quarters))),'DISTANCE':np.mean,'AIR_TIME':lambda x: np.sum(x)}).reset_index()
    if only_big_carriers:
        t100_summed=t100_summed[t100_summed['UNIQUE_CARRIER'].isin(carriers_of_interest)]        
    #convert airtime to hours
    t100_summed['AIR_HOURS']=(t100_summed['AIR_TIME']/60)
    t100_summed['FLIGHT_TIME']=t100_summed['AIR_HOURS']/t100_summed['DEPARTURES_PERFORMED']
    
    t100_summed['DAILY_FREQ']=t100_summed['DEPARTURES_SCHEDULED']/(365/(4/len(quarters)))
    t100_summed = t100_summed.drop('AIR_TIME',axis=1)
    #average values between segments sharing a bidirectional market 
    t100fields =['BI_MARKET','UNIQUE_CARRIER','AIRCRAFT_TYPE','DEPARTURES_SCHEDULED','SEATS','PASSENGERS','DISTANCE','AIR_HOURS', 'DAILY_FREQ']
    #merge t100 data with cost data
    t100_summed=pd.merge(t100_summed,expenses_by_type,on=['AIRCRAFT_TYPE','UNIQUE_CARRIER'])
    #NOTE, CHECL FLIGHT COSTS HERE, PERHAPS DO DEPARTURES PERFORMED
    t100_summed['FLIGHT_COST'] = t100_summed['AIR_HOURS']*t100_summed['EXP_PER_HOUR']/t100_summed['DEPARTURES_PERFORMED'] #get cost per flight type
    #NOTE: FOR THIS HOURS AND FRACTION FOF TOTAL HOURS FOR FLIGHT FOR CARRIER MUST BE ADDED , I THINK IT CAN BE DONE HERE 
    t100_summed = t100_summed[t100_summed['PASSENGERS']>0]
    t100_summed = t100_summed[t100_summed['DEPARTURES_SCHEDULED']>0]    
    t100_summed.to_csv(t100_summed_fn) # NOTE: SEE DISTRIBUTION WITHIN MARKETS, IS AVERAGING REASONABLE -> weight by passengers: before averaging markets: plane types back and forth might not be same , passengers more likely to correlate, but now we can test
    #average flight cost between different types  
    t100fields =['BI_MARKET','ORIGIN','DEST','UNIQUE_CARRIER','AIRCRAFT_TYPE','DEPARTURES_SCHEDULED','SEATS','PASSENGERS','DISTANCE', 'DAILY_FREQ','FLIGHT_COST','FLIGHT_TIME','AIR_HOURS']
    t100_summed_avgs = t100_summed[t100fields].groupby(['UNIQUE_CARRIER','BI_MARKET']).apply(avg_costs)
    t100_craft_avg = t100_summed_avgs[t100fields].groupby(['UNIQUE_CARRIER','BI_MARKET','ORIGIN','DEST']).aggregate({'DEPARTURES_SCHEDULED':np.sum,'SEATS':np.sum,'PASSENGERS':np.sum,'DISTANCE':np.mean, 'DAILY_FREQ':np.sum,'FLIGHT_COST':np.mean,'FLIGHT_TIME':np.mean,'AIR_HOURS':np.sum}).reset_index()
    #textfile of t100 summed over months, to check passenger equivalence between market directions
    t100_craft_avg.to_csv(t100_craft_avg_fn)
    #average values between segments sharing a bidirectional market 
    t100fields =['BI_MARKET','UNIQUE_CARRIER','DEPARTURES_SCHEDULED','SEATS','PASSENGERS','DISTANCE', 'DAILY_FREQ','FLIGHT_COST','FLIGHT_TIME','AIR_HOURS']
    t100_avgd = t100_craft_avg[t100fields].groupby(['UNIQUE_CARRIER','BI_MARKET']).aggregate({'DEPARTURES_SCHEDULED':np.mean,'DAILY_FREQ':np.mean,'SEATS':np.mean,'PASSENGERS':np.mean,'DISTANCE':np.mean,'FLIGHT_COST': np.mean,'FLIGHT_TIME':np.mean,'AIR_HOURS':np.mean}).reset_index()
    #save data frame to csv: costs and frequencies by market, carrier, aircraft type
    t100_avgd.to_csv(t100_avgd_fn,sep="\t")  
    #remove entries below daily frequency cuttoff
    t100_avgd_clip = t100_avgd[t100_avgd['DAILY_FREQ']>=freq_cuttoff]
    #group and rank carriers within markets
    t100_grouped = t100_avgd_clip.groupby('BI_MARKET')
    grouplist = []
    for market in list(set(t100_avgd_clip['BI_MARKET'].tolist())):
        market_group = t100_grouped.get_group(market)
        new_group = market_rank(market_group, ms_cuttoff=ms_cuttoff)
        grouplist.append(new_group)
    t100ranked = pd.concat(grouplist,axis=0)
    t100ranked=t100ranked.sort(columns=['BI_MARKET','MARKET_RANK'])
    #Add aotp based average flight time, to replace flaulty flight time NOTE: REMOVE OLD FLIGHT TIME ABOVE
    def seg_block_hours(row):
        #idiosyncratic function for estimating block hours from AOTP 
        try: 
            block_hours=aotp_mar_times.get_group((row['UNIQUE_CARRIER'],row['BI_MARKET']))['AIR_TIME'].iloc[0]
        except KeyError:
            try:
                aotp_mar_times_avg =aotp_mar[['UNIQUE_CARRIER','BI_MARKET','AIR_TIME']].groupby(['UNIQUE_CARRIER','BI_MARKET']).aggregate(lambda x: np.mean(x)/60)
                aotp_mar_times_avg =aotp_mar_times_avg.reset_index().groupby(['BI_MARKET'])
                block_hours=aotp_mar_times_avg.get_group(row['BI_MARKET'])['AIR_TIME'].iloc[0]
            except KeyError:
                mkk=row['BI_MARKET'].replace('ONT','LAX')
                block_hours=aotp_mar_times_avg.get_group(mkk)['AIR_TIME'].iloc[0]   
        return block_hours
    t100ranked['AOTP_FLIGHT_TIME']=t100ranked.apply(seg_block_hours,1)
    t100ranked['BACKFOURTH'] = 2*(t100ranked['AOTP_FLIGHT_TIME']+45/60)

    ##t100_avgd_clip = t100_avgd_clip.groupby('BI_MARKET').apply(market_rank)
    t100ranked.to_csv(output_file)
    
    return t100ranked
'''
helper function to create a bidirectional market indicator (with airports sorted by text) for origin-destination pairs
'''    
def create_market(row):
    market = [row['ORIGIN'], row['DEST']]
    market.sort()
    return "_".join(market)
'''
helper function get a weighed average costs and flight times across a directional market
'''
def avg_costs(gb):
    cost_weighted = np.average(gb['FLIGHT_COST'], weights=gb['DAILY_FREQ'])
    gb['FLIGHT_COST'] = np.repeat(cost_weighted,gb.shape[0])
    time_weighted = np.average(gb['FLIGHT_TIME'], weights=gb['DAILY_FREQ'])
    gb['FLIGHT_TIME'] = np.repeat(time_weighted,gb.shape[0])            
    return gb
'''
helper function to average across aircraft types and rank carriers by passenger flow 
via pandas groupby function, recieves sub-dataframes, each one comprising a market
'''      
def market_rank(gb, ms_cuttoff):                                  
    Mtot = gb['PASSENGERS'].sum()
    gb['MARKET_TOT'] = np.repeat(Mtot,gb.shape[0] )    
    Mcount =gb.shape[0]
    gb['MARKET_COMPETITORS'] = np.repeat(Mcount,gb.shape[0] )
    rank = np.array(gb['PASSENGERS'].tolist()).argsort()[::-1].argsort() +1 
    gb['MARKET_RANK'] = rank         
    gb = gb.sort(columns=['MARKET_RANK'],ascending=True,axis =0)        
    gb['MS_TOT']=gb['PASSENGERS']/gb['MARKET_TOT']
    #cumulative market share upto and including that ranking
    gb['CUM_MS']=gb.apply(lambda x: gb['MS_TOT'][:x['MARKET_RANK']].sum(), axis=1)
    #cumulative market share upto that ranking
    gb['PREV_CUM_MS']=gb.apply(lambda x: gb['MS_TOT'][:x['MARKET_RANK']-1].sum(), axis=1)
    #remove those carriers that appear after cuttoff
    gb=gb[gb['MS_TOT']>=ms_cuttoff]
    #recalculate market shares
    Mtot = gb['PASSENGERS'].sum()
    #get total market size
    gb['MARKET_TOT'] = np.repeat(Mtot,gb.shape[0] )   
    #get total number of competitors in market and save as column 
    Mcount =gb.shape[0]
    gb['MARKET_COMPETITORS'] = np.repeat(Mcount,gb.shape[0] )
    #get market share as passengers for that carrier over total market size 
    gb['MS_TOT']=gb['PASSENGERS']/gb['MARKET_TOT']
    return gb    


 #function to add modifiers to market size for each market/carier combo based on fraction of connecting passengers 
def get_market_connection_modifiers( market_table_fn= "processed_data/nonstop_competitive_markets_reg2.csv",outfile='processed_data/nonstop_competitive_markets_mktmod_reg2.csv'):   
    t100ranked = pd.read_csv(market_table_fn)   
    t100ranked['CARRIER_MARKET'] = t100ranked.apply(lambda row: row['UNIQUE_CARRIER'] + '_' + row['BI_MARKET'],1)
    t100ranked['connection_demands'] = np.zeros((t100ranked.shape[0],1))
    t100ranked = t100ranked.set_index('CARRIER_MARKET')
    markets_sorted = sorted(list(set(t100ranked['BI_MARKET'].tolist())))   
    nonstop_numbers = {mkt:0 for mkt in markets_sorted}
    total_numbers={mkt:0 for mkt in markets_sorted}
    ###carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    market_dict={mkt: list(set(t100ranked.groupby('BI_MARKET').get_group(mkt)['UNIQUE_CARRIER'].tolist()).intersection(['AS','UA','US','WN'])) for mkt in markets_sorted}
    route_demands = pd.read_csv('bts_data/route_demand_Q1.csv')
    def create_market(row):
        market = [row['ORIGIN'], row['DESTINATION']]
        market.sort()
        return "_".join(market)
    for i,line in enumerate(route_demands.to_dict('records')):       
        if i%1000 ==0:
            print(i)
        bimarket="_".join(sorted([line['ORIGIN'],line['DESTINATION']]))
        leg1 = "_".join(sorted([line['ORIGIN'],line['CONNECTION']])) if line['NUM_FLIGHTS']==2 else 'NULL'
        leg2 = "_".join(sorted([line['CONNECTION'],line['DESTINATION']])) if line['NUM_FLIGHTS']==2 else 'NULL'
        carrier1 =line['FIRST_OPERATING_CARRIER']
        carrier2 =line['SECOND_OPERATING_CARRIER']
        #if non-stop market passengers, add to 
        if line['NUM_FLIGHTS']==1 and bimarket in market_dict and carrier1 in market_dict[bimarket]:
            nonstop_numbers[bimarket] += line['PASSENGERS']
            total_numbers[bimarket] +=line['PASSENGERS']
        if line['NUM_FLIGHTS']==2 and leg1 in market_dict and carrier1 in market_dict[leg1]:
            total_numbers[leg1] +=line['PASSENGERS']
            t100ranked.loc[carrier1 + '_' + leg1,'connection_demands'] += line['PASSENGERS']
        if line['NUM_FLIGHTS']==2 and leg2 in market_dict and carrier2 in market_dict[leg2]:
            total_numbers[leg2] +=line['PASSENGERS']
            t100ranked.loc[carrier2 + '_' + leg2,'connection_demands'] += line['PASSENGERS']
    def market_adjust(row):
        if row['UNIQUE_CARRIER'] in ['AS','UA','US','WN']:
            ratio=(row['connection_demands'] + nonstop_numbers[row['BI_MARKET']])/total_numbers[row['BI_MARKET']]
            mkt_adj = row['MARKET_TOT']*ratio
        else:
            ratio = -1
            mkt_adj = row['MARKET_TOT']
        return pd.Series({'adj_ratio': ratio, 'new_market': mkt_adj})
    t100ranked[['adj_ratio','new_market']]=t100ranked.apply(market_adjust,1)
    t100ranked.reset_index().to_csv(outfile)
    return t100ranked
        

'''
STEP TWO: ANALYZE NETWORK FLEET COMPOSITION
NOTE: Ftable creation requires hand association of SHORT NAME and MODEL categories, and the set may differ if the airlines are diferent
'''    

'''
function to get fleets available to each carrier by comparing time ratios in and out of network and comparing to full inventory size
use_lower_F_bound: if true, use frequency rather than airtime to determine fleet size
'''
def Ftable_new(output_fn="processed_data/fleet_lookup_reg3.csv", include_regional_carriers=True, use_lower_F_bound=True, full_t00_fn="bts_data/t100_seg_all.csv", ac_type_fn ="bts_data/AIRCRAFT_TYPE_LOOKUP.csv",t100summed_fn="processed_data/t100_summed_reg3.csv",market_table_fn= "processed_data/nonstop_competitive_markets_reg3.csv",b43_fn = "bts_data/SCHEDULE_B43.csv"):
    #load domestic and international T100 records
    t100_all = pd.read_csv(full_t00_fn)
    #load inventory and aircraft data
    b43 = pd.read_csv(b43_fn)
    type1 = pd.read_csv(ac_type_fn)
    #load output of nonstop_market_profile function, and premptively grouby carrier for efficient looping
    t100ranked = pd.read_csv(market_table_fn)
    t100ranked_gb =t100ranked.groupby(['UNIQUE_CARRIER'])
    t100_summed =pd.read_csv(t100summed_fn)
    t100_gb = t100_summed.groupby(['UNIQUE_CARRIER'])
    #get sets of relevant markets and carriers
    markets_sorted = sorted(list(set(t100ranked['BI_MARKET'].tolist())))       
    carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    #using T100 records get proportion of flight time in and out of our network for different craft/carrier combinations
    treduced=t100_all[t100_all['UNIQUE_CARRIER'].isin(carriers_sorted)]
    treduced['BI_MARKET']=treduced.apply(create_market,1)
    treduced['AIR_TIME']= treduced['AIR_TIME']/60
    treduced['IN_NETWORK'] = treduced.apply(lambda x: 1 if x['BI_MARKET'] in markets_sorted else 0, 1)
    treduced['TIME_IN_NETWORK'] = treduced['IN_NETWORK']*treduced['AIR_TIME']
    tr_gb = treduced.groupby(['UNIQUE_CARRIER','AIRCRAFT_TYPE'])
    #create aircraft type numerical ID to short name converson table
    keys =sorted(list(set(t100_summed['AIRCRAFT_TYPE'].tolist()))) #relevant aircraft types        
    reduced_type = type1.set_index('AC_TYPEID').loc[keys].reset_index()[['AC_TYPEID','SHORT_NAME']] #first, filter type type list by relevant aircraft types
        #apparently corresponding model (constructed by hand from examination of SHORT_NAME from Aircraft Type Table and Model fromschedule B43
    #models with regional carrier
    if include_regional_carriers:
        model =[['SF-340/A'],['EMB-120'],['DASH8-Q4'],['DASH8-1'],['DHC8-200'],['B737-7','B737-7/L'],['B737-8'],['B737-5'],['B737-4'],['B737-3'],['B757-2'],['B767-2'],['B767-3'],['B777-2'],['CRJ-2/4'],['RJ-700'],['B737-9'],['CRJ-900'],['A318'],['MD-80'],['MD-90'],['EMB-135'],['EMB-145'],['EMB-140'],['A320-1/2'],['A319'],['A321']]    
    else:        
        model = [['B737-7','B737-7/L'],['B737-8'],['B737-5'],['B737-4'],['B737-3'],['B757-2'],['B767-3'],['B777-2'],['B737-9'],['MD-80'],['A320-1/2'],['A319'],['A321']]    
    reduced_type['model']=pd.DataFrame({'model':model})
    reduced_type=reduced_type.set_index('AC_TYPEID')
    reduced_type.to_csv('processed_data/model_lookup_reg.csv')
    #average freqs for market-carrier-ac_Tye
    freqs_across_markets_carrier_ac_type = t100_summed[['UNIQUE_CARRIER','BI_MARKET','DAILY_FREQ','AIRCRAFT_TYPE']].groupby(['UNIQUE_CARRIER','BI_MARKET','AIRCRAFT_TYPE']).aggregate(np.mean).reset_index().groupby(['UNIQUE_CARRIER','AIRCRAFT_TYPE'])
    # construct F tabke
    rows=[]
    for airline in carriers_sorted:
        #get relevant aircraft types for this carrier
        ac_types = list(set(t100_gb.get_group(airline)['AIRCRAFT_TYPE'].tolist()))
        for ac_type in ac_types:
            row={}            
            row['carrier'] = airline
            row['aircraft_type']=ac_type
            #get time in network ratio for this craft computed above
            carrier_type = tr_gb.get_group((row['carrier'],row['aircraft_type']))
            rat=carrier_type['TIME_IN_NETWORK'].sum()/carrier_type['AIR_TIME'].sum()            
            #b43 model code
            model = reduced_type.loc[ac_type]['model']
            print(model) #should be a lists
            #pull data on this craft for this carrier from b4
            craft_data =b43[b43['CARRIER']==airline][b43['MODEL'].isin(model)]
            craft_data = craft_data[craft_data['OPERATING_STATUS']=='Y'] #get only operating craft
            craft_data = craft_data[craft_data['NUMBER_OF_SEATS']>0] #get only passenger  craft
            fleet_size = craft_data.shape[0] #number of craft
            #take mean number of seats for this craft and carrier
            craft_seats = craft_data['NUMBER_OF_SEATS'].mean()
            #calculate F total in inventory times time in network ratio
            F=fleet_size*rat
            #get lower bound on F from observed flight times and frequencies of this craft 
            current_freqs_df=freqs_across_markets_carrier_ac_type.get_group((airline, ac_type))  
            freqs_by_market=[[record['BI_MARKET'],record['DAILY_FREQ']] for record in current_freqs_df.to_dict('records')]               
            carrier_df = t100ranked_gb.get_group(airline).set_index('BI_MARKET')            
            F_lower_bound = sum([2*f[1]*(45/60+carrier_df.loc[f[0]]['AOTP_FLIGHT_TIME']) for f in freqs_by_market if f[0] in carrier_df.index ])/18
            #save data to row
            row['fleet_full'] = fleet_size
            row['craft_seats'] = craft_seats
            row['F_old'] = F
            row['F_lower_bound'] = F_lower_bound
            row['in_net_ratio'] =  rat
            rows.append(row)
    #create data frame, create new F accounting for lower bount        
    fleet_lookup =pd.DataFrame(rows)  
    
    fleet_lookup['below_bound'] = fleet_lookup.apply(lambda x: 1 if x['F_old'] < x['F_lower_bound'] else 0,1)
    if use_lower_F_bound:
        fleet_lookup['F'] = fleet_lookup['F_lower_bound']
    else:
        fleet_lookup['F'] = fleet_lookup.apply(lambda x: max( x['F_old'], x['F_lower_bound']),1)
    fleet_lookup.to_csv(output_fn)
    return [fleet_lookup, reduced_type]


#divide fleets up by market type (1,2,or 3 player) (make sure things sum to intermarket values), creates additional lookup table for divided markets
def Ftable_MarketDiv(output_fn="processed_data/fleet_lookup_reg3_marketdiv.csv", include_regional_carriers=True,use_lower_F_bound=True, full_t00_fn="bts_data/t100_seg_all.csv", ac_type_fn ="bts_data/AIRCRAFT_TYPE_LOOKUP.csv",t100summed_fn="processed_data/t100_summed_reg3.csv",market_table_fn= "processed_data/nonstop_competitive_markets_reg3.csv",b43_fn = "bts_data/SCHEDULE_B43.csv"):
    #load domestic and international T100 records
    t100_all = pd.read_csv(full_t00_fn)
    #load inventory and aircraft data
    b43 = pd.read_csv(b43_fn)
    type1 = pd.read_csv(ac_type_fn)
    #load output of nonstop_market_profile function, and premptively grouby carrier for efficient looping
    t100ranked = pd.read_csv(market_table_fn)
    t100_gb_market = t100ranked.groupby('BI_MARKET')
    t100ranked_gb =t100ranked.groupby(['UNIQUE_CARRIER'])
    t100_summed =pd.read_csv(t100summed_fn)
    t100_gb = t100_summed.groupby(['UNIQUE_CARRIER'])
    #get sets of relevant markets and carriers
    markets_sorted = sorted(list(set(t100ranked['BI_MARKET'].tolist())))       
    carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    #using T100 records get proportion of flight time in and out of our network for different craft/carrier combinations
    treduced=t100_all[t100_all['UNIQUE_CARRIER'].isin(carriers_sorted)]
    treduced['BI_MARKET']=treduced.apply(create_market,1)
    treduced['AIR_TIME']= treduced['AIR_TIME']/60
    treduced['IN_NETWORK'] = treduced.apply(lambda x: 1 if x['BI_MARKET'] in markets_sorted else 0, 1)
    treduced['TIME_IN_NETWORK'] = treduced['IN_NETWORK']*treduced['AIR_TIME']    
    #create aircraft type numerical ID to short name converson table
    keys =sorted(list(set(t100_summed['AIRCRAFT_TYPE'].tolist()))) #relevant aircraft types        
    reduced_type = type1.set_index('AC_TYPEID').loc[keys].reset_index()[['AC_TYPEID','SHORT_NAME']] #first, filter type type list by relevant aircraft types
        #apparently corresponding model (constructed by hand from examination of SHORT_NAME from Aircraft Type Table and Model fromschedule B43
     #models with regional carrier
    if include_regional_carriers:
        model =[['SF-340/A'],['EMB-120'],['DASH8-Q4'],['DASH8-1'],['DHC8-200'],['B737-7','B737-7/L'],['B737-8'],['B737-5'],['B737-4'],['B737-3'],['B757-2'],['B767-2'],['B767-3'],['B777-2'],['CRJ-2/4'],['RJ-700'],['B737-9'],['CRJ-900'],['A318'],['MD-80'],['MD-90'],['EMB-135'],['EMB-145'],['EMB-140'],['A320-1/2'],['A319'],['A321']]    
    else:        
        model = [['B737-7','B737-7/L'],['B737-8'],['B737-5'],['B737-4'],['B737-3'],['B757-2'],['B767-3'],['B777-2'],['B737-9'],['MD-80'],['A320-1/2'],['A319'],['A321']]    
    reduced_type['model']=pd.DataFrame({'model':model})
    reduced_type=reduced_type.set_index('AC_TYPEID')
    reduced_type.to_csv('processed_data/model_lookup.csv')
    #average freqs for market-carrier-ac_Tye
    freqs_across_markets = t100_summed[['UNIQUE_CARRIER','BI_MARKET','DAILY_FREQ','AIRCRAFT_TYPE']].groupby(['UNIQUE_CARRIER','BI_MARKET','AIRCRAFT_TYPE']).aggregate(np.mean).reset_index()
    # construct F table   
    freqs_across_markets_filt = freqs_across_markets[freqs_across_markets['BI_MARKET'].isin(markets_sorted)]
    def add_competitors(row):
        return int(t100_gb_market.get_group(row['BI_MARKET'])['MARKET_COMPETITORS'].iloc[0])
    freqs_across_markets_filt['MARKET_COMPETITORS'] = freqs_across_markets_filt.apply(add_competitors,1)
    freqs_across_markets_carrier_ac_type =freqs_across_markets_filt.groupby(['UNIQUE_CARRIER','AIRCRAFT_TYPE','MARKET_COMPETITORS'])
    rows=[]
    for airline in carriers_sorted:
        for competitors in range(1,4):
            #get relevant aircraft types for this carrier
            ac_types = list(set(t100_gb.get_group(airline)['AIRCRAFT_TYPE'].tolist()))
            for ac_type in ac_types:
                row={}            
                row['carrier'] = airline
                row['aircraft_type']=str(ac_type) + '_' + str(competitors)
                #get time in network ratio for this craft computed above
                
                rat=np.nan           
                #b43 model code
                model = reduced_type.loc[ac_type]['model']
                print(model) #should be a lists
                #pull data on this craft for this carrier from b4
                craft_data =b43[b43['CARRIER']==airline][b43['MODEL'].isin(model)]
                craft_data = craft_data[craft_data['OPERATING_STATUS']=='Y'] #get only operating craft
                craft_data = craft_data[craft_data['NUMBER_OF_SEATS']>0] #get only passenger  craft
                fleet_size = craft_data.shape[0] #number of craft
                #take mean number of seats for this craft and carrier
                craft_seats = craft_data['NUMBER_OF_SEATS'].mean()
                #calculate F total in inventory times time in network ratio
                F=fleet_size*rat
                #get lower bound on F from observed flight times and frequencies of this craft 
                try:
                    current_freqs_df=freqs_across_markets_carrier_ac_type.get_group((airline, ac_type,competitors))  
                    freqs_by_market=[[record['BI_MARKET'],record['DAILY_FREQ']] for record in current_freqs_df.to_dict('records')]               
                  
                
                    carrier_df = t100ranked_gb.get_group(airline).set_index('BI_MARKET')
                    F_lower_bound = sum([2*f[1]*(45/60+carrier_df.loc[f[0]]['AOTP_FLIGHT_TIME']) for f in freqs_by_market if f[0] in carrier_df.index ])/18
                    #save data to row
                    row['fleet_full'] = fleet_size
                    row['craft_seats'] = craft_seats
                    row['F_old'] = F
                    row['F_lower_bound'] = F_lower_bound
                    row['in_net_ratio'] =  rat
                    rows.append(row)
                except KeyError:
                    pass
                
        #create data frame, create new F accounting for lower bount        
    fleet_lookup =pd.DataFrame(rows)  
    
    fleet_lookup['below_bound'] = fleet_lookup.apply(lambda x: 1 if x['F_old'] < x['F_lower_bound'] else 0,1)
    if use_lower_F_bound:
        fleet_lookup['F'] = fleet_lookup['F_lower_bound']
    else:
        fleet_lookup['F'] = fleet_lookup.apply(lambda x: max( x['F_old'], x['F_lower_bound']),1)
    fleet_lookup.to_csv(output_fn)
    return fleet_lookup
     
'''
function to find most common type of plane used on each segment
and to get a fleet composition for network for each carrier

'''  
def fleet_assign(output_fn="processed_data/fleetdist_reg3.csv", airtimes_fn_out='processed_data/airtimes_reg3.csv',t100_summed_fn = 'processed_data/t100_summed_reg3.csv',fleet_lookup_fn = "processed_data/fleet_lookup_reg3.csv",market_table_fn= "processed_data/nonstop_competitive_markets_reg3.csv"):
    #load netowrk data file created by nonstop_market_profile function
    t100ranked = pd.read_csv(market_table_fn)
    #get carriers and markets under study
    markets =  t100ranked['BI_MARKET'].tolist()
    carriers = t100ranked['UNIQUE_CARRIER'].tolist()
    #load network data broken down by craft type
    t100_summed= pd.read_csv(t100_summed_fn)
    t100gb = t100_summed.groupby(['UNIQUE_CARRIER','BI_MARKET'])
    t100gb_carrier = t100_summed.groupby(['UNIQUE_CARRIER'])
    #load fleet data for relevant carriers on this network
    fleet_lookup= pd.read_csv(fleet_lookup_fn)
    fleet_lookup_gb = fleet_lookup.groupby(['carrier','aircraft_type'])   
    #loop through each carrier market combo found, catalogue the presence of aircraft types
    air_times = [] #initialize seperate list for airtimes by carrier/market/aircrafttype
    rows = []            
    i=0
    for market, carrier in zip(markets, carriers):
        i+=1
        row = {}
        group = t100gb.get_group((carrier, market))  
        #sum over market directions #NOTE: PERHAPS CHANGE SEATS TO OTHER METHOD IF LOOKING INCONGRUENT
        group =group[['BI_MARKET','UNIQUE_CARRIER','PASSENGERS','FLIGHT_TIME','FLIGHT_COST','DAILY_FREQ','AIRCRAFT_TYPE','AIR_HOURS']].groupby(['BI_MARKET','UNIQUE_CARRIER','AIRCRAFT_TYPE']).aggregate({'PASSENGERS':np.sum,'DAILY_FREQ':np.sum,'AIR_HOURS':np.sum, 'FLIGHT_TIME':np.mean,'FLIGHT_COST':np.mean}).reset_index()
        #append group to airtime table       
        air_times.append(group[['BI_MARKET','UNIQUE_CARRIER','AIRCRAFT_TYPE','AIR_HOURS']])        
        #total passengers across types
        totpax = group['PASSENGERS'].sum()
        #total frequency across types
        totfreq = group['DAILY_FREQ'].sum()
        #sort types according to frequency
        group_sort = group.sort(columns=['DAILY_FREQ'], axis=0,ascending=False)
        #get proportions of frequency and passengers each type accounts for 
        group_sort['PPAX']=group_sort['PASSENGERS']/totpax
        group_sort['PFREQ']=group_sort['DAILY_FREQ']/totfreq
        #get type with max freq and its associated data
        max_perc = group_sort['PFREQ'].iloc[0]        
        max_pax = group_sort['PASSENGERS'].iloc[0]
        max_type=group_sort['AIRCRAFT_TYPE'].iloc[0]  
        #get the number of seats this aircraft has        
        
        def get_seats(x):           
            return float(fleet_lookup_gb.get_group((carrier,x['AIRCRAFT_TYPE']))['craft_seats'])            
            
        group_sort['CRAFT_SEATS'] = group_sort.apply(get_seats, axis=1)  
        def get_F(x):
            return float(fleet_lookup_gb.get_group((carrier,x['AIRCRAFT_TYPE']))['F'])        
        group_sort['F']= group_sort.apply(get_F, axis=1)
        #create a dictionary of all types for this carrier/segment            
        type_dict = {gs['AIRCRAFT_TYPE']:[round(gs['CRAFT_SEATS']), round(gs['DAILY_FREQ'],2),round(gs['PFREQ'],2),round(gs['F'],2)] for gs in group_sort.to_dict('records')}
       #place into row
        row['bimarket'] = market
        row['carrier'] = carrier         
        row['max_type'] = max_type
        row['max_perc'] = max_perc
        row['max_pax'] = max_pax        
        row['type_dict_seats_f_pf_F'] = type_dict   
        #carrier distribution overall all segments
        craftlist=list(set(t100gb_carrier.get_group(carrier)['AIRCRAFT_TYPE'].tolist()))
        #get seats for each craft        
        craft_seats = [(craft,round(float(fleet_lookup_gb.get_group((carrier,craft))['craft_seats']))) for craft in craftlist]
        row['type_list'] = craft_seats
        rows.append(row)
        #construct data frame (MAY WISH TO PRESENT DATA IN A WAY THAT MAKES FOR EASIER ASsIGNEMENT  DECISIONS)
    fleet_dist = pd.DataFrame(rows)     
    fleet_dist.to_csv(output_fn, sep=';')    
    airtime_merge=pd.concat(air_times)
    airtime_merge.to_csv(airtimes_fn_out)
    return fleet_dist


'''
NOTE: now make segment craft assignments, by hand
'''   
'''
function to add market size as a fleet type to aug_fleet so that markets of different sizes do not share fleets
DO THIS FOR UNSEGMENTED MARKETS AS WELL
ADD COMMENTS

'''
def aug_fleet_market_add(outfile='processed_data/aug_fleet_mktDiv_reg1.csv',only_big_carriers=False, carriers_of_interest = ['AS','UA','US','WN'],market_table_fn= "processed_data/nonstop_competitive_markets_reg1.csv", fleet_dist_aug_fn='processed_data/fleet_dist_aug_reg1.csv'):
    aug_fleet = pd.read_csv(fleet_dist_aug_fn).dropna(axis=0)
    if only_big_carriers:
        aug_fleet = aug_fleet[aug_fleet['carrier'].isin(carriers_of_interest)]
    t100ranked = pd.read_csv(market_table_fn)
    t100_gb_market = t100ranked.groupby('BI_MARKET') 
    def add_competitors(row):
        return int(t100_gb_market.get_group(row['bimarket'])['MARKET_COMPETITORS'].iloc[0])
    aug_fleet['competitors']=aug_fleet.apply(add_competitors,1)
    def add_competitor_types(row):
        comps = row['competitors']
        split_types = row['assigned_type'].split('-')
        new_types = '-'.join([tp + '_' + str(comps) for tp in split_types])
        return new_types
    aug_fleet2=aug_fleet
    aug_fleet2['assigned_type']=aug_fleet2.apply(add_competitor_types,1)
    aug_fleet2.to_csv(outfile)
    return aug_fleet2
    
        

'''
function to create table of carrier and market data used by matlab myopic best response network game
#NOTE:CREATE AND OFFICIAL INDEX OF CARRIER MARKET COMBO FOR EASY MAPPING
if use_adj_market is True, market sizes will be adjusted to account for nonstop passengers

'''    

def create_network_game_datatable(outfile='processed_data/carrier_data_reg2.txt',coef_outfile='processed_data/transcoef_table_mktMod_reg2.csv', use_adj_market=True,t100ranked_fn = 'processed_data/nonstop_competitive_markets_mktmod_reg2.csv', fleet_lookup_fn = "processed_data/fleet_lookup_reg2.csv",aotp_fn = 'bts_data/aotp_march.csv',fleet_dist_aug_fn='processed_data/fleet_dist_aug_reg2.csv'):   
    #read in data files     
    fleet_lookup= pd.read_csv(fleet_lookup_fn)
    aug_fleet = pd.read_csv(fleet_dist_aug_fn) 
    t100ranked  = pd.read_csv(t100ranked_fn)
    #flgith times by airline market combo
    aotp_mar = pd.read_csv(aotp_fn)
    aotp_mar['BI_MARKET']=aotp_mar.apply(create_market,1) 
    #NOTE: DISSAGREGGATE BY AIRCRAFT TYPE LATER
    aotp_mar_times = aotp_mar[['UNIQUE_CARRIER','BI_MARKET','AIR_TIME']].groupby(['UNIQUE_CARRIER','BI_MARKET']).aggregate(lambda x: np.mean(x)/60)
    aotp_mar_times = aotp_mar_times.reset_index().groupby(['UNIQUE_CARRIER','BI_MARKET'])
    #create input file for MATLAB based myopic best response network game
    with open(outfile,'w') as outfile:       
        # group competitive markets table by market
        t100_gb_market = t100ranked.groupby('BI_MARKET')
        #get set of markets
        markets_sorted = sorted(list(set(t100ranked['BI_MARKET'].tolist())))
        num_mkts = len(markets_sorted)
        #get set of carriers
        carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
        num_carriers = len(carriers_sorted)
        #write number of carries and number of markets as first line in file
        outfile.write(str(num_carriers) + "\t" + str(num_mkts) + "\n")
        #write market sizes in order of markets sorted alphabetically as second line in file (as matlab vector)
        mkt_sizes = [str(t100_gb_market.get_group(mkt)['MARKET_COMPETITORS'].iloc[0]) for mkt in markets_sorted]
        mkt_sizes_str = "["+",".join(mkt_sizes)+"]"
        outfile.write(mkt_sizes_str + "\n")
        #write line of empirical frequencies in order of sorted markets (each sorted my market rank)
        empirical_freqs = t100ranked['DAILY_FREQ'].tolist()
        empirical_freqs_str = "["+",".join([str(f) for f in empirical_freqs])+"]"
        outfile.write(empirical_freqs_str + "\n")
        #write line of the carrier (by MATLAB index in carriers sorted) of each of the frequencies above
        corresponding_carriers = [carriers_sorted.index(CR) +1  for CR in t100ranked['UNIQUE_CARRIER'].tolist()]
        corresponding_carriers_str = "["+",".join([str(cr) for cr in corresponding_carriers])+"]"
        outfile.write(corresponding_carriers_str + "\n")
        #group fleet table by carrier for ease of access
        aug_fleet_gb_carrier = aug_fleet.groupby('carrier')
        #loop through carriers, for each line, write A matrix for optimization inequality constraints, corresponding b vector, indices
        #of markets that carrier is in , index of that carriers frequency within that market (in market frequency data structure to be created 
        #in MATLAB, and concatenated profit coefficient vectors in order of sorted market)
        coefficient_table_rows = [] #initialize coef table
        for i, carrier in enumerate(carriers_sorted):
            print(carrier)
            #get data related to  current carrier from t100ranked, sorted in order of market and then carrier rank in market
            carrier_data = t100ranked[t100ranked['UNIQUE_CARRIER']==carrier]
            carrier_markets_str = carrier_data['BI_MARKET'].tolist() #markets under consideration
            #from fleet table get relevant rows on current carrier in order of sorted markets
            fleet_assign=aug_fleet_gb_carrier.get_group(carrier).set_index('bimarket').loc[carrier_markets_str].reset_index()
            ##fleet_assign['bimarket']=fleet_assign['index']
            fleet_assign=fleet_assign.sort(columns=['bimarket'])
            #get different craft types  for this carrier (sorted)
            ac_types = sorted(list(set(fleet_assign['assigned_type'].tolist())))
            # group fleet table subset by craft type
            fleet_assign_gb_type = fleet_assign.groupby('assigned_type')
            #build A matrix and b matrix
            A_rows = []
            b_rows = []
            #each row of A and b is  (potentially hybrid) aircraft type...
            for ac_type in ac_types:
                #get rows from fleet table relevant to this carrier/craft type
                mkts_for_craft_df = fleet_assign_gb_type.get_group(ac_type)
                #get markets for these, (these will form the columns of the A matrix)
                mkts_for_craft = mkts_for_craft_df['bimarket'].tolist()
                a_row = [] #initialize row
                #for each column of A matrix (in this row)...
                for mk in carrier_markets_str:
                    #if market is relevant to current aircraft type, cell is 2(blockhours +turnaround_time)
                    if mk in mkts_for_craft:
                        #attempt to calculate the above from AOTP data
                        try:
                            block_hours=aotp_mar_times.get_group((carrier,mk))['AIR_TIME'].iloc[0]
                        except KeyError: #if blackhours can't be found for specific carrier, take averaege accross carrier
                            try:
                                aotp_mar_times_avg =aotp_mar[['UNIQUE_CARRIER','BI_MARKET','AIR_TIME']].groupby(['UNIQUE_CARRIER','BI_MARKET']).aggregate(lambda x: np.mean(x)/60)
                                aotp_mar_times_avg =aotp_mar_times_avg.reset_index().groupby(['BI_MARKET'])
                                block_hours=aotp_mar_times_avg.get_group(mk)['AIR_TIME'].iloc[0]
                            except KeyError: #if ONT time can't be found, approximate with LAX
                                mkk=mk.replace('ONT','LAX') 
                                block_hours=aotp_mar_times_avg.get_group(mkk)['AIR_TIME'].iloc[0]    
                        a_row.append(2*(block_hours +45/60))
                    #otherwise, no constraint for this market
                    else:
                        a_row.append(0)                    
                A_rows.append(a_row)
                #sum F accross compents of hybrid carrier
                try:
                    F = sum([fleet_lookup.groupby(['carrier','aircraft_type']).get_group((carrier,int(subtype)))['F'].iloc[0] for subtype in ac_type.split('-') ])
                except KeyError:
                    F = sum([fleet_lookup.groupby(['carrier','aircraft_type']).get_group((carrier,subtype))['F'].iloc[0] for subtype in ac_type.split('-') ])
                b_rows.append(18*F)
            #index of relevant markets doe rgia carrier 
            carrier_Markets = [markets_sorted.index(mk)+1 for mk in carrier_markets_str]
            #index of frequency for each of these market vectors, based on market rank
            carrier_freq_ind = carrier_data['MARKET_RANK'].tolist()
            #get coefficients, stacked in order of markets
            carrier_coef = []
           
            #go through each market of carrier (in alphabetical order)
            for record in carrier_data.to_dict('records'):
                #parameters for transformation from base coefficients to coefficients reflecting particular costs and market sizes
                #old and new costs
                Cold = 10000
                Cnew = record['FLIGHT_COST']
                #old and new market sizes
                Mold = 1000
                #get new adjusted market size if requested by user, else use normal market size (same across carriers in a market)
                if use_adj_market:
                    Mnew = record['new_market']
                else:
                    Mnew = record['MARKET_TOT']
                #frequency index of carrier in market to determine order of coefficients
                freq_ind = record['MARKET_RANK']
                #create coefficients based on how many competitors in market                
                if record['MARKET_COMPETITORS']==1:
                    base = [-95164.0447,-36238.3083,1148.0305]
                    transcoef = [-(Mnew/Mold)*base[0],(Mnew/Mold)*(Cold-base[1])-Cnew,-(Mnew/Mold)*base[2] ]
                    coef_cats = [100,100,100]
                elif record['MARKET_COMPETITORS']==2:
                    base = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]
                    transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,3)] + [-(Mnew/Mold)*base[3] if i==freq_ind else -(Mnew/Mold)*base[4] for i in range(1,3)] + [-(Mnew/Mold)*base[5]]
                    coef_cats = [1] + [2 if i==freq_ind else 3 for i in range(1,3)] + [4 if i==freq_ind else 5 for i in range(1,3)] + [6]
                    
                elif record['MARKET_COMPETITORS']==3:
                    base=[-150395.5496,-10106.6470,13135.9798,13136.1506,264.4822,-376.1793,-376.1781,270.2080,270.1927,-260.0113]
                    transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,4)] + [-(Mnew/Mold)*base[4] if i==freq_ind else -(Mnew/Mold)*base[5] for i in range(1,4)]
                    coef_cats = [1] + [2 if i==freq_ind else 3 for i in range(1,4)] + [4 if i==freq_ind else 5 for i in range(1,4)]
                    if freq_ind ==1:
                        transcoef += [-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[9]]
                        coef_cats += [6,6,7]
                    elif freq_ind == 2:
                        transcoef += [-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[7]]
                        coef_cats += [6,7,6]
                    elif freq_ind == 3:
                        transcoef += [-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[7]]
                        coef_cats += [7,6,6]
                elif record['MARKET_COMPETITORS']==4:
                    base=[-101456.3779,-5039.0076,6450.0318,6450.0511,6450.0624,134.9756,-137.7129,-137.7135,-137.7157,169.9196,169.9198,169.9212,-126.7018,-126.7025,-126.7034]    
                    transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,5)] + [-(Mnew/Mold)*base[5] if i==freq_ind else -(Mnew/Mold)*base[6] for i in range(1,5)]
                    coef_cats= [100,100,100]
                    if freq_ind ==1:
                        transcoef += [-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[12]]
                    elif freq_ind == 2:
                        transcoef += [-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[12]]
                    elif freq_ind == 3:
                        transcoef += [-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[9]]
                    elif freq_ind == 4:
                        transcoef += [-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[12],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[9],]
                else:
                    print('ERROR: UNEXPECTED COMPETITORS >4')
                    return None
                #save coeifficient data to seperate table
                coefficient_table_rows.append( {'carrier':record['UNIQUE_CARRIER'],'bimarket':record['BI_MARKET'],'competitors':record['MARKET_COMPETITORS'],'rank':record['MARKET_RANK'],'coefs':transcoef, 'coef_cats':coef_cats})
                #add vector to full stacked coefficient vector
                carrier_coef += transcoef 
        
            row_string = '['
            for a_row in A_rows:
                row_string+=",".join([str(num) for num in a_row])
                row_string+=";"
            row_string+=']'+'\t'+'['
            row_string+=",".join([str(num) for num in b_rows])
            row_string+=']'+'\t'+'['
            row_string+=",".join([str(num) for num in carrier_Markets])
            row_string+=']'+'\t'+'['
            row_string+=",".join([str(num) for num in carrier_freq_ind])
            row_string+=']'+'\t'+'['
            row_string+=",".join([str(num) for num in carrier_coef])
            row_string+=']'+'\n'
            #write to outfile
            outfile.write(row_string)
    #construct rowstring, using MATLAB vector notation for each componentf
    coef_df = pd.DataFrame(coefficient_table_rows)   
    coef_df.to_csv(coef_outfile,sep=';')
    return coef_df
    
    
    
'''
function to divide coef_df from network game function into 3player, hub, 1player and other 2 player categories
'''
def experiment_categories_1(row):    
    #create list of double-hubs for carriers
    hub_sets = {'WN':['LAX','OAK','PHX','SAN','LAS'],'US':['LAS','PHX'],'UA':['LAX','SFO'],'AS':['SEA','LAX']}
    hub_groups = []    
    for carrier, hubs in hub_sets.items():
        pairs =[sorted([pair[0],pair[1]]) for pair in product(hubs,hubs) if pair[0]!=pair[1] ]
        txtpairs = list(set(["_".join(pair) for pair in pairs]))
        carrier_hubs = [carrier + '_' + txtpair for txtpair in txtpairs ]
        hub_groups += carrier_hubs       
            
    #first check if 3 player, assign to category (split WN and non WN)      
    if int(row['competitors']) ==3:
        cat = 1
    elif row['carrier']+'_' +row['bimarket'] in hub_groups and int(row['competitors']) ==2:
        cat = 2
    elif int(row['competitors']) ==1:
        cat = 4
    else:
        cat = 3
 
    return cat

'''
function to divide coef_df from network game function into 3player, hub, 1player and other 2 player categories, with WN as seperate category
'''
def experiment_categories_WN(row):    
    #create list of double-hubs for carriers
    hub_sets = {'WN':['LAX','OAK','PHX','SAN','LAS'],'US':['LAS','PHX'],'UA':['LAX','SFO'],'AS':['SEA','LAX']}
    hub_groups = []    
    for carrier, hubs in hub_sets.items():
        pairs =[sorted([pair[0],pair[1]]) for pair in product(hubs,hubs) if pair[0]!=pair[1] ]
        txtpairs = list(set(["_".join(pair) for pair in pairs]))
        carrier_hubs = [carrier + '_' + txtpair for txtpair in txtpairs ]
        hub_groups += carrier_hubs       
            
    #first check if 3 player, assign to category (split WN and non WN)
    if row['carrier']!='WN':        
        if int(row['competitors']) ==3:
            cat = 1
        elif row['carrier']+'_' +row['bimarket'] in hub_groups:
            cat = 2
        elif int(row['competitors']) ==1:
            cat = 4
        else:
            cat = 3
    else:
        if int(row['competitors']) ==3:
            cat = 5
        elif row['carrier']+'_' +row['bimarket'] in hub_groups:
            cat = 6
        elif int(row['competitors']) ==1:
            cat = 8
        else:
            cat = 7
    return cat

            
            
            
            
   
             

    
    
    
    

    
#create experimental carrier files with new F and premodified base    
def create_exp_files_modbase(use_adj_market=True):  
    carriers_to_modify = ['AS','UA','US','WN'] #select subset of carriers whose coefficients will be modifed #['WN']
    coef_df = pd.read_csv('processed_data/transcoef_table_mktMod_reg2.csv',sep=';')
    #coef_df is in order of carriers/coefficient vectors used to create file already
    coef_df['category'] = coef_df.apply(experiment_categories_1,1)        
    #category_inds = [list(range(1,8)),list(range(1,7)),list(range(1,7)),list(range(1,4))]

    t100ranked_fn = "processed_data/nonstop_competitive_markets_mktmod_reg1.csv"##"processed_data/nonstop_competitive_markets.csv"
    t100ranked = pd.read_csv(t100ranked_fn)
    t100ranked_gb = t100ranked.groupby(['BI_MARKET','UNIQUE_CARRIER'])
    '''
    coef_3player = [-150395.5496,-10106.6470,13135.9798,13136.1506,264.4822,-376.1793,-376.1781,270.2080,270.1927,-260.0113]
    coef_mat3player = pd.DataFrame([[coef+coef*modif for modif in [round(-1.5+.1*j,1) for j in range(0,31)]] for coef in coef_3player],columns=[round(-1.5+.1*j,1) for j in range(0,31)])
 
    coef_2player = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]
    coef_mat2player = pd.DataFrame([[coef+coef*modif for modif in [round(-1.5+.1*j,1) for j in range(0,31)]] for coef in coef_2player],columns=[round(-1.5+.1*j,1) for j in range(0,31)])
 
    coef_1player = [-95164.0447,-36238.3083,1148.0305]
    coef_mat1player = pd.DataFrame([[coef+coef*modif for modif in [round(-1.5+.1*j,1) for j in range(0,31)]] for coef in coef_1player],columns=[round(-1.5+.1*j,1) for j in range(0,31)])
    '''
    carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    file_ind = 0 #index for which file we are on
    #loop through each carrier-market category 
    coef_ind = 0 #coefficient increment, goes to 22
    coef_cats ={1:[2,4,6],2:[9,11,13],3:[15,17,19],4:[21,22]}
    for coef_ind in [2, 4, 6, 9, 11, 13, 15, 17, 19, 21, 22]:#which coefficient to modify            
        #how much to modify it by
        for modification_factor in [round(-1.5+.1*j,1) for j in range(0,36)]:   
            #read from base file, write to new outfile
            with open('processed_data/carrier_data_reg2.txt','r') as basefile, open('exp_files/carrier_data_basemod_reg2_%s_%s.txt' % (str(coef_ind),str(modification_factor)),'w') as outfile:
                file_ind+=1 #increment file index
                if file_ind % 50 == 0 :
                    print("FILE %s" % file_ind)
                for i,line in enumerate(basefile):
                    if i<4: #first three lines just copy
                        outfile.write(line)
                    else: #make files
                        splitline = line.strip().split()
                        #for carrier in carriers_sorted: NO
                        carrier=carriers_sorted[i-4]                        
                        carrier_group = coef_df[coef_df['carrier']==carrier]
                        #full new coefficient vector
                        new_coefs = [] 
                        #modify coefficients
                        for coef_row in carrier_group.to_dict('records'):
                            #if category being modified, modify coefficients relevant to coef  number
                            if coef_ind in coef_cats[coef_row['category']] and  carrier in carriers_to_modify:
                                coef_record = t100ranked_gb.get_group((coef_row['bimarket'],coef_row['carrier']))
                                Cold = 10000
                                Cnew = float(coef_record['FLIGHT_COST'])
                                #old and new market sizes
                                Mold = 1000                                
                                if use_adj_market:
                                    Mnew = float(coef_record['new_market'])
                                else:
                                    Mnew = float(coef_record['MARKET_TOT'])
                                #frequency index of carrier in market to determine order of coefficients
                                freq_ind = int(coef_record['MARKET_RANK'])
                                if coef_row['competitors']==1:                                            
                                    base = [-95164.0447,-36238.3083,1148.0305]
                                    if coef_ind==21:
                                        base[1] += base[1]*modification_factor
                                    elif coef_ind==22:
                                        base[2] += base[2]*modification_factor
                                    else:
                                        return '1 competitor error' #,freq_ind,coef_ind,coef_cats[coef_row['category']],coef_row
                                    transcoef = [-(Mnew/Mold)*base[0],(Mnew/Mold)*(Cold-base[1])-Cnew,-(Mnew/Mold)*base[2] ]
                                    
                                elif coef_row['competitors']==2:                                            
                                    base = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]
                                    if coef_ind in [9,15]:
                                        base[1] += base[1]*modification_factor
                                    elif coef_ind in [11,17]:
                                        base[3] += base[3]*modification_factor
                                    elif coef_ind in [13,19]:
                                        base[5] += base[5]*modification_factor
                                    else:
                                        return '2 competitor error'
                                    transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,3)] + [-(Mnew/Mold)*base[3] if i==freq_ind else -(Mnew/Mold)*base[4] for i in range(1,3)] + [-(Mnew/Mold)*base[5]]
                                    
                                elif coef_row['competitors']==3:
                                    base=[-150395.5496,-10106.6470,13135.9798,13136.1506,264.4822,-376.1793,-376.1781,270.2080,270.1927,-260.0113]
                                    if coef_ind==2:
                                        base[1] += base[1]*modification_factor
                                    elif coef_ind ==4:
                                        base[4] += base[4]*modification_factor
                                    elif coef_ind==6:
                                        base[7] += base[7]*modification_factor
                                        base[8] += base[8]*modification_factor
                                   
                                    else:
                                        return '3 competitor error'
                                    transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,4)] + [-(Mnew/Mold)*base[4] if i==freq_ind else -(Mnew/Mold)*base[5] for i in range(1,4)]
                                    if freq_ind ==1:
                                        transcoef += [-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[9]]                                                
                                    elif freq_ind == 2:
                                        transcoef += [-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[7]]                                                
                                    elif freq_ind == 3:
                                        transcoef += [-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[7]]
                                    else:
                                        return 'freq ind error'          
                                    
                               
                                   
                                else:
                                    return('MORE THAN THREE CARRIERS, MASSIVE ERROR')
                                mod_coefs=transcoef
                            else:# keep the same if not the current category being modified
                                mod_coefs =  ast.literal_eval(coef_row['coefs'])
                            if file_ind == 100:
                                print(carrier)
                                print('mf: %s, coef num: %s, category: %s, row_cat: %s, ind: %s, c: %s, M %s,' % (modification_factor,50000,777,coef_row['category'],coef_ind,Cnew, Mnew))
                              
                                print(coef_row['coefs'])
                                print(mod_coefs)
                           
                            #add potentially modified coefficients to new vector
                            new_coefs += mod_coefs
                        splitline[-1] = "["+",".join([str(num) for num in new_coefs])+"]"
                        newline = "\t".join(splitline) + "\n"
                        outfile.write(newline)
    return "done" 


'''
function to build easily read data table from MATLAB output
'''
def create_results_table(outfile_fn='network_table_mktDiv_newM.csv',input_fn = "matlab_2stagegames/network_results_mktDiv_newM.csv",t100ranked_fn = "nonstop_competitive_markets.csv"):
    #read in original market profile file    
    t100ranked  = pd.read_csv(t100ranked_fn) 
    #use subset of this as base for results table
    network_results_raw = pd.read_csv(input_fn,header=None)
    network_results = t100ranked[['UNIQUE_CARRIER','BI_MARKET','MARKET_RANK','MARKET_COMPETITORS','DAILY_FREQ']]
    # add estimated frequency column from MATLAB results
    network_results['EST_FREQ'] = network_results_raw[2].tolist()
    #group results by market
    results_market_grouped =network_results.groupby('BI_MARKET')    
    t100_gb_market = t100ranked.groupby('BI_MARKET')
    #extract markets in alphabetical order
    markets_sorted = sorted(list(set(t100ranked['BI_MARKET'].tolist())))
    #get number of competitors in all these markets
    mkt_sizes = [str(t100_gb_market.get_group(mkt)['MARKET_COMPETITORS'].iloc[0]) for mkt in markets_sorted]
    #compute market-wise MAPE    
    MAPES=[]
    for mkt in markets_sorted:
        mkt_gb = results_market_grouped.get_group(mkt)
        fs = mkt_gb['DAILY_FREQ'].tolist()
        f_hats = mkt_gb['EST_FREQ'].tolist()
        mape = sum([abs(f_hat-f) for f_hat,f in zip(f_hats,fs)])/sum(fs)
        MAPES.append(mape)
    #append this calculation to network results table (repeated where market is same)
    mape_column = []
    for competitors, mape in zip(mkt_sizes, MAPES):
        mape_column += np.repeat(mape,int(competitors)).tolist()
    network_results['MAPE'] = mape_column
    #add individual Error column
    network_results['Error']= abs(network_results['DAILY_FREQ']-network_results['EST_FREQ'])/network_results['DAILY_FREQ']
    #compute carrier-wise MAPE
    carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    results_carrier_grouped =network_results.groupby('UNIQUE_CARRIER')
    network_results = network_results.sort(columns=['UNIQUE_CARRIER','BI_MARKET'])
    CARRIER_MAPES=[]    
    num_mkts =  [] #corresponding list of number of markets per carrier
    for cr in carriers_sorted:
        crt_gb = results_carrier_grouped.get_group(cr)
        fs = crt_gb['DAILY_FREQ'].tolist()
        f_hats = crt_gb['EST_FREQ'].tolist()
        mape = sum([abs(f_hat-f) for f_hat,f in zip(f_hats,fs)])/sum(fs)
        CARRIER_MAPES.append(mape)
        num_mkts.append(crt_gb.shape[0])
    #append this calculation to network results table (repeated where carrier is same)
    crmape_column = []    
    for mkts, mape in zip(num_mkts, CARRIER_MAPES):
        crmape_column += np.repeat(mape,int(mkts)).tolist()
    network_results['CR_MAPE'] = crmape_column
    network_results['CATEGORY'] = network_results.apply(experiment_categories_2,1)   
    network_results['ABS_DIFF'] = abs(network_results['DAILY_FREQ']-network_results['EST_FREQ'])
    network_results['DIFF'] = network_results['DAILY_FREQ']-network_results['EST_FREQ']
    #resort dataframe and save to file
    network_results = network_results.sort(columns=['BI_MARKET','MARKET_RANK'])
    network_results.to_csv(outfile_fn,sep='\t')
    return network_results



'''
function to read outpur files, analyze, calculate overall MAPE
NEED FUNCTION TO READ EACH OUTPUT FILE, CALCULATE FULL MAPE, PUT INTO TABLE, USING FUNCTION ABOVE, WHICH WILL NEED TO BE MODIFIED TO HAVE OVERALL MAPE
ADD OVERALL MAPE TO ABOVE FUNCTION SO THAT IT CAN BE USED IN A LOOP INSTEAD OF UGLY FUNCTION BELOW
'''
def experimental_results_table(IO_base = "mkMod_REG_",t100ranked_fn = "processed_data/nonstop_competitive_markets_mktmod_reg.csv"):    
    #resTABLE = pd.DataFrame(index=list(range(1,end_coef)),columns=[round(-.5+.1*i,1) for i in range(0,11)])    
    resTABLE = pd.DataFrame(index=[2, 4, 6, 9, 11, 13, 15, 17, 19, 21, 22],columns=[round(-1.5+.1*j,1) for j in range(0,36)])
    for i in [2, 4, 6, 9, 11, 13, 15, 17, 19, 21, 22]:#range(1,end_coef):        
        for modification_factor in [round(-1.5+.1*j,1) for j in range(0,36)]: #[round(-.5+.1*j,1) for j in range(0,11)]:#[round(-.5,1),round(-.2,1),round(.2,1),round(.5,1)]:#[round(-.5+.1*j,1) for j in range(0,11)]:
            input_fn = "exp_files/exp_results_" + IO_base + "%s_%s.txt" % (i,modification_factor)
            try:                
                #read in original market profile file    
                t100ranked  = pd.read_csv(t100ranked_fn) 
                #use subset of this as base for results table
                network_results_raw = pd.read_csv(input_fn,header=None)
                network_results = t100ranked[['UNIQUE_CARRIER','BI_MARKET','MARKET_RANK','MARKET_COMPETITORS','DAILY_FREQ']]
                # add estimated frequency column from MATLAB results
                network_results['EST_FREQ'] = network_results_raw[2].tolist()
                #group results by market
                results_market_grouped =network_results.groupby('BI_MARKET')    
                t100_gb_market = t100ranked.groupby('BI_MARKET')
                #extract markets in alphabetical order
                markets_sorted = sorted(list(set(t100ranked['BI_MARKET'].tolist())))
                #get number of competitors in all these markets
                mkt_sizes = [str(t100_gb_market.get_group(mkt)['MARKET_COMPETITORS'].iloc[0]) for mkt in markets_sorted]
                #compute market-wise MAPE    
                MAPES=[]
                for mkt in markets_sorted:
                    mkt_gb = results_market_grouped.get_group(mkt)
                    fs = mkt_gb['DAILY_FREQ'].tolist()
                    f_hats = mkt_gb['EST_FREQ'].tolist()
                    mape = sum([abs(f_hat-f) for f_hat,f in zip(f_hats,fs)])/sum(fs)
                    MAPES.append(mape)
                #append this calculation to network results table (repeated where market is same)
                mape_column = []
                for competitors, mape in zip(mkt_sizes, MAPES):
                    mape_column += np.repeat(mape,int(competitors)).tolist()
                network_results['MAPE'] = mape_column
                #add individual Error column
                network_results['Error']= abs(network_results['DAILY_FREQ']-network_results['EST_FREQ'])/network_results['DAILY_FREQ']
                #compute carrier-wise MAPE
                carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
                results_carrier_grouped =network_results.groupby('UNIQUE_CARRIER')
                network_results = network_results.sort(columns=['UNIQUE_CARRIER','BI_MARKET'])
                CARRIER_MAPES=[]    
                num_mkts =  [] #corresponding list of number of markets per carrier
                for cr in carriers_sorted:
                    crt_gb = results_carrier_grouped.get_group(cr)
                    fs = crt_gb['DAILY_FREQ'].tolist()
                    f_hats = crt_gb['EST_FREQ'].tolist()
                    mape = sum([abs(f_hat-f) for f_hat,f in zip(f_hats,fs)])/sum(fs)
                    CARRIER_MAPES.append(mape)
                    num_mkts.append(crt_gb.shape[0])
                #append this calculation to network results table (repeated where carrier is same)
                crmape_column = []    
                for mkts, mape in zip(num_mkts, CARRIER_MAPES):
                    crmape_column += np.repeat(mape,int(mkts)).tolist()
                network_results['CR_MAPE'] = crmape_column
                #resort dataframe and save to file
                network_results = network_results.sort(columns=['BI_MARKET','MARKET_RANK']) 
                network_results.to_csv('exp_files/net_results_' + IO_base + '%s_%s.txt' % (i,modification_factor))
                #calculate overall MAPE                       
                reduced_net = network_results.set_index('UNIQUE_CARRIER').loc[['AS','UA','US','WN']] 
                fs = reduced_net['DAILY_FREQ'].tolist()
                f_hats = reduced_net['EST_FREQ'].tolist()
                overall_MAPE = sum([abs(f_hat-f) for f_hat,f in zip(f_hats,fs)])/sum(fs) 
                #calculate in market mape
                market_mapes = []
                for mkt_size in range(1,4):
                    reduced_net_sub = reduced_net.reset_index().set_index('MARKET_COMPETITORS').loc[mkt_size]
                    fs = reduced_net_sub['DAILY_FREQ'].tolist()
                    f_hats = reduced_net_sub['EST_FREQ'].tolist()
                    mkt_mape = sum([abs(f_hat-f) for f_hat,f in zip(f_hats,fs)])/sum(fs)
                    market_mapes.append(mkt_mape)
                
                resTABLE.loc[i,modification_factor]=str(overall_MAPE) + ';'+';'.join([str(mp) for mp in market_mapes])
            except OSError:
                print(input_fn)
    resTABLE.to_csv("exp_files/experimental_table_" + IO_base +".csv")#'exp_results_table.csv')
    return resTABLE
    
    
def experiment_categories_2(row):    
    #create list of double-hubs for carriers
    hub_sets = {'WN':['LAX','OAK','PHX','SAN','LAS'],'US':['LAS','PHX'],'UA':['LAX','SFO'],'AS':['SEA','LAX']}
    hub_groups = []    
    for carrier, hubs in hub_sets.items():
        pairs =[sorted([pair[0],pair[1]]) for pair in product(hubs,hubs) if pair[0]!=pair[1] ]
        txtpairs = list(set(["_".join(pair) for pair in pairs]))
        carrier_hubs = [carrier + '_' + txtpair for txtpair in txtpairs ]
        hub_groups += carrier_hubs       
            
    #first check if 3 player, assign to category
    if int(row['MARKET_COMPETITORS']) ==3:
        cat = 1
    elif row['UNIQUE_CARRIER']+'_' +row['BI_MARKET'] in hub_groups:
        cat = 2
    elif int(row['MARKET_COMPETITORS']) ==1:
        cat = 4
    else:
        cat = 3
    return cat
    
    
    

#create experimental carrier files with new F and premodified base    
def create_exp_files_modmkt():  
    
    coef_df = pd.read_csv('transcoef_table_mktDiv.csv',sep=';')
    #coef_df is in order of carriers/coefficient vectors used to create file already
    coef_df['category'] = coef_df.apply(experiment_categories_1,1)        
    #category_inds = [list(range(1,8)),list(range(1,7)),list(range(1,7)),list(range(1,4))]

    t100ranked_fn = "nonstop_competitive_markets.csv"
    t100ranked = pd.read_csv(t100ranked_fn)
    t100ranked_gb = t100ranked.groupby(['BI_MARKET','UNIQUE_CARRIER'])
   
    carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    file_ind = 0 #index for which file we are on
    #loop through each carrier-market category 
    coef_ind = 0 #coefficient increment, goes to 22
    with open('processed_data/mkt_mod_fns.txt','w') as fn_file:
        for market_carrier_combo in [(row['UNIQUE_CARRIER'],row['BI_MARKET']) for row in t100ranked.to_dict('records') if row['UNIQUE_CARRIER'] in ['AS','UA','US','WN']]:
            for sign,market_mod in enumerate([-500,500]):   
                fn_file.write('carrier_data_mktmod_MktDiv_500_%s_%s_%s.txt\n' % (market_carrier_combo[0],market_carrier_combo[1],sign))
                #read from base file, write to new outfile
                with open('carrier_data_mktDiv.txt','r') as basefile, open('matlab_2stagegames/carrier_data_mktmod_MktDiv_500_%s_%s_%s.txt' % (market_carrier_combo[0],market_carrier_combo[1],sign),'w') as outfile:
                    file_ind+=1 #increment file index
                    if file_ind % 50 == 0 :
                        print("FILE %s" % file_ind)
                    for i,line in enumerate(basefile):
                        if i<4: #first three lines just copy
                            outfile.write(line)
                        else: #make files
                            splitline = line.strip().split()
                            #for carrier in carriers_sorted: NO
                            carrier=carriers_sorted[i-4]                        
                            carrier_group = coef_df[coef_df['carrier']==carrier]
                            #full new coefficient vector
                            new_coefs = [] 
                            #modify coefficients
                            for coef_row in carrier_group.to_dict('records'):
                                #if category being modified, modify coefficients relevant to coef  number
                                if coef_row['bimarket']==market_carrier_combo[1] and coef_row['carrier']==market_carrier_combo[0]:
                                    coef_record = t100ranked_gb.get_group((coef_row['bimarket'],coef_row['carrier']))
                                    Cold = 10000
                                    Cnew = float(coef_record['FLIGHT_COST'])
                                    #old and new market sizes
                                    Mold = 1000                                
                                    Mnew = float(coef_record['MARKET_TOT'])                                
                                    Mnew += market_mod
                                    #frequency index of carrier in market to determine order of coefficients
                                    freq_ind = int(coef_record['MARKET_RANK'])
                                    if coef_row['competitors']==1:                                            
                                        base = [-95164.0447,-36238.3083,1148.0305]                                    
                                        transcoef = [-(Mnew/Mold)*base[0],(Mnew/Mold)*(Cold-base[1])-Cnew,-(Mnew/Mold)*base[2] ]
                                        
                                    elif coef_row['competitors']==2:                                            
                                        base = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]                                    
                                        transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,3)] + [-(Mnew/Mold)*base[3] if i==freq_ind else -(Mnew/Mold)*base[4] for i in range(1,3)] + [-(Mnew/Mold)*base[5]]
                                        
                                    elif coef_row['competitors']==3:
                                        base=[-150395.5496,-10106.6470,13135.9798,13136.1506,264.4822,-376.1793,-376.1781,270.2080,270.1927,-260.0113]
                                        
                                        transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,4)] + [-(Mnew/Mold)*base[4] if i==freq_ind else -(Mnew/Mold)*base[5] for i in range(1,4)]
                                        if freq_ind ==1:
                                            transcoef += [-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[9]]                                                
                                        elif freq_ind == 2:
                                            transcoef += [-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[7]]                                                
                                        elif freq_ind == 3:
                                            transcoef += [-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[7]]
                                        else:
                                            return 'freq ind error'                             
                                       
                                    else:
                                        return('MORE THAN THREE CARRIERS, MASSIVE ERROR')
                                    mod_coefs=transcoef
                                else:# keep the same if not the current category being modified
                                    mod_coefs =  ast.literal_eval(coef_row['coefs'])
                                if file_ind == 100:
                                    print(carrier)
                                    print('mf: %s, coef num: %s, category: %s, row_cat: %s, ind: %s, c: %s, M %s,' % (market_mod,50000,777,coef_row['category'],coef_ind,Cnew, Mnew))
                                  
                                    print(coef_row['coefs'])
                                    print(mod_coefs)
                               
                                #add potentially modified coefficients to new vector
                                new_coefs += mod_coefs
                            splitline[-1] = "["+",".join([str(num) for num in new_coefs])+"]"
                            newline = "\t".join(splitline) + "\n"
                            outfile.write(newline)
    return "done" 


def analyze_marketmod():
    market_table_fn= "nonstop_competitive_markets.csv"
    t100ranked = pd.read_csv(market_table_fn) 
    t100ranked['ind'] = t100ranked.index
    rows = []
    for market_carrier_combo in [[row['UNIQUE_CARRIER'],row['BI_MARKET'],row] for row in t100ranked.to_dict('records') if row['UNIQUE_CARRIER'] in ['AS','UA','US','WN']]:
         row=market_carrier_combo[2]
         index = int(row['ind'])
         low_fn='matlab_2stagegames/exp_results_mktmod_MktDiv%s_%s_%s.txt' % (market_carrier_combo[0],market_carrier_combo[1],0)
         low_df = pd.read_csv(low_fn)
         low_freq=float(low_df.iloc[index,2])
         high_fn='matlab_2stagegames/exp_results_mktmod_MktDiv%s_%s_%s.txt' % (market_carrier_combo[0],market_carrier_combo[1],1)
         high_df = pd.read_csv(high_fn)
         high_freq=float(high_df.iloc[index,2])
         increase = 1 if high_freq > low_freq else 0
         rows.append({'UNIQUE_CARRIER':row['UNIQUE_CARRIER'],'BI_MARKET':row['BI_MARKET'],'lowmkt':low_freq,'highmkt':high_freq,'increase':increase})
    mkt_df=pd.DataFrame(rows)
    mkt_df['diff']=mkt_df['highmkt']-mkt_df['lowmkt']
    mkt_div_results_unmod = pd.read_csv('net_results_basemod_WNmod_MktDiv13_0.0.txt')
    mkt_merge_table = pd.merge(mkt_df,mkt_div_results_unmod,on=['UNIQUE_CARRIER','BI_MARKET'])
    mkt_merge_table.to_csv('mkt_perturbation300_equilibrium_mktDiv.csv')

        
            
            
##extend to other player numbers, select player and markets by name , and coefficient modification
            #add modified market values

def test_marketsize_2player():
    test_carrier = 'WN'
    #2 PLAYER MARKETS FOR WN
    #markets=['LAS_PHX','LAS_SAN','LAS_SJC','LAS_SMF','OAK_PHX','OAK_SEA','ONT_PHX','PHX_SAN','PHX_SMF','SEA_SJC','SEA_SMF']
    #3 PLAYER MARKETS FOR WN
    markets=['LAS_LAX','LAS_PDX','LAS_SEA','LAS_SFO','PDX_PHX','PHX_SEA','SAN_SFO']
    labels_fn = 'term_mod_lin_3ps.txt'
    labels_mkt_fn = 'term_mods_lin3p.txt'
    outfile_prefix = 'r_mod_lin3'
    interaction_modifs=[-2+i*.1 for i in range(0,21)] #
    quadratic_modifs = [-2+i*.1 for i in range(0,21)]
    lin_modifs = [-1.5+i*.1 for i in range(0,31)]
    r_modifs=[1]#[0+i*.1 for i in range(0,11)];   
    markets_to_modify = [0]#[i for i, mk in enumerate(markets)]
    market_modifs = [0]#[-700+i*100 for i in range(0,16)]##[0]#when changed, make sure to modify file output name and output labels appropriately
    market_table_fn= "nonstop_competitive_markets.csv"
    t100ranked = pd.read_csv(market_table_fn)   
    t100ranked_mktgb=t100ranked.groupby('BI_MARKET')  
    
     
   
    with open('exp_files/%s' % labels_mkt_fn,'w') as outfile_mkt:
        for i,market_ind in enumerate(markets):
            print(market_ind)
            #get current market
            current_market = t100ranked_mktgb.get_group(market_ind)     
            player_of_interest =current_market[current_market['UNIQUE_CARRIER']==test_carrier]
            other_players = current_market[current_market['UNIQUE_CARRIER']!=test_carrier]
            if player_of_interest.shape[0] ==0:
                raise Exception('player %s not in market %s!' % (test_carrier, market_ind))
            numplayers =current_market.shape[0]
            Cold=10000;
            Mold = 1000;
            rows=[]    
            with open('matlab_2stagegames/%s' % labels_fn,'w') as outfile:  
                for market_number in markets_to_modify:
                    for market_mod in market_modifs:
                        for modif_interaction in interaction_modifs:       
                            for modif_quad in quadratic_modifs:
                                for modif_lin in lin_modifs:
                                    for r in r_modifs:                            
                                        if numplayers==2:
                                            base = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]    
                                            base[1] += base[1]*modif_lin                                             
                                            base[3] -= base[3]*modif_quad     
                                            base[5] -= base[5]*modif_interaction                    
                                            Cnew = player_of_interest['FLIGHT_COST']
                                            
                                            Mnew = max(float(player_of_interest['MARKET_TOT']) + market_mod,0) if i==market_number else float(player_of_interest['MARKET_TOT'])
                                            
                                            freq_ind = int(player_of_interest['MARKET_RANK'])
                                            
                                            #print((r*Mnew + (1-r)))
                                            transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,3)] + [-(1/Mold)*base[3]*(r*Mnew + (1-r)) if i==freq_ind else -(Mnew/Mold)*base[4] for i in range(1,3)] + [-(Mnew/Mold)*base[5]]
                                            transcoef = [float(tf) for tf in transcoef]                
                                            #get constraints for player of interest                
                                            F = float(player_of_interest['DAILY_FREQ']*player_of_interest['BACKFOURTH']/18 )
                                            bf = float(player_of_interest['BACKFOURTH'])
                                            #row is:  other player frequency, mmarket rank of carrier of interest,contstraint data for market (F and flight time data), number of players in market, and matlab vector of empirical frequencies in order of market rank
                                            row= [float(other_players['DAILY_FREQ']),freq_ind,F,bf] + transcoef + [numplayers,float(player_of_interest['DAILY_FREQ'])]  #list of emp. freqs: '['+' '.join(current_market['DAILY_FREQ'].apply(str).tolist())+']'
                                            rows.append(row)
                                            
                                        elif numplayers ==3:         #HOW ARE OTHER FREQS PASSED THROUGH
                                            #     intercept       f                 f'     f''       f^2                          f*f'   f*f''
                                            base=[-150395.5496,-10106.6470,13135.9798,13136.1506,264.4822,-376.1793,-376.1781,270.2080,270.1927,-260.0113]
                                            base[1] += base[1]*modif_lin                                             
                                            base[4] -= base[4]*modif_quad   
                                            base[7] -= base[7]*modif_interaction  
                                            base[8] -= base[8]*modif_interaction  
                                            Cnew = player_of_interest['FLIGHT_COST']
                                            
                                            Mnew = max(float(player_of_interest['MARKET_TOT']) + market_mod,0) if i==market_number else float(player_of_interest['MARKET_TOT'])
                                            
                                            freq_ind = int(player_of_interest['MARKET_RANK'])
                                            
                                            transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,4)] + [-(1/Mold)*base[4]*(r*Mnew + (1-r)) if i==freq_ind else -(Mnew/Mold)*base[5] for i in range(1,4)]
                                            if freq_ind ==1:
                                                transcoef += [-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[9]]                                                
                                            elif freq_ind == 2:
                                                transcoef += [-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[7]]                                                
                                            elif freq_ind == 3:
                                                transcoef += [-(Mnew/Mold)*base[9],-(Mnew/Mold)*base[7],-(Mnew/Mold)*base[7]]
                                            else:
                                                print('freq ind error')         
                                            transcoef = [float(tf) for tf in transcoef] 
                                            #get constraints for player of interest                
                                            F = float(player_of_interest['DAILY_FREQ']*player_of_interest['BACKFOURTH']/18 )
                                            bf = float(player_of_interest['BACKFOURTH'])
                                            #row is:  other player frequency, mmarket rank of carrier of interest,contstraint data for market (F and flight time data), number of players in market, and matlab vector of empirical frequencies in order of market rank
                                            row= other_players['DAILY_FREQ'].tolist() + [freq_ind,F,bf] + transcoef + [numplayers,float(player_of_interest['DAILY_FREQ'])]  #list of emp. freqs: '['+' '.join(current_market['DAILY_FREQ'].apply(str).tolist())+']'
                                            rows.append(row)
                                        if len(market_modifs) ==1:
                                            outfile.write(','.join([str(modif_interaction), str(modif_quad), str(modif_lin)]) + '\n') 
                                        elif i==market_number: 
                                            outfile_mkt.write(','.join([str(Mnew), str(market_mod),str(market_number), str(r)]) + '\n')
                                        
            df_out=pd.DataFrame(rows)
            df_out.to_csv('matlab_2stagegames/%s_%s.csv' % (outfile_prefix,market_ind))
        
    return 'done'
    
    





