# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 14:56:14 2015

@author: d29905p
"""

# -*- coding: utf-8 -*-
"""
Created on Sun May 17 23:07:54 2015

@author: Reed
"""
### CLEAN UP FUNCTIONS, TIMES GOOD SO MAYBE JUST FORGET AOTP? THEN DONT HAVE TO BANDAID A SOLUTION. SAME WITh SEATS, OR MAKE THAT AN OPTION
### IMPLEMENT SPSA and MYOICC BEST RESPONSE INP PYTHON 

### MAKE SURE ALL FILES STORED CORRECTLY, PERHAPS SEPERATE PROCESSED DATA AND CODE FILES
import os
import numpy as np
import pandas as pd
from itertools import product
import ast
import pickle

try: 
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
except NameError:
    os.chdir("C:/Users/d29905p/documents/airline_competition_paper/code/network_games")



##CLEAN UP TOP, SUPPRESS WARNINGS, GENERALIZE MAJOR CARRIERS, FUNTIONIZE NETWORK CONNECTION, COMPLETE DOWNLOADING PIPELINE, CLEAN UP ROUTE ESTIMATOR, ADD OTHER ONE FROM PAPER
    ### CLEAN UP AND CHEK CODE COMMENTARY
### WHY random MISCODING OF RANK?
#airport sets    
#HNL removed from ope35
major_carriers_2014 = ['DL','WN','UA','US','AA','B6','NK','AS','F9','VX']
ope35 = ['ATL', 'BOS', 'BWI', 'CLE', 'CLT', 'CVG', 'DCA', 'DEN', 'DFW', 'DTW', 'EWR', 'FLL', 'IAD', 'IAH', 'JFK', 'LAS', 'LAX', 'LGA', 'MCO', 'MDW', 'MEM', 'MIA', 'MSP', 'ORD', 'PDX', 'PHL', 'PHX', 'PIT', 'SAN', 'SEA', 'SFO', 'SLC', 'STL', 'TPA']
western= ['SEA','PDX','SFO','SAN','LAX','LAS','PHX','OAK','ONT','SMF','SJC']

def main_data_pipeline(year = 2014, quarters = [1], session_id="ope2014_2", parameter_file="parameters_default.txt", airport_network=ope35, major_carriers =major_carriers_2014):
    #parse parameters file, extract variables 
    variable_dict = parse_params(parameter_file,str_replacements={'%YEAR%':str(year),'%SESSION_ID%':session_id})
    
    #create t100ranked file and supplementary data files (primary data on carrier-segment combos)
    print("creating carrier-segment data files...")
    t100ranked = nonstop_market_profile(output_file = variable_dict['t100ranked_output_fn'],aotp_fn = variable_dict['aotp_fn'], quarters=variable_dict['quarters'], \
        t100_fn=variable_dict['t100_fn'],p52_fn=variable_dict['p52_fn'], t100_avgd_fn=variable_dict['t100_avgd_fn'], merge_HP=variable_dict['merge_HP'], \
        t100_summed_fn = variable_dict['t100_summed_fn'], t100_craft_avg_fn=variable_dict['t100_craft_avg_fn'],\
        ignore_mkts  = variable_dict['ignore_mkts'],\
        freq_cuttoff = variable_dict['freq_cuttoff'], ms_cuttoff=variable_dict['ms_cuttoff'], fs_cuttoff=variable_dict['fs_cuttoff'], only_big_carriers=variable_dict['only_big_carriers'], carriers_of_interest = variable_dict['carriers_of_interest'],airports = airport_network)
    
    #modify carrier-segment market sizes by considering connecting passengers
    print("modifing market sizes via connecting passenge..." ) 
    ###COME BACK TO THIS AFTER ROUTE DEMANDS CREATOR HAS BEEN FIXED
    ###ADDTIONALLY, DOWNLOAD BTS FILES, MARK NEEDED CATEGORIES, ETC
    t100ranked=get_market_connection_modifiers(major_carriers = major_carriers,  market_table_fn= variable_dict['t100ranked_output_fn'],outfile=variable_dict['t100ranked_mktmod_output_fn'],route_demands_fn = variable_dict['route_demands_fn'])
    print("estimating fleets available...")
    ###change t100 file when appropriate
    fleet_lookup=Ftable_new(output_fn=variable_dict['ftable_output_fn'], ac_lookup_dict = variable_dict['ac_lookup_dict'],include_regional_carriers=variable_dict['include_regional_carriers'],\
        use_lower_F_bound=variable_dict['use_lower_F_bound'], full_t00_fn=variable_dict['t100_fn'], ac_type_fn =variable_dict['ac_type_fn'],\
        t100summed_fn=variable_dict['t100_summed_fn'],market_table_fn= variable_dict['t100ranked_mktmod_output_fn'],\
        b43_fn = variable_dict['b43_fn'])
    #set up fleet assignment data 
    print("assigning fleets to markets...")
    fleet_dist=fleet_assign(major_carriers = major_carriers,output_fn=variable_dict['fleetdist_output_fn'], airtimes_fn_out=variable_dict['airtimes_fn_out'],\
        t100_summed_fn = variable_dict['t100_summed_fn'],fleet_lookup_fn = variable_dict['ftable_output_fn'],\
        market_table_fn=variable_dict['t100ranked_mktmod_output_fn'],\
        assign_min_score = variable_dict['assign_min_score'], assign_max_partitions =  variable_dict['assign_max_partitions'], assign_max_partition_set =  variable_dict['assign_max_partitions'])
    # creating game datatable GENERALIZE THE VARIABLES LATER!
    print("creating data tables for best response freqeuncy estimation...")
    coef_df  = create_network_game_datatable(outfile=variable_dict['network_game_output_fn'],coef_outfile=variable_dict['coef_outfile'],\
        use_adj_market=variable_dict['use_adj_market'],t100ranked_fn = variable_dict['t100ranked_mktmod_output_fn'],\
        fleet_lookup_fn = variable_dict['ftable_output_fn'],aotp_fn = variable_dict['aotp_fn'],\
        fleet_dist_fn=variable_dict['fleetdist_output_fn'])   
    # creating data table for SPSA optimization
    print("creating data tables for SPSA coefficient optimization...")
    t100sorted = create_SPSA_datamat(t100ranked_fn = variable_dict['t100ranked_mktmod_output_fn'],outfile_fn = variable_dict['SPSA_outfile_fn'])
    print("Done")
    #return list of DataFrames used
    return [t100ranked,fleet_lookup,fleet_dist,coef_df,t100sorted]
'''
STEP ONE: CREATE NETWORK PROFILE TABLE, MAJOR CARRIERS IN MAJOR MARKETS IN SELECT AIRPORTS

'''



    
            
'''
function to parse parameter file into variable dictionary, replacing %....% according to string replacements dict
'''
def parse_params(parameter_file,str_replacements):
    variable_dict = {}
    with open(parameter_file) as infile:
        for line in infile:
            if line.rstrip() and line[0]!='#': #ignore comments and empty lines        
                linesplit = line.split('=') #split on equals sign              
                var  = linesplit[0].strip()
                val  = linesplit[1].strip()
                #try lists
                try: 
                    val = ast.literal_eval(val)
                except (ValueError, SyntaxError):
                    #try numbers
                    try: 
                        val = float(val)
                    except ValueError:
                        #try bools 
                        if val in ['True','False']:
                            val  = True if val =='True' else False
                        else: #make string replacements
                            for key, value in str_replacements.items():
                                val = val.replace(key, value)
                #save in variable dict 
                variable_dict[var] = val
        return variable_dict
        
                                

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
def nonstop_market_profile(output_file = "processed_data/nonstop_competitive_markets_reg1_q1.csv",aotp_fn = 'bts_data/aotp_march.csv', quarters=[1], \
    t100_fn="bts_data/T100_2007.csv",p52_fn="bts_data/P52_2007.csv", t100_avgd_fn="processed_data/t100_avgd_reg1_q1.csv", merge_HP=True, \
    t100_summed_fn = 'processed_data/t100_summed_reg1_q1.csv', t100_craft_avg_fn='processed_data/t100_craft_avg_reg1_q1.csv',\
    ignore_mkts = ['PDX_SJC','PDX_SFO','OAK_PDX'],\
    freq_cuttoff = .5, ms_cuttoff=.1, fs_cuttoff = .1, only_big_carriers=False, carriers_of_interest = ['AS','UA','US','WN'],airports = ['SEA','PDX','SFO','SAN','LAX','LAS','PHX','OAK','ONT','SMF','SJC']):
        
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
    
    #average quarterly costs if necessary 
    if len(quarters) > 1:
        expenses_by_type=relevant_p52[['AIRCRAFT_TYPE','UNIQUE_CARRIER','TOT_AIR_OP_EXPENSES', 'TOTAL_AIR_HOURS']].groupby(['AIRCRAFT_TYPE','UNIQUE_CARRIER']).aggregate(np.sum).reset_index().dropna()
    else:
        expenses_by_type = relevant_p52[['AIRCRAFT_TYPE','UNIQUE_CARRIER','TOT_AIR_OP_EXPENSES', 'TOTAL_AIR_HOURS']].dropna()   
    #calculate expenses per air hour for each type for each airline
    expenses_by_type['EXP_PER_HOUR'] = expenses_by_type['TOT_AIR_OP_EXPENSES'] / expenses_by_type['TOTAL_AIR_HOURS']

    #average relevant monthly frequency to get daily freqencies
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
        new_group = market_rank(market_group, ms_cuttoff=ms_cuttoff,fs_cuttoff=fs_cuttoff)
        grouplist.append(new_group)
    t100ranked = pd.concat(grouplist,axis=0)
    t100ranked=t100ranked.sort(columns=['BI_MARKET','MARKET_RANK'])    
    t100ranked['BACKFOURTH'] = 2*(t100ranked['FLIGHT_TIME']+45/60)
    #remove markets more with more than 3 competitors
    
    original_count = t100ranked.shape[0]
    t100ranked = t100ranked[t100ranked['MARKET_COMPETITORS']<4]
    new_count = t100ranked.shape[0]
    print('removed %s markets with more than 3 competitors, out of %s in total' % (original_count - new_count, original_count))
    
    #save t100ranked to file
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
def market_rank(gb, ms_cuttoff,fs_cuttoff):                                  
    Mtot = gb['PASSENGERS'].sum()
    Ftot =gb['DAILY_FREQ'].sum()
    gb['FREQ_TOT'] = np.repeat(Ftot,gb.shape[0] )   
    gb['MARKET_TOT'] = np.repeat(Mtot,gb.shape[0] )    
    Mcount =gb.shape[0]
    gb['MARKET_COMPETITORS'] = np.repeat(Mcount,gb.shape[0] )
    rank = np.array(gb['PASSENGERS'].tolist()).argsort()[::-1].argsort() +1 
    gb['MARKET_RANK'] = rank         
    gb = gb.sort(columns=['MARKET_RANK'],ascending=True,axis =0)        
    gb['MS_TOT']=gb['PASSENGERS']/gb['MARKET_TOT']
    gb['FS_TOT']=gb['DAILY_FREQ']/gb['FREQ_TOT']
    #cumulative market share upto and including that ranking
    gb['CUM_MS']=gb.apply(lambda x: gb['MS_TOT'][:x['MARKET_RANK']].sum(), axis=1)
    #cumulative market share upto that ranking
    gb['PREV_CUM_MS']=gb.apply(lambda x: gb['MS_TOT'][:x['MARKET_RANK']-1].sum(), axis=1)
    #remove those carriers that appear after cuttoff
    gb=gb[gb['MS_TOT']>=ms_cuttoff]
    gb=gb[gb['FS_TOT']>=fs_cuttoff]
    #recalculate market shares
    Mtot = gb['PASSENGERS'].sum()
    Ftot =gb['DAILY_FREQ'].sum()
    #get total market size
    gb['MARKET_TOT'] = np.repeat(Mtot,gb.shape[0] )   
    gb['FREQ_TOT'] = np.repeat(Ftot,gb.shape[0] )  
    #get total number of competitors in market and save as column 
    Mcount =gb.shape[0]
    gb['MARKET_COMPETITORS'] = np.repeat(Mcount,gb.shape[0] )
    #get market share as passengers for that carrier over total market size 
    gb['MS_TOT']=gb['PASSENGERS']/gb['MARKET_TOT']
    gb['FS_TOT']=gb['DAILY_FREQ']/gb['FREQ_TOT']
    return gb    


 #function to add modifiers to market size for each market/carier combo based on fraction of connecting passengers 
def get_market_connection_modifiers( major_carriers = ['AS','UA','US','WN'], market_table_fn= "processed_data/nonstop_competitive_markets_reg1_q1.csv",outfile='processed_data/nonstop_competitive_markets_mktmod_reg1_q1.csv',route_demands_fn = 'bts_data/route_demand_Q1.csv'):   
    t100ranked = pd.read_csv(market_table_fn)   
    t100ranked['CARRIER_MARKET'] = t100ranked.apply(lambda row: row['UNIQUE_CARRIER'] + '_' + row['BI_MARKET'],1)
    t100ranked['connection_demands'] = np.zeros((t100ranked.shape[0],1))
    t100ranked = t100ranked.set_index('CARRIER_MARKET')
    markets_sorted = sorted(list(set(t100ranked['BI_MARKET'].tolist())))   
    nonstop_numbers = {mkt:0 for mkt in markets_sorted}
    total_numbers={mkt:0 for mkt in markets_sorted}
    ###carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    market_dict={mkt: list(set(t100ranked.groupby('BI_MARKET').get_group(mkt)['UNIQUE_CARRIER'].tolist()).intersection(major_carriers)) for mkt in markets_sorted}
    route_demands = pd.read_csv(route_demands_fn)
    def create_market(row):
        market = [row['ORIGIN'], row['DESTINATION']]
        market.sort()
        return "_".join(market)
    for i,line in enumerate(route_demands.to_dict('records')):       
        if i%100000 ==0:
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
        if row['UNIQUE_CARRIER'] in major_carriers:
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
def Ftable_new(output_fn="processed_data/fleet_lookup_reg1_q1.csv", ac_lookup_dict ='bts_data/ac_lookup_dict.pickle',include_regional_carriers=True, use_lower_F_bound=True, full_t00_fn="bts_data/t100_seg_all.csv", ac_type_fn ="bts_data/AIRCRAFT_TYPE_LOOKUP.csv",t100summed_fn="processed_data/t100_summed_reg1_q1.csv",market_table_fn= "processed_data/nonstop_competitive_markets_reg1_q1.csv",b43_fn = "bts_data/SCHEDULE_B43.csv"):
    #load domestic and international T100 records
    t100_all = pd.read_csv(full_t00_fn)
    #load inventory and aircraft data
    b43 = pd.read_csv(b43_fn)
    ####type1 = pd.read_csv(ac_type_fn)
    conversion_dict = pickle.load(open(ac_lookup_dict,'rb'))
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
    
    #average freqs for market-carrier-ac_Tye    
    freqs_across_markets_carrier_ac_type = t100_summed[['UNIQUE_CARRIER','BI_MARKET','DAILY_FREQ','AIRCRAFT_TYPE']].groupby(['UNIQUE_CARRIER','BI_MARKET','AIRCRAFT_TYPE']).aggregate(np.mean).reset_index().groupby(['UNIQUE_CARRIER','AIRCRAFT_TYPE'])
    # construct F tabke
    rows=[]
    for airline in carriers_sorted:
        #get relevant aircraft types for this carrier
        ac_types = list(set(t100_gb.get_group(airline)['AIRCRAFT_TYPE'].tolist()))
        for ac_type in ac_types:
            ac_type = int(ac_type)
            row={}            
            row['carrier'] = airline
            row['aircraft_type']=ac_type
            #get time in network ratio for this craft computed above
            carrier_type = tr_gb.get_group((row['carrier'],row['aircraft_type']))
            rat=carrier_type['TIME_IN_NETWORK'].sum()/carrier_type['AIR_TIME'].sum()            
            #b43 model code
            ##model = reduced_type.loc[ac_type]['model']
            ##print(model) #should be a lists
            model =conversion_dict[ac_type] 
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
            F_lower_bound = sum([2*f[1]*(45/60+carrier_df.loc[f[0]]['FLIGHT_TIME']) for f in freqs_by_market if f[0] in carrier_df.index ])/18
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
    return fleet_lookup



##alternate fleet assigner via overlapping graph method
def create_fleet_assignments_graphwise(fleet_dist,fleet_lookup, major_carriers, min_score = .95):
    
    #function to find all connected subgraphs in graph
    def connected_components(neighbors):
        components = []
        seen = set()
        def component(node):
            subgraph = []
            nodes = set([node])
            while nodes:
                node = nodes.pop()
                seen.add(node)
                nodes |= neighbors[node] - seen
                subgraph.append(node)
            return list(set(subgraph))
        for node in neighbors:
            if node not in seen:
                components.append(component(node)) 
        return components

    # create graph from

## helper function to create fleet assignments, based on heuristic algorithm
#ADD COMMENTS! SEND PARAMETERS TO OUTER FUNCTION  AND THEN TO PARAMETER FILE!
#ONLY COMPUTE AS MANY PARTITIONS AS NECESSARY! # SO CHECK HOW PARTITION FUNCTIO WORKS
#PARTITION AS A GENERATOR! THOUGH MODIFY CODE SO IN ORDER OF NUMBER OF PARTITIONS PERHAPS
def create_fleet_assignments(fleet_dist, fleet_lookup, major_carriers, min_score = .95, max_partitions = 3, max_partition_set = 11):
    
    carriers = sorted(list(set(fleet_dist['carrier'].tolist())))
    
    
    assignments_full = []

    def partition(collection):
        if len(collection) == 1:
            yield [ collection ]
            return
    
        first = collection[0]
        for smaller in partition(collection[1:]):
            # insert `first` in each of the subpartition's subsets
            for n, subset in enumerate(smaller):
                yield smaller[:n] + [[ first ] + subset]  + smaller[n+1:]
            # put `first` in its own subset 
            yield [ [ first ] ] + smaller
        

    fd_gb = fleet_dist.groupby('carrier')
    
    fl_gb  =fleet_lookup.groupby('carrier')
    
    for CARRIER in carriers:
        group = fd_gb.get_group(CARRIER)
        group_fleet = fl_gb.get_group(CARRIER)['aircraft_type'].tolist()
        if CARRIER in major_carriers: 
            print(CARRIER)                
            fleet_list = [actype for actype in group_fleet]
            if len(fleet_list) <= max_partition_set:
                part = list(partition([actype for actype in group_fleet]))
                i = 0
                count = 0
                good_partitions = []
                good_partition_mean_scores = []
                good_partition_lenths = []
                while i<len(part):
                    if len(part[i])>max_partitions:
                        i+=1        
                    else: 
                        potential_partition = True
                        
                        count+=1
                        if count % 1000 == 0:
                            print(count)
                        seg_partition_assignments = []
                        seg_partition_scores = []
                        for seg_ind in range(0,group.shape[0]):            
                            max_score = 0
                            try:
                                seg_dict = ast.literal_eval(group.iloc[seg_ind]['type_dict_seats_f_pf_F'])
                            except ValueError:
                                seg_dict=group.iloc[seg_ind]['type_dict_seats_f_pf_F']
                            for p in part[i]:
                                score = sum([seg_dict[plane][2] for plane in p if plane in seg_dict.keys() ])
                                if score > max_score:
                                    max_score = score
                                    max_partition = p
                            if max_score < min_score:
                                potential_partition = False
                                break
                            else: 
                                seg_partition_assignments.append(max_partition)
                                
                        if potential_partition == True:
                            good_partitions.append(seg_partition_assignments)
                            good_partition_mean_scores.append(np.mean(seg_partition_scores))
                            good_partition_lenths.append(len(p))
                        i+=1   
                #get max length partitions satisfying criteria
                max_len = max(good_partition_lenths)
                max_inds=[i for i, j in enumerate(good_partition_lenths) if j == max_len]
                if len(max_inds)==1:
                    assignment = good_partitions[0]
                else: 
                    max_score = 0    
                    for ind in max_inds:
                        if good_partition_mean_scores[ind] > max_score:
                            max_score =good_partition_mean_scores[ind]
                            final_ind = ind
                    assignment = good_partitions[final_ind]
               
                
                assignments_full = assignments_full + assignment
            else:
                assignments_full = assignments_full  + [fleet_list for i in range(0,group.shape[0])]
                
        else: 
            assigns = group['max_type'].tolist()
            assigns = [[a] for a in assigns]
            assignments_full = assignments_full + assigns
        
        
    return ["-".join([str(o) for o in a]) for a in assignments_full]    
        
    


    
'''
function to find most common type of plane used on each segment
and to get a fleet composition for network for each carrier

'''  
def fleet_assign(major_carriers = ['AS','UA','US','WN'], output_fn="processed_data/fleetdist_reg1_q1.csv", \
    airtimes_fn_out='processed_data/airtimes_reg1_q1.csv',t100_summed_fn = 'processed_data/t100_summed_reg1_q1.csv',\
    fleet_lookup_fn = "processed_data/fleet_lookup_reg1_q1.csv",market_table_fn= "processed_data/nonstop_competitive_markets_reg1_q1.csv",\
    assign_min_score = .95, assign_max_partitions = 3, assign_max_partition_set = 11):
        
    #load network data file created by nonstop_market_profile function
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
    #if no seat information available, assume 0 seats
    fleet_lookup = fleet_lookup.fillna(0)  # FOR NOW, WHEN SEAT NUMBERS ACTUALLY MATTER FIL WITH T100 AVERAGES LIKE THUS:  t100_summed.groupby(['UNIQUE_CARRIER','AIRCRAFT_TYPE']).get_group(('US',617))
    #THEN, CREATE SYNTHETIC CARRIERS, WITH SIZES THE WEIGHTED AVERAGE (BY FREQ SHARE) OF ALL, PERHAPS INDIVIDUALIZED FOR EACH MARKET!!! MAYBE    
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
        ##for craft in craftlist:
          ##  round(float(fleet_lookup_gb.get_group((carrier,int(craft)))['craft_seats']
        craft_seats=[]
        for craft in craftlist:
            try:
                craft_seats.append((int(craft),round(float(fleet_lookup_gb.get_group((carrier,int(craft)))['craft_seats']))))
            #if craft from T100 is actually not in inventory, ASSUME ZERO SEATS
            except ValueError:
                craft_seats.append((int(craft),0))
        row['type_list'] = craft_seats
        rows.append(row)
        #construct data frame (MAY WISH TO PRESENT DATA IN A WAY THAT MAKES FOR EASIER ASsIGNEMENT  DECISIONS)
    fleet_dist = pd.DataFrame(rows)   
    fleet_dist = fleet_dist.sort(columns=['carrier','bimarket'])
    
    fleet_dist['assigned_type'] = create_fleet_assignments(fleet_dist, fleet_lookup, major_carriers, min_score = assign_min_score, max_partitions = assign_max_partitions, max_partition_set = assign_max_partition_set)
    
    
    fleet_dist.to_csv(output_fn, sep=';')    
    airtime_merge=pd.concat(air_times)
    airtime_merge.to_csv(airtimes_fn_out)
    return fleet_dist



    
        

'''
function to create table of carrier and market data used by matlab myopic best response network game
#NOTE:CREATE AND OFFICIAL INDEX OF CARRIER MARKET COMBO FOR EASY MAPPING
if use_adj_market is True, market sizes will be adjusted to account for nonstop passengers

'''    

def create_network_game_datatable(outfile='processed_data/carrier_data_reg1_q1.txt',coef_outfile='processed_data/transcoef_table_reg1_q1.csv', use_adj_market=True,t100ranked_fn = 'processed_data/nonstop_competitive_markets_mktmod_reg1_q1.csv', fleet_lookup_fn = "processed_data/fleet_lookup_reg1_q1.csv",aotp_fn = 'bts_data/aotp_march.csv',fleet_dist_fn='processed_data/fleet_dist_aug_reg1_q1.csv'):   
    #read in data files     
    fleet_lookup= pd.read_csv(fleet_lookup_fn)
    aug_fleet = pd.read_csv(fleet_dist_fn, sep=';') 
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
                            except KeyError: #if ONT time can't be found, approximate with LAX  GENERALIzE WITH TIMES FROM T!10000000
                                ###mkk=mk.replace('ONT','LAX') 
                                block_hours=2  #VERY APPROXIMATE!!  
                        a_row.append(2*(block_hours +45/60))
                    #otherwise, no constraint for this market
                    else:
                        a_row.append(0)                    
                A_rows.append(a_row)
                #sum F accross compents of hybrid carrier
                try:
                    F = sum([fleet_lookup.groupby(['carrier','aircraft_type']).get_group((carrier,int(float(subtype))))['F'].iloc[0] for subtype in ac_type.split('-') ])
                except KeyError:
                    F = sum([fleet_lookup.groupby(['carrier','aircraft_type']).get_group((carrier,subtype))['F'].iloc[0] for subtype in ac_type.split('-') ])
                b_rows.append(18*F)
            #index of relevant markets for each  carrier 
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


            
            
            
#create datamat to be run with SPSA
def create_SPSA_datamat(t100ranked_fn = "processed_data/nonstop_competitive_markets_mktmod_reg1_q1.csv",outfile_fn = "processed_data/SPSAdatamat_mktmod_reg1_q1.csv"):             
     t100ranked = pd.read_csv(t100ranked_fn)
     
     data_mat = t100ranked
     data_mat['category']=data_mat.apply(experiment_categories_2,1)
     def cat_start_end(row):
         if row['category']==1:
             return pd.Series({'basestart':1,'baseend':10})
         elif row['category']==2:
             return pd.Series({'basestart':11,'baseend':16})
         elif row['category']==3:
             return pd.Series({'basestart':17,'baseend':22})
         elif row['category']==4:
             return pd.Series({'basestart':23,'baseend':25})
     data_mat[['baseend','basestart']]=data_mat.apply(cat_start_end,1).sort(axis=1) #????? WHAT. WHAT AM I WHATING ABOUT 
     def transcoef_ind(gb):
         gb = gb.sort('BI_MARKET',axis=0)
         rows=[]
         start_index = 1
         for row in gb.to_dict('records'):
             if row['MARKET_COMPETITORS']==3:
                 end_index = start_index + 10 -1
             elif row['MARKET_COMPETITORS']==2:
                 end_index = start_index + 6 -1
             elif row['MARKET_COMPETITORS']==1:
                 end_index = start_index + 3 -1
             else:
                 raise('Error!')
             
             row['transstart']=start_index
             row['transend']=end_index
             start_index = end_index + 1
             rows.append(row)         
         return pd.DataFrame(rows)     
     data_mat=data_mat.groupby('UNIQUE_CARRIER').apply(transcoef_ind)
     t100ranked_sort = data_mat.sort(['UNIQUE_CARRIER','BI_MARKET'],axis=0)[['MARKET_COMPETITORS','MARKET_RANK','new_market','FLIGHT_COST','basestart','baseend','transstart','transend']]
     t100ranked_sort.to_csv(outfile_fn)
     return t100ranked_sort
    
    


'''
function to build easily read data table from MATLAB output
'''
def create_results_table(outfile_fn='exp_files/SPSA_results_table_ope2014.csv',input_fn = "exp_files/SPSA_results_fulleq_MAPE_ope2014.csv",t100ranked_fn = "processed_data/nonstop_competitive_markets_mktmod_ope2014.csv"):
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





# function to assign experimental categories

def experiment_categories_2(row):    
    #create list of double-hubs for carriers
    hub_sets = {'F9': ['DEN', 'DFW'], 'VX': ['LAX', 'SFO', 'DCA', 'MCO'], 'WN': ['MDW', 'BWI', 'DEN', 'LAS', 'PHX', 'STL', 'ATL', 'MCO', 'TPA', 'LAX', 'SAN'], 'AS': ['SEA', 'PDX', 'ORD', 'DFW', 'LAX', 'SAN', 'LAS', 'ATL', 'MSP', 'SFO', 'DCA', 'DEN', 'SLC'], 'UA': ['ORD', 'DEN', 'IAH', 'IAD', 'EWR', 'SFO', 'LAX', 'CLE', 'MCO'], 'US': ['CLT', 'PHL', 'PHX', 'DCA', 'DFW', 'ORD', 'DEN', 'LAX'], 'DL': ['ATL', 'MSP', 'DTW', 'SLC', 'JFK', 'LGA', 'LAX', 'CVG'], 'AA': ['DFW', 'ORD', 'MIA', 'LAX', 'CLT', 'JFK', 'DCA', 'PHX', 'PHL'], 'NK': ['FLL', 'DFW', 'LAS', 'ORD', 'MSP'], 'B6': ['JFK', 'BOS', 'MCO', 'FLL', 'TPA']}
    hub_groups = []    
    for carrier, hubs in hub_sets.items():
        pairs =[sorted([pair[0],pair[1]]) for pair in product(hubs,hubs) if pair[0]!=pair[1] ]
        txtpairs = list(set(["_".join(pair) for pair in pairs]))
        carrier_hubs = [carrier + '_' + txtpair for txtpair in txtpairs ]
        hub_groups += carrier_hubs       
            
    #first check if 3 player, assign to category
    if int(row['MARKET_COMPETITORS']) ==3:
        cat = 1
    elif row['UNIQUE_CARRIER']+'_' +row['BI_MARKET'] in hub_groups and int(row['MARKET_COMPETITORS']) ==2:
        cat = 2
    elif int(row['MARKET_COMPETITORS']) ==1:
        cat = 4
    else:
        cat = 3
    return cat
    

