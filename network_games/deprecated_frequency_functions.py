# -*- coding: utf-8 -*-
"""
Created on Fri Aug 28 13:32:24 2015

@author: d29905p
"""
import os
import numpy as np
import pandas as pd
from itertools import product, combinations
import ast
os.chdir("C:/Users/d29905p/documents/airline_competition_paper/code/network_games")
'''
function to compute proportion airtime of assigned craft or craft hybrids in each market within each carrier, and then compute "sub F" values broken down by segment from orignal F value 
DEPRECATED : better way to do unit tests exists
'''
  
  
def compute_subF(new_aug_fleet_fn = "processed_data/fleet_dist_minigames.csv", fleet_dist_aug_fn='processed_data/fleet_dist_aug.csv',airtimes_df_fn='processed_data/airtimes.csv',fleet_lookup_fn = "processed_data/fleet_lookup.csv"):
    #load files
    aug_fleet = pd.read_csv(fleet_dist_aug_fn)
    airtimes_gb=pd.read_csv(airtimes_df_fn).groupby(['UNIQUE_CARRIER','BI_MARKET'])
    fleet_lookup_gb= pd.read_csv(fleet_lookup_fn).groupby(['carrier'])
    #loop through lines of augmented fleet file, get total times for aircraft, hybrid or otherwise
    aug_fleet['airhours']=aug_fleet.apply(lambda row:airtimes_gb.get_group((row['carrier'],row['bimarket'])).set_index('AIRCRAFT_TYPE').loc[[int(craft) for craft in row['assigned_type'].split('-')]]['AIR_HOURS'].sum(), axis=1)
    #function to sum times across markets and get proportion and sub F for each market    
    def subF(carrier_type_grp):    
        total_airhours=carrier_type_grp['airhours'].sum()
        #proportion air hours in each market
        carrier_type_grp['airhours_rat'] =carrier_type_grp['airhours']/total_airhours
        carrier_type_grp['subF'] = carrier_type_grp.apply(lambda row: fleet_lookup_gb.get_group(row['carrier']).set_index('aircraft_type').loc[[int(craft) for craft in row['assigned_type'].split('-')]]['F'].sum()*row['airhours_rat'], axis=1)
        return carrier_type_grp
    #apply to original data frame, adding appropriate columns     
    aug_fleet_aug = aug_fleet.groupby(['carrier','assigned_type']).apply(subF)
    aug_fleet_aug.to_csv(new_aug_fleet_fn, sep=';')
    return aug_fleet
    

#DEPRECATED    , old method of coefficient modification
def create_exp_files():  
    coef_df = pd.read_csv('transcoef_table.csv',sep=';')
    #coef_df is in order of carriers/coefficient vectors used to create file already
    coef_df['category'] = coef_df.apply(experiment_categories_1,1)        
    category_inds = [list(range(1,8)),list(range(1,7)),list(range(1,7)),list(range(1,4))]

    t100ranked_fn = "nonstop_competitive_markets.csv"
    t100ranked = pd.read_csv(t100ranked_fn)
    carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    file_ind = 0 #index for which file we are on
    #loop through each carrier-market category 
    coef_ind = 0 #coefficient increment, goes to 22
    for category in range(1,5):       
        for coef_number in category_inds[category-1]: #which coefficient to modify
            coef_ind += 1
            #how much to modify it by
            for modification_factor in [round(-1+.1*i,1) for i in range(0,21)][0:5] +[round(-1+.1*i,1) for i in range(0,21)][16:21]:
 #[round(-.5+.1*i,1) for i in range(0,11)]:
                #read from base file, write to new outfile
                with open('carrier_data.txt','r') as basefile, open('carrier_data_%s_%s.txt' % (str(coef_ind),str(modification_factor)),'w') as outfile:
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
                                if coef_row['category']==category:
                                    if category == 4: #if just 1 carrier, modify appropriate coefficient
                                        mod_coefs = ast.literal_eval(coef_row['coefs'])
                                        mod_coefs[coef_number-1] +=  modification_factor*mod_coefs[coef_number-1]
                                    else: #modify coefficients that match current coefficient number
                                        mod_coefs = ast.literal_eval(coef_row['coefs'])
                                        coef_cats = ast.literal_eval(coef_row['coef_cats'])
                                        mod_coefs = [(B + modification_factor*B if ind==coef_number else B) for B,ind in zip(mod_coefs,coef_cats )]
                                else:# keep the same if not the current category being modified
                                    mod_coefs =  ast.literal_eval(coef_row['coefs'])
                                if file_ind == 100:
                                    print(carrier)
                                    print('mf: %s, coef num: %s, category: %s' % (modification_factor,coef_number,category))
                                    print(coef_cats)
                                    print(coef_row['coefs'])
                                    print(mod_coefs)
                                #add potentially modified coefficients to new vector
                                new_coefs += mod_coefs
                            splitline[-1] = "["+",".join([str(num) for num in new_coefs])+"]"
                            newline = "\t".join(splitline) + "\n"
                            outfile.write(newline)
    return "done" 
    
    
    
#DEPRECATED, more efficent ways to do combo testing
#function for modifying more than 1 coefficient at once, based on optimal modification factors found in other experiments
def create_exp_combo_files(file_names_file = 'combo_file_names.txt',optimal_noWN = [[2,-.2],[4,1.3],[6,.6],[15,-.2],[17,.8],[19,.3]]):
    coef_df = pd.read_csv('transcoef_table.csv',sep=';')
    #optimal non WN coefficient/modification factor combos, for use in combo search    
    combo_list=[]
    for k in range(2,7):
        combo_list += list(combinations(optimal_noWN,k))            
    t100ranked_fn = "nonstop_competitive_markets.csv"
    t100ranked = pd.read_csv(t100ranked_fn)
    carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    with open(file_names_file,'w') as file_file:
        for combo in combo_list:            
            outfile_name = 'matlab_2stagegames/carrier_data_combo__' + '_'.join([str(coef[0]) + '_' +str(coef[1]) for coef in combo]) + '.txt' 
            file_file.write(outfile_name + '\n')
            with open('carrier_data.txt','r') as basefile, open(outfile_name,'w') as outfile:
                #coefficients to modify
                coefs_mods = {coef[0]:coef[1] for coef in combo}      
                ##CONTINUE MULTIPLE ADJUSTMETNS HERE
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
                            if coef_row['category']==category:
                                if category in [4,8]: #if just 1 carrier, modify appropriate coefficient
                                    mod_coefs = ast.literal_eval(coef_row['coefs'])
                                    mod_coefs[coef_number-1] +=  modification_factor*mod_coefs[coef_number-1]
                                else: #modify coefficients that match current coefficient number
                                    mod_coefs = ast.literal_eval(coef_row['coefs'])
                                    coef_cats = ast.literal_eval(coef_row['coef_cats'])
                                    mod_coefs = [(B + modification_factor*B if ind==coef_number else B) for B,ind in zip(mod_coefs,coef_cats )]
                            else:# keep the same if not the current category being modified
                                mod_coefs =  ast.literal_eval(coef_row['coefs'])
                            if file_ind == 100:
                                print(carrier)
                                print('mf: %s, coef num: %s, category: %s' % (modification_factor,coef_number,category))
                                print(coef_cats)
                                print(coef_row['coefs'])
                                print(mod_coefs)
                            #add potentially modified coefficients to new vector
                            new_coefs += mod_coefs
                        splitline[-1] = "["+",".join([str(num) for num in new_coefs])+"]"
                        newline = "\t".join(splitline) + "\n"
                        outfile.write(newline)
                        

#DEPRECATED     , an older method of modifying only WN       
def create_exp_files_WN():      
    coef_df = pd.read_csv('transcoef_table.csv',sep=';')
    #coef_df is in order of carriers/coefficient vectors used to create file already
    coef_df['category'] = coef_df.apply(experiment_categories_WN,1)        
    category_inds = [list(range(1,8)),list(range(1,7)),list(range(1,7)),list(range(1,4)),list(range(1,8)),list(range(1,7)),list(range(1,7)),list(range(1,4))]

    t100ranked_fn = "nonstop_competitive_markets.csv"
    t100ranked = pd.read_csv(t100ranked_fn)
    carriers_sorted = sorted(list(set(t100ranked['UNIQUE_CARRIER'].tolist())))
    file_ind = 0 #index for which file we are on
    #loop through each carrier-market category 
    coef_ind = 0 #coefficient increment, goes to 44
    for category in range(1,9):       
        for coef_number in category_inds[category-1]: #which coefficient to modify
            coef_ind += 1
            if coef_ind in [2,4,6,15,17,19,21,22]: #if in coefficients of interest for either WN or non WN...
                #how much to modify it by
                for modification_factor in [round(-1+.1*i,1) for i in range(0,26)]:  #[0:5] +[round(-1+.1*i,1) for i in range(0,21)][16:21]:
     #[round(-.5+.1*i,1) for i in range(0,11)]:
                    #read from base file, write to new outfile
                    with open('carrier_data.txt','r') as basefile, open('matlab_2stagegames/carrier_data_WN_%s_%s.txt' % (str(coef_ind),str(modification_factor)),'w') as outfile:
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
                                    if coef_row['category']==category:
                                        if category in [4,8]: #if just 1 carrier, modify appropriate coefficient
                                            mod_coefs = ast.literal_eval(coef_row['coefs'])
                                            mod_coefs[coef_number-1] +=  modification_factor*mod_coefs[coef_number-1]
                                        else: #modify coefficients that match current coefficient number
                                            mod_coefs = ast.literal_eval(coef_row['coefs'])
                                            coef_cats = ast.literal_eval(coef_row['coef_cats'])
                                            mod_coefs = [(B + modification_factor*B if ind==coef_number else B) for B,ind in zip(mod_coefs,coef_cats )]
                                    else:# keep the same if not the current category being modified
                                        mod_coefs =  ast.literal_eval(coef_row['coefs'])
                                    if file_ind == 100:
                                        print(carrier)
                                        print('mf: %s, coef num: %s, category: %s' % (modification_factor,coef_number,category))
                                        print(coef_cats)
                                        print(coef_row['coefs'])
                                        print(mod_coefs)
                                    #add potentially modified coefficients to new vector
                                    new_coefs += mod_coefs
                                splitline[-1] = "["+",".join([str(num) for num in new_coefs])+"]"
                                newline = "\t".join(splitline) + "\n"
                                outfile.write(newline)
    return "done"                            
    
    
    
    
# DEPRECATED, old method for combining MAPES
def weighted_results_table(outfile_fn='experimental_table_comp_unbounded2.csv',t100ranked_fn = "nonstop_competitive_markets.csv"):
    resTABLEs = [0,0,0,0]    
    airlines = ['AS','UA','US','WN']
    for airline in [1,2,3,4]:
        resTABLEs[airline-1] = pd.DataFrame(index=[2, 4, 6, 9, 11, 13, 15, 17, 19, 21, 22],columns=[round(-1+.1*j,1) for j in range(0,21)])    
        for i in [2, 4, 6, 9, 11, 13, 15, 17, 19, 21, 22]:        
            for modification_factor in [round(-1+.1*j,1) for j in range(0,21)]:#[round(-.5,1),round(-.2,1),round(.2,1),round(.5,1)]:#[round(-.5+.1*j,1) for j in range(0,11)]:
                input_fn = "matlab_2stagegames/exp_results_fixed_nobounds_WNsep%s_%s_%s.txt" % (airline, i,modification_factor)
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
                    #network_results.to_csv('net_results_WNbouned%s_%s.txt' % (i,modification_factor))
                    #calculate overall MAPE                       
                    reduced_net = network_results.set_index('UNIQUE_CARRIER').loc[[airlines[airline-1]]] 
                    fs = reduced_net['DAILY_FREQ'].tolist()
                    f_hats = reduced_net['EST_FREQ'].tolist()
                    overall_MAPE = sum([abs(f_hat-f) for f_hat,f in zip(f_hats,fs)])/sum(fs)            
                    resTABLEs[airline-1].loc[i,modification_factor]=overall_MAPE
                except OSError:
                    print(input_fn)
    full_restable = pd.DataFrame(index=[2,4,6,15,17,19,24,26,28,37,39,41],columns=[round(-1+.1*j,1) for j in range(0,21)])
    
    #resTABLE.to_csv(outfile_fn)#'exp_results_table.csv')
    for i in [2,4,6,15,17,19,24,26,28,37,39,41]:        
        for modification_factor in [round(-1+.1*j,1) for j in range(0,21)]:
            full_restable.loc[i,modification_factor] = (resTABLEs[0].loc[i,modification_factor]*88 + resTABLEs[1].loc[i,modification_factor]*68 + resTABLEs[2].loc[i,modification_factor]*96 + resTABLEs[3].loc[i,modification_factor]*305)/558
            #full_restable.loc[i,modification_factor] = (resTABLEs[0].loc[i,modification_factor]*88 + resTABLEs[1].loc[i,modification_factor]*68 + resTABLEs[2].loc[i,modification_factor]*96 )/(558-305)
    full_restable.to_csv('processed_data/weighted_MAPE_fixed_unbounded.csv')
    
    return full_restable
    
    
    

    
#DEPRECATED: APPARENTLY IDENTICAL TO create_network_game_datatable    
#HOW IS THIS DIFFERENT FROM THE ABOVE? IS IT NEEDED FOR SOMETHING? COMPARE TO create_network_game side by side   , DO TEXT COMPARE IN NOTEPAD. IN THE MEANTIME
def create_basemod_experiments(t100ranked_fn = "processed_data/nonstop_competitive_markets.csv", fleet_lookup_fn = "processed_data/fleet_lookup.csv",aotp_fn = 'bts_data/aotp_march.csv',fleet_dist_aug_fn='processed_data/fleet_dist_aug.csv'):   
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
    with open('processed_data/carrier_data_newF.txt','w') as outfile:       
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
                F = sum([fleet_lookup.groupby(['carrier','aircraft_type']).get_group((carrier,int(subtype)))['F'].iloc[0] for subtype in ac_type.split('-') ])
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
                Mnew = record['MARKET_TOT']
                #frequency index of carrier in market to determine order of coefficients
                freq_ind = record['MARKET_RANK']
                #create coefficients based on how many competitors in market
                '''
                coef_3player = [-150395.5496,-10106.6470,13135.9798,13136.1506,264.4822,-376.1793,-376.1781,270.2080,270.1927,-260.0113]
                coef_mat3player = pd.DataFrame([[coef+coef*modif for modif in [round(-1.5+.1*j,1) for j in range(0,31)]] for coef in coef_3player],columns=[round(-1.5+.1*j,1) for j in range(0,31)])
 
                coef_2player = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]
                coef_mat2player = pd.DataFrame([[coef+coef*modif for modif in [round(-1.5+.1*j,1) for j in range(0,31)]] for coef in coef_2player],columns=[round(-1.5+.1*j,1) for j in range(0,31)])
 
                coef_1player = [-95164.0447,-36238.3083,1148.0305]
                coef_mat1player = pd.DataFrame([[coef+coef*modif for modif in [round(-1.5+.1*j,1) for j in range(0,31)]] for coef in coef_1player],columns=[round(-1.5+.1*j,1) for j in range(0,31)])
 
                '''
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
    coef_df.to_csv('transcoef_table.csv',sep=';')
    return coef_df
    
    
    
    



#DEPRECATED" initial script for single carrier optimization
'''
   
    #create coefficients, US and WN in LAS PHX
    for i 
    # ADD iN OTHER FREQUENCIES FOR MAPE COUNTS IN MATLAB, CHECK ALL MATH/TAKE CALCS THROUGH, CHANGE CLASSIFICATION OF MARKETS WITH, do coefficient modification in a big  loop

    #DAILY_FREQ	FLIGHT_COST	MARKET_TOT	MARKET_COMPETITORS	MARKET_RANK	MS_TOT	AOTP_FLIGHT_TIME	BACKFOURTH
    US_LAS_PHX = [13.14794521,	2741.966489,	2893.984932,	2,	2,0.458567582,	0.753976102,	3.007952204];
    WN_LAS_PHX=[18.09863014,	2471.672902,	2893.984932,	2,	1,	0.541432418,	0.755375861,	3.010751722];
    #for WN...
    Cold=10000;
    Mold = 1000;
    rows=[]    
    for r in [0+i*.1 for i in range(0,11)]:
        base = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]
        Cnew = WN_LAS_PHX[1]
        Mnew = WN_LAS_PHX[2]
        freq_ind = WN_LAS_PHX[4]
        
        print((r*Mnew + (1-r)))
        transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,3)] + [-(1/Mold)*base[3]*(r*Mnew + (1-r)) if i==freq_ind else -(Mnew/Mold)*base[4] for i in range(1,3)] + [-(Mnew/Mold)*base[5]]
        F = WN_LAS_PHX[0]*WN_LAS_PHX[-1]/18 
        bf = WN_LAS_PHX[-1]
        row= [US_LAS_PHX[0],freq_ind,F,bf] + transcoef
        rows.append(row)
    df_out=pd.DataFrame(rows)
    df_out.to_csv('matlab_2stagegames/r_mod1.csv')
    US_LAS_PHX=[3.22739726,	2904.198215,	1617.493151,	2,	2,	0.195259873,	0.798509485,	3.09701897]
    WN_LAS_PHX=[13.55479452,	2477.744583,	1617.493151,	2,	1,	0.804740127,	0.79801444,	3.096028881]

    #for WN...
    Cold=10000;
    Mold = 1000;
    rows=[]    
    for r in [0+i*.1 for i in range(0,11)]:
        base = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]
        Cnew = WN_LAS_PHX[1]
        Mnew = WN_LAS_PHX[2]
        freq_ind = WN_LAS_PHX[4]
        
        print((r*Mnew + (1-r)))
        transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,3)] + [-(1/Mold)*base[3]*(r*Mnew + (1-r)) if i==freq_ind else -(Mnew/Mold)*base[4] for i in range(1,3)] + [-(Mnew/Mold)*base[5]]
        F = WN_LAS_PHX[0]*WN_LAS_PHX[-1]/18 
        bf = WN_LAS_PHX[-1]
        row= [US_LAS_PHX[0],freq_ind,F,bf] + transcoef
        rows.append(row)
    df_out=pd.DataFrame(rows)
    df_out.to_csv('matlab_2stagegames/r_mod2.csv')    
    
    
    US_LAS_PHX=[5.782191781,	3072.946756,	1630.075342,	2,	2	,0.361742251,	0.864029181,	3.228058361]
    WN_LAS_PHX =[11.3890411,	2702.690296	,1630.075342	,2,	1	,0.638257749,	0.854954955,	3.20990991]
    
      #for WN...
    Cold=10000;
    Mold = 1000;
    rows=[]    
    for r in [0+i*.1 for i in range(0,11)]:
        base = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]
        Cnew = WN_LAS_PHX[1]
        Mnew = WN_LAS_PHX[2]
        freq_ind = WN_LAS_PHX[4]
        
        print((r*Mnew + (1-r)))
        transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,3)] + [-(1/Mold)*base[3]*(r*Mnew + (1-r)) if i==freq_ind else -(Mnew/Mold)*base[4] for i in range(1,3)] + [-(Mnew/Mold)*base[5]]
        F = WN_LAS_PHX[0]*WN_LAS_PHX[-1]/18 
        bf = WN_LAS_PHX[-1]
        row= [US_LAS_PHX[0],freq_ind,F,bf] + transcoef
        rows.append(row)
    df_out=pd.DataFrame(rows)
    df_out.to_csv('matlab_2stagegames/r_mod3.csv')   
    
    #ADDD IN 
    
   
    US_LAS_PHX=[2.895890411,	3700.92247,	1068.210959,	2,	2	,0.248165541,	1.052,	3.604,]
    WN_LAS_PHX=[8.378082192	,3288.872331	,1068.210959,	2	,1,	0.751834459,	1.036753856,	3.573507713]
    

     #for WN...
    Cold=10000;
    Mold = 1000;
    rows=[]    
    for r in [0+i*.1 for i in range(0,11)]:
        base = [-274960.0,-16470.0,	34936.0,	425.6,	-1300.0,	595.7]
        Cnew = WN_LAS_PHX[1]
        Mnew = WN_LAS_PHX[2]
        freq_ind = WN_LAS_PHX[4]
        
        print((r*Mnew + (1-r)))
        transcoef = [-(Mnew/Mold)*base[0]] + [(Mnew/Mold)*(Cold-base[1])-Cnew if i==freq_ind else -(Mnew/Mold)*base[2] for i in range(1,3)] + [-(1/Mold)*base[3]*(r*Mnew + (1-r)) if i==freq_ind else -(Mnew/Mold)*base[4] for i in range(1,3)] + [-(Mnew/Mold)*base[5]]
        F = WN_LAS_PHX[0]*WN_LAS_PHX[-1]/18 
        bf = WN_LAS_PHX[-1]
        row= [US_LAS_PHX[0],freq_ind,F,bf] + transcoef
        rows.append(row)
    df_out=pd.DataFrame(rows)
    df_out.to_csv('matlab_2stagegames/r_mod4.csv')        
'''     