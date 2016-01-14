# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 20:25:15 2015

@author: Reed
"""
import pandas as pd

###datadir = "C:/users/d29905p/documents/airline_competition_paper/code/network_games/"

datadir = 'o:/documents/airline_competition_paper/code/network_games/'
MISS_DICT_2007 = {'B747-1': 'B747-100',
 'DC-10-F': 'DC10-30F',
 'DC-8-6F': 'DC-8-63F',
 'B767-3': 'B767-3/R',
 'B737-7/L': 'B737-7',
 'DC-10-4': 'DC-10-40',
 'B767-2': 'B767-2/R',
 'DC-10-1': 'DC-10-10',
 'B727-1': 'B727-1C',
 'DC-10-3': 'DC-10-30',
 'B727-1':'B727-1C'}
 
 
type1=pd.read_csv(datadir+"bts_data/AIRCRAFT_TYPE_LOOKUP.csv")
b43 = pd.read_csv(datadir+"bts_data/SCHEDULE_B43_FULL.txt")
b43_14 = pd.read_csv(datadir+"bts_data/SCHEDULE_B43_2014.csv")

k=[m for m in b43.MODEL.value_counts().keys() if m not in type1.SHORT_NAME.tolist()]

k2=[m for m in b43_14.MODEL.value_counts().keys() if m not in type1.SHORT_NAME.tolist()]

k3=[m for m in b43_14.MODEL.value_counts().keys() if m in type1.SHORT_NAME.tolist()]




MODEL_LIST = b43_14.MODEL.value_counts().keys()
MODEL_LIST = [m if m[:3]!='B-7' else m.replace('B-7','B7') for m in MODEL_LIST]
TYPE_LIST = type1.SHORT_NAME.tolist()

type1_lookup = type1.set_index('SHORT_NAME')

#function to get id number, dictionary converting badly chosen models to shortnames, 
# and short name of associated craft
#takes model name derived  from a schedule B-43, as well as SHORT_NAME indexed dataframe
#returns list: [id, shortname, model]
'''
could also do search by tail number from better records in 2007: actually do this first, use heuristics later
'''
def get_shortname(model,MISSDICT = MISS_DICT_2007,model_lookup=type1_lookup):    
    try:
        return [model_lookup.loc[model]['AC_TYPEID'],model,model]
    except KeyError:
        return [model_lookup.loc[MISS_DICT_2007[model]]['AC_TYPEID'],MISS_DICT_2007[model],model]
        
def estimate_shortname():
    AC_2007 = [get_shortname(model) for model in b43.MODEL.value_counts().keys()]       
    AC=[]
    failures = []
    for model in MODEL_LIST:
        row ='NULL'
        try:
            row = get_shortname(model)    
        except KeyError:
            #try cleaving off end letter by letter to make it fit 
            model_copy = model        
            while len(model_copy) >1:
                model_copy = model_copy[:-1]
                try:
                    row = get_shortname(model_copy)    
                    break
                except KeyError:
                    pass
            #if no matches so far, try adding zeros to end
            if row =='NULL':
                model_copy = model        
                for i in range(0,3):
                    model_copy += '0'
                    try:
                        row = get_shortname(model_copy)    
                        break
                    except KeyError:
                        pass
        #if still no match, append to failure list            
        if row =='NULL':
            failures.append(model)
        else:
            AC.append(row)

'''
function to get find shortname from tail number correspondences with earlier schedule B-43
'''

def shortname_from_tailnum(row,schedule=b43_base):
    pass
    

def get_ac_typeid(correspondence_fn=datadir+"processed_data/b43_type_id_correspondence.csv",new_b43_fn = datadir+"bts_data/SCHEDULE_B43_2014.csv",base_b43_fn=datadir+"bts_data/SCHEDULE_B43_FULL_2007.txt"):
    correspondence_table  = pd.read_csv(correspondence_fn).set_index('MODEL')
    
    b43_new  = pd.read_csv(new_b43_fn)
    b43_base  = pd.read_csv(base_b43_fn)
    handtable_2014 = pd.read_csv(datadir + "processed_data/b43_2014_model.csv")
    handtable_2014['MODEL']= handtable_2014['MODEL'].apply(lambda x: x.lstrip().replace(",",""))    
    handtable_2014=handtable_2014.set_index('MODEL')
    #from correspondence table, match B43 2007 records with a numeric aircraft type identifier corresponding to AC_TYPEID found in AircraftType BTS table
    b43_base['AC_TYPEID'] = b43_base.apply(lambda row: int(correspondence_table.loc[row['MODEL']]['AC_TYPEID']),axis=1)
    #b43_new['AC_TYPEID'] = b43_new.apply(shortname_from_tailnum,axis=1,schedule=b43_base)
    AC_IDS = []
    models = []
    for i in range(0,b43_new.shape[0]):
        row = b43_new.iloc[i,:]
        original_model = row['MODEL']
        ##if row['MODEL']== 'B-737-4B7':
          ##  row['MODEL']
        try:
            ac_id = correspondence_table.loc[row['MODEL']]['AC_TYPEID']
            AC_IDS.append(ac_id)
            models.append(original_model)
        except KeyError:
            ac_id = b43_base[b43_base['TAIL_NUMBER']==row['TAIL_NUMBER']]['AC_TYPEID'] ##sooo select first available
            if not ac_id.empty:
                try:
                    ac_id = int(ac_id)
                    AC_IDS.append(ac_id)
                    models.append(original_model)
                except TypeError:
                    ac_id = ac_id.iloc[0]
                    AC_IDS.append(ac_id)
                    models.append(original_model)
            else:               
                model = row['MODEL'] if row['MODEL'][:3]!='B-7' else row['MODEL'].replace('B-7','B7')
                row ='NULL'
                try:
                    row = get_shortname(model)    
                except KeyError:
                    #try cleaving off end letter by letter to make it fit 
                    model_copy = model        
                    while len(model_copy) >1:
                        model_copy = model_copy[:-1]
                        try:
                            row = get_shortname(model_copy)    
                            break
                        except KeyError:
                            pass
                    #if no matches so far, try adding zeros to end
                    if row =='NULL':
                        model_copy = model        
                        for i in range(0,3):
                            model_copy += '0'
                            try:
                                row = get_shortname(model_copy)    
                                break
                            except KeyError:
                                pass
                #if still no match, append to failure list            
                if row =='NULL':
                    table_row = handtable_2014.loc[model]
                    if table_row['SHORT_NAME']=='HERCULES' or table_row['SHORT_NAME'][0]=='HERCULES':
                        ac_id =556  
                        AC_IDS.append(ac_id)
                        models.append(original_model)
                    else: 
                        try:
                            ac_id = int(type1_lookup.loc[table_row['SHORT_NAME']]['AC_TYPEID'])                                    
                            AC_IDS.append(ac_id)
                            models.append(original_model)
                        except TypeError:
                           
                                ac_id =int(type1_lookup.loc[table_row['SHORT_NAME'].iloc[0]]['AC_TYPEID'])  
                                AC_IDS.append(ac_id)
                                models.append(original_model)
                            
                else:
                    ac_id = row[0]
                    
                    AC_IDS.append(ac_id)
                    models.append(original_model) 
                    
    conversion_df=pd.DataFrame([AC_IDS, models]).transpose().groupby(0)    
    idset = list(set(AC_IDS))
    conversion_dict = {k:list(set(conversion_df.get_group(k)[1].tolist())) for k in idset}
    


    
    
    