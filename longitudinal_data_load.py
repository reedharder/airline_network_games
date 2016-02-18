# -*- coding: utf-8 -*-
"""
Created on Tue Sep 22 15:26:47 2015


"""

'''
AOTP columns to load: year, quarter, month, dayofmonth, dayofweek, flightdate, uniqueCarrier, airlineID, TailNum, OriginArportID, Origin, DestinationAirport, Destination,number of flights, cancelled, diverted, distance, all delay types (LATER), airtime
Will need T100 to asses aircraft type stability (type, number of seats, market size, etc) !
will need route demands to assess market size more realistically
will need schedule p52 to assess cost and cost changes (at a different interval)
    need code to get number of major players in market, longitudinal index conversion, \
    reduce to markets and players of interest
    decide frequency and marketshare thresholds of interest? with AOTP, we can only do the former, so for convenience set at .5 a day, test sensitivity or try more sophisticated methods later
    detect interesting events (mergers, competitor changes/entry/exit, dramatic increases in frequency)
    plot market averages, carrier average, bin by month, quarter, year (to sanity check with frequency_prediction.py data), observe seasoality 
    
will need D1B1 to get prices
we will have different time scales interestingly/annoyingly (especially costs, which is on quarterly basis, vs frequency on daily basis. Smoothing to be done? mergers, number of competitors, number of regional vs major, perhaps some estimate of fares from DB1B, could be done however)
enumerate all the different time scales we have in an excel sheet

for AOTP start with one year first, then proceed
and start just with our network perhaps

rember that 2008 etc is a leap year

for AOTP, in market if 10 day moving average of marketshare/frequency meets cuttoff (could be useful to plot as well)

some day, try shipping airline competition
'''
import io
import zipfile
import sys
import requests
import os 
import time

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pylab

oep35 = ['ATL',
 'BOS',
 'BWI',
 'CLE',
 'CLT',
 'CVG',
 'DCA',
 'DEN',
 'DFW',
 'DTW',
 'EWR',
 'FLL',
 'HNL',
 'IAD',
 'IAH',
 'JFK',
 'LAS',
 'LAX',
 'LGA',
 'MCO',
 'MDW',
 'MEM',
 'MIA',
 'MSP',
 'ORD',
 'PDX',
 'PHL',
 'PHX',
 'PIT',
 'SAN',
 'SEA',
 'SFO',
 'SLC',
 'STL',
 'TPA']

AOTP_POST='''UserTableName=On_Time_Performance&DBShortName=On_Time&RawDataTable=T_ONTIME&sqlstr=+SELECT+YEAR%2CQUARTER
%2CMONTH%2CDAY_OF_MONTH%2CDAY_OF_WEEK%2CFL_DATE%2CUNIQUE_CARRIER%2CORIGIN_AIRPORT_ID%2CORIGIN%2CDEST_AIRPORT_ID
%2CDEST%2CCANCELLED%2CDIVERTED%2CDISTANCE%2CCARRIER_DELAY%2CLATE_AIRCRAFT_DELAY+FROM++T_ONTIME+WHERE
+Month+%3D2+AND+YEAR%3D2007&varlist=YEAR%2CQUARTER%2CMONTH%2CDAY_OF_MONTH%2CDAY_OF_WEEK%2CFL_DATE%2CUNIQUE_CARRIER
%2CORIGIN_AIRPORT_ID%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST%2CCANCELLED%2CDIVERTED%2CDISTANCE%2CCARRIER_DELAY
%2CLATE_AIRCRAFT_DELAY&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time
={month}&timename=Month&GEOGRAPHY=All&XYEAR={year}&FREQUENCY={frequency}&VarName=YEAR&VarDesc=Year&VarType=Num&VarName
=QUARTER&VarDesc=Quarter&VarType=Num&VarName=MONTH&VarDesc=Month&VarType=Num&VarName=DAY_OF_MONTH&VarDesc
=DayofMonth&VarType=Num&VarName=DAY_OF_WEEK&VarDesc=DayOfWeek&VarType=Num&VarName=FL_DATE&VarDesc=FlightDate
&VarType=Char&VarName=UNIQUE_CARRIER&VarDesc=UniqueCarrier&VarType=Char&VarDesc=AirlineID&VarType=Num
&VarDesc=Carrier&VarType=Char&VarDesc=TailNum&VarType=Char&VarDesc=FlightNum&VarType=Char&VarName=ORIGIN_AIRPORT_ID
&VarDesc=OriginAirportID&VarType=Num&VarDesc=OriginAirportSeqID&VarType=Num&VarDesc=OriginCityMarketID
&VarType=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarDesc=OriginCityName&VarType=Char&VarDesc=OriginState
&VarType=Char&VarDesc=OriginStateFips&VarType=Char&VarDesc=OriginStateName&VarType=Char&VarDesc=OriginWac
&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarDesc=DestAirportSeqID&VarType
=Num&VarDesc=DestCityMarketID&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarDesc=DestCityName
&VarType=Char&VarDesc=DestState&VarType=Char&VarDesc=DestStateFips&VarType=Char&VarDesc=DestStateName
&VarType=Char&VarDesc=DestWac&VarType=Num&VarDesc=CRSDepTime&VarType=Char&VarDesc=DepTime&VarType=Char
&VarDesc=DepDelay&VarType=Num&VarDesc=DepDelayMinutes&VarType=Num&VarDesc=DepDel15&VarType=Num&VarDesc
=DepartureDelayGroups&VarType=Num&VarDesc=DepTimeBlk&VarType=Char&VarDesc=TaxiOut&VarType=Num&VarDesc
=WheelsOff&VarType=Char&VarDesc=WheelsOn&VarType=Char&VarDesc=TaxiIn&VarType=Num&VarDesc=CRSArrTime&VarType
=Char&VarDesc=ArrTime&VarType=Char&VarDesc=ArrDelay&VarType=Num&VarDesc=ArrDelayMinutes&VarType=Num&VarDesc
=ArrDel15&VarType=Num&VarDesc=ArrivalDelayGroups&VarType=Num&VarDesc=ArrTimeBlk&VarType=Char&VarName
=CANCELLED&VarDesc=Cancelled&VarType=Num&VarDesc=CancellationCode&VarType=Char&VarName=DIVERTED&VarDesc
=Diverted&VarType=Num&VarDesc=CRSElapsedTime&VarType=Num&VarDesc=ActualElapsedTime&VarType=Num&VarDesc
=AirTime&VarType=Num&VarDesc=Flights&VarType=Num&VarName=DISTANCE&VarDesc=Distance&VarType=Num&VarDesc
=DistanceGroup&VarType=Num&VarName=CARRIER_DELAY&VarDesc=CarrierDelay&VarType=Num&VarDesc=WeatherDelay
&VarType=Num&VarDesc=NASDelay&VarType=Num&VarDesc=SecurityDelay&VarType=Num&VarName=LATE_AIRCRAFT_DELAY
&VarDesc=LateAircraftDelay&VarType=Num&VarDesc=FirstDepTime&VarType=Char&VarDesc=TotalAddGTime&VarType
=Num&VarDesc=LongestAddGTime&VarType=Num&VarDesc=DivAirportLandings&VarType=Num&VarDesc=DivReachedDest
&VarType=Num&VarDesc=DivActualElapsedTime&VarType=Num&VarDesc=DivArrDelay&VarType=Num&VarDesc=DivDistance
&VarType=Num&VarDesc=Div1Airport&VarType=Char&VarDesc=Div1AirportID&VarType=Num&VarDesc=Div1AirportSeqID
&VarType=Num&VarDesc=Div1WheelsOn&VarType=Char&VarDesc=Div1TotalGTime&VarType=Num&VarDesc=Div1LongestGTime
&VarType=Num&VarDesc=Div1WheelsOff&VarType=Char&VarDesc=Div1TailNum&VarType=Char&VarDesc=Div2Airport
&VarType=Char&VarDesc=Div2AirportID&VarType=Num&VarDesc=Div2AirportSeqID&VarType=Num&VarDesc=Div2WheelsOn
&VarType=Char&VarDesc=Div2TotalGTime&VarType=Num&VarDesc=Div2LongestGTime&VarType=Num&VarDesc=Div2WheelsOff
&VarType=Char&VarDesc=Div2TailNum&VarType=Char&VarDesc=Div3Airport&VarType=Char&VarDesc=Div3AirportID
&VarType=Num&VarDesc=Div3AirportSeqID&VarType=Num&VarDesc=Div3WheelsOn&VarType=Char&VarDesc=Div3TotalGTime
&VarType=Num&VarDesc=Div3LongestGTime&VarType=Num&VarDesc=Div3WheelsOff&VarType=Char&VarDesc=Div3TailNum
&VarType=Char&VarDesc=Div4Airport&VarType=Char&VarDesc=Div4AirportID&VarType=Num&VarDesc=Div4AirportSeqID
&VarType=Num&VarDesc=Div4WheelsOn&VarType=Char&VarDesc=Div4TotalGTime&VarType=Num&VarDesc=Div4LongestGTime
&VarType=Num&VarDesc=Div4WheelsOff&VarType=Char&VarDesc=Div4TailNum&VarType=Char&VarDesc=Div5Airport
&VarType=Char&VarDesc=Div5AirportID&VarType=Num&VarDesc=Div5AirportSeqID&VarType=Num&VarDesc=Div5WheelsOn
&VarType=Char&VarDesc=Div5TotalGTime&VarType=Num&VarDesc=Div5LongestGTime&VarType=Num&VarDesc=Div5WheelsOff
&VarType=Char&VarDesc=Div5TailNum&VarType=Char'''


DB1BMARKETS_POST='''UserTableName=DB1BMarket&DBShortName=Origin_and_Destination_Survey&RawDataTable=T_DB1B_MARKET&sqlstr
=+SELECT+ITIN_ID%2CMKT_ID%2CMARKET_COUPONS%2CYEAR%2CQUARTER%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID
%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST
%2CREPORTING_CARRIER%2CTICKET_CARRIER%2COPERATING_CARRIER%2CBULK_FARE%2CPASSENGERS%2CMARKET_FARE%2CMARKET_MILES_FLOWN
%2CNONSTOP_MILES+FROM++T_DB1B_MARKET+WHERE+Quarter+%3D{quarter}+AND+YEAR%3D{year}&varlist=ITIN_ID%2CMKT_ID%2CMARKET_COUPONS
%2CYEAR%2CQUARTER%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CDEST_AIRPORT_ID
%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CREPORTING_CARRIER%2CTICKET_CARRIER%2COPERATING_CARRIER
%2CBULK_FARE%2CPASSENGERS%2CMARKET_FARE%2CMARKET_MILES_FLOWN%2CNONSTOP_MILES&grouplist=&suml=&sumRegion
=&filter1=title%3D&filter2=title%3D&geo=All%A0&time=Q+{quarter}&timename=Quarter&GEOGRAPHY=All&XYEAR={year}&FREQUENCY
=1&VarName=ITIN_ID&VarDesc=ItinID&VarType=Num&VarName=MKT_ID&VarDesc=MktID&VarType=Num&VarName=MARKET_COUPONS
&VarDesc=MktCoupons&VarType=Num&VarName=YEAR&VarDesc=Year&VarType=Num&VarName=QUARTER&VarDesc=Quarter
&VarType=Num&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID
&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType
=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarDesc=OriginCountry&VarType=Char&VarDesc=OriginStateFips
&VarType=Char&VarDesc=OriginState&VarType=Char&VarDesc=OriginStateName&VarType=Char&VarDesc=OriginWac
&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc
=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID&VarType=Num&VarName
=DEST&VarDesc=Dest&VarType=Char&VarDesc=DestCountry&VarType=Char&VarDesc=DestStateFips&VarType=Char&VarDesc
=DestState&VarType=Char&VarDesc=DestStateName&VarType=Char&VarDesc=DestWac&VarType=Num&VarDesc=AirportGroup
&VarType=Char&VarDesc=WacGroup&VarType=Char&VarDesc=TkCarrierChange&VarType=Num&VarDesc=TkCarrierGroup
&VarType=Char&VarDesc=OpCarrierChange&VarType=Num&VarDesc=OpCarrierGroup&VarType=Char&VarName=REPORTING_CARRIER
&VarDesc=RPCarrier&VarType=Char&VarName=TICKET_CARRIER&VarDesc=TkCarrier&VarType=Char&VarName=OPERATING_CARRIER
&VarDesc=OpCarrier&VarType=Char&VarName=BULK_FARE&VarDesc=BulkFare&VarType=Num&VarName=PASSENGERS&VarDesc
=Passengers&VarType=Num&VarName=MARKET_FARE&VarDesc=MktFare&VarType=Num&VarDesc=MktDistance&VarType=Num
&VarDesc=MktDistanceGroup&VarType=Num&VarName=MARKET_MILES_FLOWN&VarDesc=MktMilesFlown&VarType=Num&VarName
=NONSTOP_MILES&VarDesc=NonStopMiles&VarType=Num&VarDesc=ItinGeoType&VarType=Num&VarDesc=MktGeoType&VarType
=Num
'''

DB1BCOUPONS_POST='''UserTableName=DB1BCoupon&DBShortName=&RawDataTable=T_DB1B_COUPON&sqlstr=+SELECT+ITIN_ID%2CMKT_ID%2CSEQ_NUM
%2CCOUPONS%2CYEAR%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CQUARTER%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID
%2CDEST%2CTICKET_CARRIER%2COPERATING_CARRIER%2CREPORTING_CARRIER%2CPASSENGERS%2CFARE_CLASS+FROM++T_DB1B_COUPON
+WHERE+Quarter+%3D{quarter}+AND+YEAR%3D{year}&varlist=ITIN_ID%2CMKT_ID%2CSEQ_NUM%2CCOUPONS%2CYEAR%2CORIGIN_AIRPORT_ID
%2CORIGIN_AIRPORT_SEQ_ID%2CQUARTER%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST%2CTICKET_CARRIER
%2COPERATING_CARRIER%2CREPORTING_CARRIER%2CPASSENGERS%2CFARE_CLASS&grouplist=&suml=&sumRegion=&filter1
=title%3D&filter2=title%3D&geo=All%A0&time=Q+{quarter}&timename=Quarter&GEOGRAPHY=All&XYEAR={year}&FREQUENCY=1
&VarName=ITIN_ID&VarDesc=ItinID&VarType=Num&VarName=MKT_ID&VarDesc=MktID&VarType=Num&VarName=SEQ_NUM
&VarDesc=SeqNum&VarType=Num&VarName=COUPONS&VarDesc=Coupons&VarType=Num&VarName=YEAR&VarDesc=Year&VarType
=Num&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID&VarDesc
=OriginAirportSeqID&VarType=Num&VarDesc=OriginCityMarketID&VarType=Num&VarName=QUARTER&VarDesc=Quarter
&VarType=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarDesc=OriginCountry&VarType=Char&VarDesc=OriginStateFips
&VarType=Char&VarDesc=OriginState&VarType=Char&VarDesc=OriginStateName&VarType=Char&VarDesc=OriginWac
&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc
=DestAirportSeqID&VarType=Num&VarDesc=DestCityMarketID&VarType=Num&VarName=DEST&VarDesc=Dest&VarType
=Char&VarDesc=DestCountry&VarType=Char&VarDesc=DestStateFips&VarType=Char&VarDesc=DestState&VarType=Char
&VarDesc=DestStateName&VarType=Char&VarDesc=DestWac&VarType=Num&VarDesc=Break&VarType=Char&VarDesc=CouponType
&VarType=Char&VarName=TICKET_CARRIER&VarDesc=TkCarrier&VarType=Char&VarName=OPERATING_CARRIER&VarDesc
=OpCarrier&VarType=Char&VarName=REPORTING_CARRIER&VarDesc=RPCarrier&VarType=Char&VarName=PASSENGERS&VarDesc
=Passengers&VarType=Num&VarName=FARE_CLASS&VarDesc=FareClass&VarType=Char&VarDesc=Distance&VarType=Num
&VarDesc=DistanceGroup&VarType=Num&VarDesc=Gateway&VarType=Num&VarDesc=ItinGeoType&VarType=Num&VarDesc
=CouponGeoType&VarType=Num'''

    
#send PORT request to BTS server at specified url, make appropriate request, extract and save fi;e
#post_data is htlm form data for requests
#post_vars is a dictionary of associated variabls to replace     
#outdir is directory to save files to
def bts_table_request(url,post_data,post_vars,outfile,outdir):
    #assign content type
    headers = {
        "Content-Type" : "application/x-www-form-urlencoded",
        } 
    #post database request
    try: 
        r = requests.post(url, data=post_data.format(**post_vars),headers=headers)
    except:
        print('Http request error')
        print(post_vars)
        print(sys.exc_info()[0])
        return 0
    try: 
        #get file handle for returned ZIP file
        z=zipfile.ZipFile(io.BytesIO(r.content),'r')
        #exctract csv file from returned bytecode, save in appropriate directory
        for filename in z.namelist():
            if filename.split('.')[-1] =='csv':
                z.extract(filename,outdir)
                os.rename(outdir + filename, outdir + outfile)
        #just to prevent BTS from getting mad
        time.sleep(.5)
    except:
        print('IO error')
        print(post_vars)
        print(sys.exc_info()[0])
        return 0
    return 1

# download a set of D1B1 MARKETS
def DB1BMarkets_download(post=DB1BMARKETS_POST,years = [2007], quarters=list(range(1,5)), root_filename = 'DB1B_MARKET', outdir='C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'):
    #url for DB1B markets table    
    url = 'http://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=236&Has_Group=3&Is_Zipped=0'    
    #download table for each 
    for year in years:
        for quarter in quarters:
            #request variables           
            post_vars={'year':year,'quarter':quarter} 
            print('downloading db1b markets {year} Q{quarter}'.format(**post_vars))
            outfile = root_filename+'_'+str(year)+'_Q'+str(quarter)+'.csv'
            post_data = post
            #make request
            status = bts_table_request(url,post_data,post_vars,outfile,outdir)
            if status==1:
                print('Done')
            else:
                print('Error')
    return('All files downloaded')
  
def DB1BCoupons_download(post=DB1BCOUPONS_POST,years = [2007], quarters=list(range(1,5)), root_filename = 'DB1B_COUPON', outdir='C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'):
    #url for DB1B markets table    
    url = 'http://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=289&Has_Group=0&Is_Zipped=0'    
    #download table for each 
    for year in years:
        for quarter in quarters:
            #request variables           
            post_vars={'year':year,'quarter':quarter} 
            print('downloading db1b coupons {year} Q{quarter}'.format(**post_vars))
            outfile = root_filename+'_'+str(year)+'_Q'+str(quarter)+'.csv'
            post_data = post
            #make request
            status = bts_table_request(url,post_data,post_vars,outfile,outdir)
            if status==1:
                print('Done')
            else:
                print('Error')
    return('All files downloaded')
    

  

def aotp_download(post=AOTP_POST,years = [2007], months=list(range(1,13)), data_dir='C:/users/d29905p/Documents/longitudinal_airline_network/'):
    '''    
    https://github.com/isaacobezo/get_rita/blob/master/get_transtat_data.py
    https://public.tableau.com/s/blog/2013/08/data-scraping-part-iii-python
    http://docs.python-requests.org/en/latest/user/quickstart/
    '''    
    months_str = ['January','February','March','April','May','June','July','August','September','October','November','December']
    headers = {
        "Content-Type" : "application/x-www-form-urlencoded",
        }    
    ######base_url = PUT AOTP BASE HERE
    for year in years:
        for month in months:
            ########request variables           
            post_vars={'year':year,'month':month,frequency:str(1)} 
            print('downloading db1b coupons {year} Q{quarter}'.format(**post_vars))
            outfile = root_filename+'_'+str(year)+'_Q'+str(quarter)+'.csv'
            post_data = post
            #make request
            status = bts_table_request(url,post_data,post_vars,outfile,outdir)
            if status==1:
                print('Done')
            else:
                print('Error')
            #NOT ACTUALLY SURE WHAT FREQUENCY DOES
            ##frequency=str(month)
            #string of month
            month_str = months_str[month-1]
            year = str(year)
            #format post string
            AOTP_POST_formatted = AOTP_POST.format(month=month_str, year=year, frequency=1)
            AOTP_POST_formatted = AOTP_POST_formatted.replace('\n','')
            data = AOTP_POST_formatted.splot
           
    
            r = requests.post(base_url, data=data,headers=headers)


    time.sleep(.5)
    
    
    
    
    
    
    
    
    
def target_market(sector,program_target):
    print(sector)
    print(program_target)
    return []
    
def aotp_merge(airports = ['SEA','PDX','SFO','SAN','LAX','LAS','PHX','OAK','ONT','SMF','SJC'],years = [2007], months=list(range(1,13)),data_dir='C:/users/d29905p/Documents/longitudinal_airline_network/'):
    
    days_in_month = [31,28,31,30,31,30,31,31,30,31,30,31]
    quarters = [1,1,1,2,2,2,3,3,3,4,4,4]
    months_str = ['January','February','March','April','May','June','July','August','September','October','November','December']
    for year in years:
        for month in months:
            month_str = months_str[month-1]
            yr_month_full_file = pd.read_csv(data_dir + 'aotp_files/aotp_%s_%s.csv' % (str(year),str(month)))
            #filter by airport
            yr_month_full_file = yr_month_full_file
   

def t100_merge( merge_HP = True, freq_cuttoff = 2, ms_cuttoff=.01,airports = ['SEA','PDX','SFO','SAN','LAX','LAS','PHX','OAK','ONT','SMF','SJC'],airlines = ['AA','AS','MQ','OO','QX','UA','US','WN','HP'],years = [2007], months=list(range(1,13)),data_dir='C:/users/d29905p/Documents/longitudinal_airline_network/'):
    days_in_month = [31,28,31,30,31,30,31,31,30,31,30,31]
    quarters = [1,1,1,2,2,2,3,3,3,4,4,4]
    months_str = ['January','February','March','April','May','June','July','August','September','October','November','December']
    t100_year_dfs = []  
    def create_market(row):
        market = [row['ORIGIN'], row['DEST']]
        market.sort()
        return "_".join(market)
    for year in years:
        t100_yr = pd.read_csv(data_dir + 't100_files/t100_%s.csv' % str(year))
        if airlines:
            t100_yr_network = t100_yr[t100_yr['ORIGIN'].isin(airports) & t100_yr['DEST'].isin(airports) & t100_yr['UNIQUE_CARRIER'].isin(airlines) ]
        else:
            t100_yr_network = t100_yr[t100_yr['ORIGIN'].isin(airports) & t100_yr['DEST'].isin(airports) ]
            
        t100_yr_network['BI_MARKET'] = t100_yr_network.apply(create_market,1)
        if merge_HP:
            t100_yr_network['UNIQUE_CARRIER']=t100_yr_network['UNIQUE_CARRIER'].replace('HP','US')
        #sum between craft type
        t100_yr_network_merge_mkt = t100_yr_network[['UNIQUE_CARRIER','BI_MARKET','MONTH','DEPARTURES_SCHEDULED','PASSENGERS','ORIGIN','DEST']].groupby(('UNIQUE_CARRIER','BI_MARKET','ORIGIN','DEST','MONTH')).aggregate(np.sum).reset_index()
        #sum between craft type
        t100_yr_network_merge_craft = t100_yr_network_merge_mkt[['UNIQUE_CARRIER','BI_MARKET','MONTH','DEPARTURES_SCHEDULED','PASSENGERS']].groupby(('UNIQUE_CARRIER','BI_MARKET','MONTH')).aggregate(np.mean).reset_index()        
        t100_yr_network_merge_craft['DAILY_FREQ'] = t100_yr_network_merge_craft.apply(lambda row: row['DEPARTURES_SCHEDULED']/float(days_in_month[int(row['MONTH'])-1]),1)
        t100_yr_network_merge_craft_freq_filt = t100_yr_network_merge_craft[t100_yr_network_merge_craft['DAILY_FREQ']>=freq_cuttoff]
        #t100_yr_network_merge_craft_freq_filt
        t100_grouped = t100_yr_network_merge_craft_freq_filt.groupby(('BI_MARKET','MONTH'))
        grouplist = []
        for market in list(set(t100_yr_network_merge_craft_freq_filt['BI_MARKET'].tolist())):
            for month in range(1,13):
                try:
                    market_group = t100_grouped.get_group((market,month))
                    new_group = market_rank(market_group, ms_cuttoff=ms_cuttoff)
                    grouplist.append(new_group)
                except KeyError:
                    pass
        t100ranked = pd.concat(grouplist,axis=0)
    '''
    add concatenation of multiple years later
    '''
    
    #plot frequencies of major carriers in different markets over course of year as well as number of competitors (especially relelvant when we do a market share cuttoff)
    t100markets = t100ranked.groupby('BI_MARKET')    
    markets = list(set(t100ranked['BI_MARKET'].tolist()))  # WHY IS THIS DIFFERENT markets = list(set(t100_yr_network_merge_craft_freq_filt['BI_MARKET'].tolist())) 
    for market in markets:
        market_df = t100markets.get_group(market)    
        plt.subplot(2,1,1)
        plt.ylabel('Flights per Day')
        for carrier in list(set(market_df['UNIQUE_CARRIER'].tolist())):
            carrier_gb = market_df[market_df['UNIQUE_CARRIER']==carrier].set_index('MONTH')
            carrier_vector = []
            for i in range(1,13):
                try:
                    carrier_vector.append(float(carrier_gb.loc[i]['DAILY_FREQ']))
                except KeyError:
                    carrier_vector.append(0.0)
            plt.plot(list(range(1,13)),carrier_vector, label=carrier)
        plt.legend(shadow=True, loc=3,fancybox=True)   
        plt.title('Frequency Competition in %s Market, %s' % (market, year))
        #plt.show()
        #ADD MARKET COMPETITOR MARKER?? SEARCH FOR ENTRIES
        plt.axis([1, 12, 0, 22])
        plt.subplot(2,1,2)
        plt.ylabel('Mkt Share')
        plt.xlabel('time (months)')
        plt.axis([1, 12, 0, 1.1])
        for carrier in list(set(market_df['UNIQUE_CARRIER'].tolist())):
            carrier_gb = market_df[market_df['UNIQUE_CARRIER']==carrier].set_index('MONTH')
            carrier_vector = []
            for i in range(1,13):
                try:
                    carrier_vector.append(float(carrier_gb.loc[i]['MS_TOT']))
                except KeyError:
                    carrier_vector.append(0.0)
            plt.plot(list(range(1,13)),carrier_vector, label=carrier)
        #plt.legend(shadow=True, loc=2,fancybox=True)       
        
        plt.savefig(data_dir+'t100_%s_pics/12month_ms-freq_11port_HP-US-merge_%s.jpg' % (year,market))
        plt.clf()
            
            
            
    
        
        
'''
merge on craft type
for each market, get number of competitors  who meet frequency and (market share thresholds), each month 
get market_carrier combos, 
for each carrier, graph frequencies in each of its markets over year
more importantly, for each market, make graph (and save) of frequencies over course of the year for the major carriers in that market
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