# -*- coding: utf-8 -*-ls

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


'''
POST contents, with time specification variablized (year, month, quarter, etc)
'''


AOTP_POST = '''UserTableName=On_Time_Performance&DBShortName=On_Time&RawDataTable=T_ONTIME&sqlstr=+SELECT+YEAR%2CQUARTER
%2CMONTH%2CDAY_OF_MONTH%2CDAY_OF_WEEK%2CFL_DATE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CCARRIER%2CTAIL_NUM%2CFL_NUM
%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CORIGIN_CITY_NAME%2CORIGIN_STATE_ABR
%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_NM%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID
%2CDEST%2CDEST_CITY_NAME%2CDEST_STATE_ABR%2CDEST_STATE_FIPS%2CDEST_STATE_NM%2CDEST_WAC%2CCRS_DEP_TIME
%2CDEP_TIME%2CDEP_DELAY%2CDEP_DELAY_NEW%2CDEP_DEL15%2CDEP_DELAY_GROUP%2CDEP_TIME_BLK%2CTAXI_OUT%2CWHEELS_OFF
%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME%2CARR_DELAY%2CARR_DELAY_NEW%2CARR_DEL15%2CARR_DELAY_GROUP
%2CARR_TIME_BLK%2CCANCELLED%2CCANCELLATION_CODE%2CDIVERTED%2CCRS_ELAPSED_TIME%2CACTUAL_ELAPSED_TIME%2CAIR_TIME
%2CFLIGHTS%2CDISTANCE%2CDISTANCE_GROUP%2CCARRIER_DELAY%2CWEATHER_DELAY%2CNAS_DELAY%2CSECURITY_DELAY%2CLATE_AIRCRAFT_DELAY
+FROM++T_ONTIME+WHERE+Month+%3D{month_num}+AND+YEAR%3D{year}&varlist=YEAR%2CQUARTER%2CMONTH%2CDAY_OF_MONTH%2CDAY_OF_WEEK
%2CFL_DATE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CCARRIER%2CTAIL_NUM%2CFL_NUM%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID
%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CORIGIN_CITY_NAME%2CORIGIN_STATE_ABR%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_NM
%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CDEST_CITY_NAME%2CDEST_STATE_ABR
%2CDEST_STATE_FIPS%2CDEST_STATE_NM%2CDEST_WAC%2CCRS_DEP_TIME%2CDEP_TIME%2CDEP_DELAY%2CDEP_DELAY_NEW%2CDEP_DEL15
%2CDEP_DELAY_GROUP%2CDEP_TIME_BLK%2CTAXI_OUT%2CWHEELS_OFF%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME
%2CARR_DELAY%2CARR_DELAY_NEW%2CARR_DEL15%2CARR_DELAY_GROUP%2CARR_TIME_BLK%2CCANCELLED%2CCANCELLATION_CODE
%2CDIVERTED%2CCRS_ELAPSED_TIME%2CACTUAL_ELAPSED_TIME%2CAIR_TIME%2CFLIGHTS%2CDISTANCE%2CDISTANCE_GROUP
%2CCARRIER_DELAY%2CWEATHER_DELAY%2CNAS_DELAY%2CSECURITY_DELAY%2CLATE_AIRCRAFT_DELAY&grouplist=&suml=
&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time={month}&timename=Month&GEOGRAPHY=All&XYEAR
={year}&FREQUENCY={frequency}&VarName=YEAR&VarDesc=Year&VarType=Num&VarName=QUARTER&VarDesc=Quarter&VarType=Num&VarName
=MONTH&VarDesc=Month&VarType=Num&VarName=DAY_OF_MONTH&VarDesc=DayofMonth&VarType=Num&VarName=DAY_OF_WEEK
&VarDesc=DayOfWeek&VarType=Num&VarName=FL_DATE&VarDesc=FlightDate&VarType=Char&VarName=UNIQUE_CARRIER
&VarDesc=UniqueCarrier&VarType=Char&VarName=AIRLINE_ID&VarDesc=AirlineID&VarType=Num&VarName=CARRIER
&VarDesc=Carrier&VarType=Char&VarName=TAIL_NUM&VarDesc=TailNum&VarType=Char&VarName=FL_NUM&VarDesc=FlightNum
&VarType=Char&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID
&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType
=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarName=ORIGIN_CITY_NAME&VarDesc=OriginCityName&VarType
=Char&VarName=ORIGIN_STATE_ABR&VarDesc=OriginState&VarType=Char&VarName=ORIGIN_STATE_FIPS&VarDesc=OriginStateFips
&VarType=Char&VarName=ORIGIN_STATE_NM&VarDesc=OriginStateName&VarType=Char&VarName=ORIGIN_WAC&VarDesc
=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID
&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID&VarType=Num
&VarName=DEST&VarDesc=Dest&VarType=Char&VarName=DEST_CITY_NAME&VarDesc=DestCityName&VarType=Char&VarName
=DEST_STATE_ABR&VarDesc=DestState&VarType=Char&VarName=DEST_STATE_FIPS&VarDesc=DestStateFips&VarType
=Char&VarName=DEST_STATE_NM&VarDesc=DestStateName&VarType=Char&VarName=DEST_WAC&VarDesc=DestWac&VarType
=Num&VarName=CRS_DEP_TIME&VarDesc=CRSDepTime&VarType=Char&VarName=DEP_TIME&VarDesc=DepTime&VarType=Char
&VarName=DEP_DELAY&VarDesc=DepDelay&VarType=Num&VarName=DEP_DELAY_NEW&VarDesc=DepDelayMinutes&VarType
=Num&VarName=DEP_DEL15&VarDesc=DepDel15&VarType=Num&VarName=DEP_DELAY_GROUP&VarDesc=DepartureDelayGroups
&VarType=Num&VarName=DEP_TIME_BLK&VarDesc=DepTimeBlk&VarType=Char&VarName=TAXI_OUT&VarDesc=TaxiOut&VarType
=Num&VarName=WHEELS_OFF&VarDesc=WheelsOff&VarType=Char&VarName=WHEELS_ON&VarDesc=WheelsOn&VarType=Char
&VarName=TAXI_IN&VarDesc=TaxiIn&VarType=Num&VarName=CRS_ARR_TIME&VarDesc=CRSArrTime&VarType=Char&VarName
=ARR_TIME&VarDesc=ArrTime&VarType=Char&VarName=ARR_DELAY&VarDesc=ArrDelay&VarType=Num&VarName=ARR_DELAY_NEW
&VarDesc=ArrDelayMinutes&VarType=Num&VarName=ARR_DEL15&VarDesc=ArrDel15&VarType=Num&VarName=ARR_DELAY_GROUP
&VarDesc=ArrivalDelayGroups&VarType=Num&VarName=ARR_TIME_BLK&VarDesc=ArrTimeBlk&VarType=Char&VarName
=CANCELLED&VarDesc=Cancelled&VarType=Num&VarName=CANCELLATION_CODE&VarDesc=CancellationCode&VarType=Char
&VarName=DIVERTED&VarDesc=Diverted&VarType=Num&VarName=CRS_ELAPSED_TIME&VarDesc=CRSElapsedTime&VarType
=Num&VarName=ACTUAL_ELAPSED_TIME&VarDesc=ActualElapsedTime&VarType=Num&VarName=AIR_TIME&VarDesc=AirTime
&VarType=Num&VarName=FLIGHTS&VarDesc=Flights&VarType=Num&VarName=DISTANCE&VarDesc=Distance&VarType=Num
&VarName=DISTANCE_GROUP&VarDesc=DistanceGroup&VarType=Num&VarName=CARRIER_DELAY&VarDesc=CarrierDelay
&VarType=Num&VarName=WEATHER_DELAY&VarDesc=WeatherDelay&VarType=Num&VarName=NAS_DELAY&VarDesc=NASDelay
&VarType=Num&VarName=SECURITY_DELAY&VarDesc=SecurityDelay&VarType=Num&VarName=LATE_AIRCRAFT_DELAY&VarDesc
=LateAircraftDelay&VarType=Num&VarDesc=FirstDepTime&VarType=Char&VarDesc=TotalAddGTime&VarType=Num&VarDesc
=LongestAddGTime&VarType=Num&VarDesc=DivAirportLandings&VarType=Num&VarDesc=DivReachedDest&VarType=Num
&VarDesc=DivActualElapsedTime&VarType=Num&VarDesc=DivArrDelay&VarType=Num&VarDesc=DivDistance&VarType
=Num&VarDesc=Div1Airport&VarType=Char&VarDesc=Div1AirportID&VarType=Num&VarDesc=Div1AirportSeqID&VarType
=Num&VarDesc=Div1WheelsOn&VarType=Char&VarDesc=Div1TotalGTime&VarType=Num&VarDesc=Div1LongestGTime&VarType
=Num&VarDesc=Div1WheelsOff&VarType=Char&VarDesc=Div1TailNum&VarType=Char&VarDesc=Div2Airport&VarType
=Char&VarDesc=Div2AirportID&VarType=Num&VarDesc=Div2AirportSeqID&VarType=Num&VarDesc=Div2WheelsOn&VarType
=Char&VarDesc=Div2TotalGTime&VarType=Num&VarDesc=Div2LongestGTime&VarType=Num&VarDesc=Div2WheelsOff&VarType
=Char&VarDesc=Div2TailNum&VarType=Char&VarDesc=Div3Airport&VarType=Char&VarDesc=Div3AirportID&VarType
=Num&VarDesc=Div3AirportSeqID&VarType=Num&VarDesc=Div3WheelsOn&VarType=Char&VarDesc=Div3TotalGTime&VarType
=Num&VarDesc=Div3LongestGTime&VarType=Num&VarDesc=Div3WheelsOff&VarType=Char&VarDesc=Div3TailNum&VarType
=Char&VarDesc=Div4Airport&VarType=Char&VarDesc=Div4AirportID&VarType=Num&VarDesc=Div4AirportSeqID&VarType
=Num&VarDesc=Div4WheelsOn&VarType=Char&VarDesc=Div4TotalGTime&VarType=Num&VarDesc=Div4LongestGTime&VarType
=Num&VarDesc=Div4WheelsOff&VarType=Char&VarDesc=Div4TailNum&VarType=Char&VarDesc=Div5Airport&VarType
=Char&VarDesc=Div5AirportID&VarType=Num&VarDesc=Div5AirportSeqID&VarType=Num&VarDesc=Div5WheelsOn&VarType
=Char&VarDesc=Div5TotalGTime&VarType=Num&VarDesc=Div5LongestGTime&VarType=Num&VarDesc=Div5WheelsOff&VarType
=Char&VarDesc=Div5TailNum&VarType=Char
'''

DB1BMARKETS_POST='''UserTableName=DB1BMarket&DBShortName=Origin_and_Destination_Survey&RawDataTable=T_DB1B_MARKET&sqlstr
=+SELECT+ITIN_ID%2CMKT_ID%2CMARKET_COUPONS%2CYEAR%2CQUARTER%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID
%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CORIGIN_COUNTRY%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_ABR%2CORIGIN_STATE_NM
%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CDEST_COUNTRY%2CDEST_STATE_FIPS
%2CDEST_STATE_ABR%2CDEST_STATE_NM%2CDEST_WAC%2CAIRPORT_GROUP%2CWAC_GROUP%2CTK_CARRIER_CHANGE%2CTK_CARRIER_GROUP
%2COP_CARRIER_CHANGE%2COP_CARRIER_GROUP%2CREPORTING_CARRIER%2CTICKET_CARRIER%2COPERATING_CARRIER%2CBULK_FARE
%2CPASSENGERS%2CMARKET_FARE%2CMARKET_DISTANCE%2CDISTANCE_GROUP%2CMARKET_MILES_FLOWN%2CNONSTOP_MILES%2CITIN_GEO_TYPE
%2CMKT_GEO_TYPE+FROM++T_DB1B_MARKET+WHERE+Quarter+%3D{quarter}+AND+YEAR%3D{year}&varlist=ITIN_ID%2CMKT_ID%2CMARKET_COUPONS
%2CYEAR%2CQUARTER%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CORIGIN_COUNTRY
%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_ABR%2CORIGIN_STATE_NM%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID
%2CDEST_CITY_MARKET_ID%2CDEST%2CDEST_COUNTRY%2CDEST_STATE_FIPS%2CDEST_STATE_ABR%2CDEST_STATE_NM%2CDEST_WAC
%2CAIRPORT_GROUP%2CWAC_GROUP%2CTK_CARRIER_CHANGE%2CTK_CARRIER_GROUP%2COP_CARRIER_CHANGE%2COP_CARRIER_GROUP
%2CREPORTING_CARRIER%2CTICKET_CARRIER%2COPERATING_CARRIER%2CBULK_FARE%2CPASSENGERS%2CMARKET_FARE%2CMARKET_DISTANCE
%2CDISTANCE_GROUP%2CMARKET_MILES_FLOWN%2CNONSTOP_MILES%2CITIN_GEO_TYPE%2CMKT_GEO_TYPE&grouplist=&suml
=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time=Q+{quarter}&timename=Quarter&GEOGRAPHY=All&XYEAR
={year}&FREQUENCY=1&VarName=ITIN_ID&VarDesc=ItinID&VarType=Num&VarName=MKT_ID&VarDesc=MktID&VarType=Num
&VarName=MARKET_COUPONS&VarDesc=MktCoupons&VarType=Num&VarName=YEAR&VarDesc=Year&VarType=Num&VarName
=QUARTER&VarDesc=Quarter&VarType=Num&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName
=ORIGIN_AIRPORT_SEQ_ID&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID
&VarType=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarName=ORIGIN_COUNTRY&VarDesc=OriginCountry
&VarType=Char&VarName=ORIGIN_STATE_FIPS&VarDesc=OriginStateFips&VarType=Char&VarName=ORIGIN_STATE_ABR
&VarDesc=OriginState&VarType=Char&VarName=ORIGIN_STATE_NM&VarDesc=OriginStateName&VarType=Char&VarName
=ORIGIN_WAC&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName
=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID
&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarName=DEST_COUNTRY&VarDesc=DestCountry&VarType
=Char&VarName=DEST_STATE_FIPS&VarDesc=DestStateFips&VarType=Char&VarName=DEST_STATE_ABR&VarDesc=DestState
&VarType=Char&VarName=DEST_STATE_NM&VarDesc=DestStateName&VarType=Char&VarName=DEST_WAC&VarDesc=DestWac
&VarType=Num&VarName=AIRPORT_GROUP&VarDesc=AirportGroup&VarType=Char&VarName=WAC_GROUP&VarDesc=WacGroup
&VarType=Char&VarName=TK_CARRIER_CHANGE&VarDesc=TkCarrierChange&VarType=Num&VarName=TK_CARRIER_GROUP
&VarDesc=TkCarrierGroup&VarType=Char&VarName=OP_CARRIER_CHANGE&VarDesc=OpCarrierChange&VarType=Num&VarName
=OP_CARRIER_GROUP&VarDesc=OpCarrierGroup&VarType=Char&VarName=REPORTING_CARRIER&VarDesc=RPCarrier&VarType
=Char&VarName=TICKET_CARRIER&VarDesc=TkCarrier&VarType=Char&VarName=OPERATING_CARRIER&VarDesc=OpCarrier
&VarType=Char&VarName=BULK_FARE&VarDesc=BulkFare&VarType=Num&VarName=PASSENGERS&VarDesc=Passengers&VarType
=Num&VarName=MARKET_FARE&VarDesc=MktFare&VarType=Num&VarName=MARKET_DISTANCE&VarDesc=MktDistance&VarType
=Num&VarName=DISTANCE_GROUP&VarDesc=MktDistanceGroup&VarType=Num&VarName=MARKET_MILES_FLOWN&VarDesc=MktMilesFlown
&VarType=Num&VarName=NONSTOP_MILES&VarDesc=NonStopMiles&VarType=Num&VarName=ITIN_GEO_TYPE&VarDesc=ItinGeoType
&VarType=Num&VarName=MKT_GEO_TYPE&VarDesc=MktGeoType&VarType=Num'''



#old db1b  markets (fewer fields)
'''UserTableName=DB1BMarket&DBShortName=Origin_and_Destination_Survey&RawDataTable=T_DB1B_MARKET&sqlstr
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

DB1BCOUPONS_POST='''UserTableName=DB1BCoupon&DBShortName=Origin_and_Destination_Survey&RawDataTable=T_DB1B_COUPON&sqlstr
=+SELECT+ITIN_ID%2CMKT_ID%2CSEQ_NUM%2CCOUPONS%2CYEAR%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID
%2CQUARTER%2CORIGIN%2CORIGIN_COUNTRY%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_ABR%2CORIGIN_STATE_NM%2CORIGIN_WAC
%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CDEST_COUNTRY%2CDEST_STATE_FIPS
%2CDEST_STATE_ABR%2CDEST_STATE_NM%2CDEST_WAC%2CTRIP_BREAK%2CCOUPON_TYPE%2CTICKET_CARRIER%2COPERATING_CARRIER
%2CREPORTING_CARRIER%2CPASSENGERS%2CFARE_CLASS%2CDISTANCE%2CDISTANCE_GROUP%2CGATEWAY%2CITIN_GEO_TYPE
%2CCOUPON_GEO_TYPE+FROM++T_DB1B_COUPON+WHERE+Quarter+%3D{quarter}+AND+YEAR%3D{year}&varlist=ITIN_ID%2CMKT_ID%2CSEQ_NUM
%2CCOUPONS%2CYEAR%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CQUARTER%2CORIGIN
%2CORIGIN_COUNTRY%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_ABR%2CORIGIN_STATE_NM%2CORIGIN_WAC%2CDEST_AIRPORT_ID
%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CDEST_COUNTRY%2CDEST_STATE_FIPS%2CDEST_STATE_ABR
%2CDEST_STATE_NM%2CDEST_WAC%2CTRIP_BREAK%2CCOUPON_TYPE%2CTICKET_CARRIER%2COPERATING_CARRIER%2CREPORTING_CARRIER
%2CPASSENGERS%2CFARE_CLASS%2CDISTANCE%2CDISTANCE_GROUP%2CGATEWAY%2CITIN_GEO_TYPE%2CCOUPON_GEO_TYPE&grouplist
=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time=Q+{quarter}&timename=Quarter&GEOGRAPHY=All
&XYEAR={2007}&FREQUENCY=1&VarName=ITIN_ID&VarDesc=ItinID&VarType=Num&VarName=MKT_ID&VarDesc=MktID&VarType
=Num&VarName=SEQ_NUM&VarDesc=SeqNum&VarType=Num&VarName=COUPONS&VarDesc=Coupons&VarType=Num&VarName=YEAR
&VarDesc=Year&VarType=Num&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID
&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType
=Num&VarName=QUARTER&VarDesc=Quarter&VarType=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarName=ORIGIN_COUNTRY
&VarDesc=OriginCountry&VarType=Char&VarName=ORIGIN_STATE_FIPS&VarDesc=OriginStateFips&VarType=Char&VarName
=ORIGIN_STATE_ABR&VarDesc=OriginState&VarType=Char&VarName=ORIGIN_STATE_NM&VarDesc=OriginStateName&VarType
=Char&VarName=ORIGIN_WAC&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID
&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID
&VarDesc=DestCityMarketID&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarName=DEST_COUNTRY&VarDesc
=DestCountry&VarType=Char&VarName=DEST_STATE_FIPS&VarDesc=DestStateFips&VarType=Char&VarName=DEST_STATE_ABR
&VarDesc=DestState&VarType=Char&VarName=DEST_STATE_NM&VarDesc=DestStateName&VarType=Char&VarName=DEST_WAC
&VarDesc=DestWac&VarType=Num&VarName=TRIP_BREAK&VarDesc=Break&VarType=Char&VarName=COUPON_TYPE&VarDesc
=CouponType&VarType=Char&VarName=TICKET_CARRIER&VarDesc=TkCarrier&VarType=Char&VarName=OPERATING_CARRIER
&VarDesc=OpCarrier&VarType=Char&VarName=REPORTING_CARRIER&VarDesc=RPCarrier&VarType=Char&VarName=PASSENGERS
&VarDesc=Passengers&VarType=Num&VarName=FARE_CLASS&VarDesc=FareClass&VarType=Char&VarName=DISTANCE&VarDesc
=Distance&VarType=Num&VarName=DISTANCE_GROUP&VarDesc=DistanceGroup&VarType=Num&VarName=GATEWAY&VarDesc
=Gateway&VarType=Num&VarName=ITIN_GEO_TYPE&VarDesc=ItinGeoType&VarType=Num&VarName=COUPON_GEO_TYPE&VarDesc
=CouponGeoType&VarType=Num'''


#old db1b  coupons (fewer fields)
'''UserTableName=DB1BCoupon&DBShortName=&RawDataTable=T_DB1B_COUPON&sqlstr=+SELECT+ITIN_ID%2CMKT_ID%2CSEQ_NUM
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


T100_SEG_POST = '''UserTableName=T_100_Segment__All_Carriers&DBShortName=&RawDataTable=T_T100_SEGMENT_ALL_CARRIER&sqlstr
=+SELECT+DEPARTURES_SCHEDULED%2CDEPARTURES_PERFORMED%2CPAYLOAD%2CSEATS%2CPASSENGERS%2CFREIGHT%2CMAIL
%2CDISTANCE%2CRAMP_TO_RAMP%2CAIR_TIME%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CUNIQUE_CARRIER_NAME%2CUNIQUE_CARRIER_ENTITY
%2CREGION%2CCARRIER%2CCARRIER_NAME%2CCARRIER_GROUP%2CCARRIER_GROUP_NEW%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID
%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CORIGIN_CITY_NAME%2CORIGIN_STATE_ABR%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_NM
%2CORIGIN_COUNTRY%2CORIGIN_COUNTRY_NAME%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID
%2CDEST%2CDEST_CITY_NAME%2CDEST_STATE_ABR%2CDEST_STATE_FIPS%2CDEST_STATE_NM%2CDEST_COUNTRY%2CDEST_COUNTRY_NAME
%2CDEST_WAC%2CAIRCRAFT_GROUP%2CAIRCRAFT_TYPE%2CAIRCRAFT_CONFIG%2CYEAR%2CQUARTER%2CMONTH%2CDISTANCE_GROUP
%2CCLASS%2CDATA_SOURCE+FROM++T_T100_SEGMENT_ALL_CARRIER+WHERE+YEAR%3D{year}&varlist=DEPARTURES_SCHEDULED
%2CDEPARTURES_PERFORMED%2CPAYLOAD%2CSEATS%2CPASSENGERS%2CFREIGHT%2CMAIL%2CDISTANCE%2CRAMP_TO_RAMP%2CAIR_TIME
%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CUNIQUE_CARRIER_NAME%2CUNIQUE_CARRIER_ENTITY%2CREGION%2CCARRIER%2CCARRIER_NAME
%2CCARRIER_GROUP%2CCARRIER_GROUP_NEW%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID
%2CORIGIN%2CORIGIN_CITY_NAME%2CORIGIN_STATE_ABR%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_NM%2CORIGIN_COUNTRY
%2CORIGIN_COUNTRY_NAME%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST
%2CDEST_CITY_NAME%2CDEST_STATE_ABR%2CDEST_STATE_FIPS%2CDEST_STATE_NM%2CDEST_COUNTRY%2CDEST_COUNTRY_NAME
%2CDEST_WAC%2CAIRCRAFT_GROUP%2CAIRCRAFT_TYPE%2CAIRCRAFT_CONFIG%2CYEAR%2CQUARTER%2CMONTH%2CDISTANCE_GROUP
%2CCLASS%2CDATA_SOURCE&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time
=All%A0Months&timename=Month&GEOGRAPHY=All&XYEAR={year}&FREQUENCY=All&VarName=DEPARTURES_SCHEDULED&VarDesc
=DepScheduled&VarType=Num&VarName=DEPARTURES_PERFORMED&VarDesc=DepPerformed&VarType=Num&VarName=PAYLOAD
&VarDesc=Payload&VarType=Num&VarName=SEATS&VarDesc=Seats&VarType=Num&VarName=PASSENGERS&VarDesc=Passengers
&VarType=Num&VarName=FREIGHT&VarDesc=Freight&VarType=Num&VarName=MAIL&VarDesc=Mail&VarType=Num&VarName
=DISTANCE&VarDesc=Distance&VarType=Num&VarName=RAMP_TO_RAMP&VarDesc=RampToRamp&VarType=Num&VarName=AIR_TIME
&VarDesc=AirTime&VarType=Num&VarName=UNIQUE_CARRIER&VarDesc=UniqueCarrier&VarType=Char&VarName=AIRLINE_ID
&VarDesc=AirlineID&VarType=Num&VarName=UNIQUE_CARRIER_NAME&VarDesc=UniqueCarrierName&VarType=Char&VarName
=UNIQUE_CARRIER_ENTITY&VarDesc=UniqCarrierEntity&VarType=Char&VarName=REGION&VarDesc=CarrierRegion&VarType
=Char&VarName=CARRIER&VarDesc=Carrier&VarType=Char&VarName=CARRIER_NAME&VarDesc=CarrierName&VarType=Char
&VarName=CARRIER_GROUP&VarDesc=CarrierGroup&VarType=Num&VarName=CARRIER_GROUP_NEW&VarDesc=CarrierGroupNew
&VarType=Num&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID
&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType
=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarName=ORIGIN_CITY_NAME&VarDesc=OriginCityName&VarType
=Char&VarName=ORIGIN_STATE_ABR&VarDesc=OriginState&VarType=Char&VarName=ORIGIN_STATE_FIPS&VarDesc=OriginStateFips
&VarType=Char&VarName=ORIGIN_STATE_NM&VarDesc=OriginStateName&VarType=Char&VarName=ORIGIN_COUNTRY&VarDesc
=OriginCountry&VarType=Char&VarName=ORIGIN_COUNTRY_NAME&VarDesc=OriginCountryName&VarType=Char&VarName
=ORIGIN_WAC&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName
=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID
&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarName=DEST_CITY_NAME&VarDesc=DestCityName&VarType
=Char&VarName=DEST_STATE_ABR&VarDesc=DestState&VarType=Char&VarName=DEST_STATE_FIPS&VarDesc=DestStateFips
&VarType=Char&VarName=DEST_STATE_NM&VarDesc=DestStateName&VarType=Char&VarName=DEST_COUNTRY&VarDesc=DestCountry
&VarType=Char&VarName=DEST_COUNTRY_NAME&VarDesc=DestCountryName&VarType=Char&VarName=DEST_WAC&VarDesc
=DestWac&VarType=Num&VarName=AIRCRAFT_GROUP&VarDesc=AircraftGroup&VarType=Num&VarName=AIRCRAFT_TYPE&VarDesc
=AircraftType&VarType=Char&VarName=AIRCRAFT_CONFIG&VarDesc=AircraftConfig&VarType=Num&VarName=YEAR&VarDesc
=Year&VarType=Num&VarName=QUARTER&VarDesc=Quarter&VarType=Num&VarName=MONTH&VarDesc=Month&VarType=Num
&VarName=DISTANCE_GROUP&VarDesc=DistanceGroup&VarType=Num&VarName=CLASS&VarDesc=Class&VarType=Char&VarName
=DATA_SOURCE&VarDesc=DataSource&VarType=Char'''

SCHEDULE_B43_POST = '''UserTableName=Schedule_B_43_Inventory&DBShortName=Air_Carrier_Financial&RawDataTable=T_F41SCHEDULE_B43
&sqlstr=+SELECT+YEAR%2CCARRIER%2CCARRIER_NAME%2CMANUFACTURE_YEAR%2CUNIQUE_CARRIER_NAME%2CSERIAL_NUMBER
%2CTAIL_NUMBER%2CAIRCRAFT_STATUS%2COPERATING_STATUS%2CNUMBER_OF_SEATS%2CMANUFACTURER%2CMODEL%2CCAPACITY_IN_POUNDS
%2CACQUISITION_DATE%2CAIRLINE_ID%2CUNIQUE_CARRIER+FROM++T_F41SCHEDULE_B43+WHERE+YEAR%3D{year}&varlist=YEAR
%2CCARRIER%2CCARRIER_NAME%2CMANUFACTURE_YEAR%2CUNIQUE_CARRIER_NAME%2CSERIAL_NUMBER%2CTAIL_NUMBER%2CAIRCRAFT_STATUS
%2COPERATING_STATUS%2CNUMBER_OF_SEATS%2CMANUFACTURER%2CMODEL%2CCAPACITY_IN_POUNDS%2CACQUISITION_DATE
%2CAIRLINE_ID%2CUNIQUE_CARRIER&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=Not
+Applicable&time=Not+Applicable&timename=Annual&GEOGRAPHY=All&XYEAR={year}&FREQUENCY=All&VarName=YEAR&VarDesc
=Year&VarType=Num&VarName=CARRIER&VarDesc=Carrier&VarType=Char&VarName=CARRIER_NAME&VarDesc=CarrierName
&VarType=Char&VarName=MANUFACTURE_YEAR&VarDesc=ManufactureYear&VarType=Num&VarName=UNIQUE_CARRIER_NAME
&VarDesc=UniqueCarrierName&VarType=Char&VarName=SERIAL_NUMBER&VarDesc=SerialNumber&VarType=Char&VarName
=TAIL_NUMBER&VarDesc=TailNumber&VarType=Char&VarName=AIRCRAFT_STATUS&VarDesc=AircraftStatus&VarType=Char
&VarName=OPERATING_STATUS&VarDesc=OperatingStatus&VarType=Char&VarName=NUMBER_OF_SEATS&VarDesc=NumberOfSeats
&VarType=Num&VarName=MANUFACTURER&VarDesc=Manufacturer&VarType=Char&VarName=MODEL&VarDesc=Model&VarType
=Char&VarName=CAPACITY_IN_POUNDS&VarDesc=CapacityInPounds&VarType=Num&VarName=ACQUISITION_DATE&VarDesc
=AcquisitionDate&VarType=Char&VarName=AIRLINE_ID&VarDesc=AirlineID&VarType=Num&VarName=UNIQUE_CARRIER
&VarDesc=UniqueCarrier&VarType=Char'''
    
    
'''
generalized helper function to make forward request to certain bts table

'''
#send PORT request to BTS server at specified url, make appropriate request, extract and save fi;e
#post_data is htlm form data for requests
#post_vars is a dictionary of associated variabls to replace     
#outdir is directory to save files to
def bts_table_request(url,post_data,post_vars,outfile,data_dir):
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
        print(sys.exc_info())
        return 0
    try: 
        #get file handle for returned ZIP file
        z=zipfile.ZipFile(io.BytesIO(r.content),'r')
        #exctract csv file from returned bytecode, save in appropriate directory
        for filename in z.namelist():
            if filename.split('.')[-1] =='csv':
                z.extract(filename,data_dir)
                os.rename(data_dir + filename, data_dir + outfile)
        #just to prevent BTS from getting mad
        time.sleep(.5)
    except:
        print('IO error')
        print(post_vars)
        print(sys.exc_info())
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
    
def T100segments_download(post=T100_SEG_POST,years = [2007],root_filename = 'T100_SEGMENTS', outdir='C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'):
    #url for DB1B markets table    
    url = 'http://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=293&Has_Group=3&Is_Zipped=0'    
    #download table for each 
    for year in years:        
        #request variables           
        post_vars={'year':year} 
        print('downloading t100 segments {year}'.format(**post_vars))
        outfile = root_filename+'_'+str(year)+'.csv'
        post_data = post
        #make request
        status = bts_table_request(url,post_data,post_vars,outfile,outdir)
        if status==1:
            print('Done')
        else:
            print('Error')
    return('All files downloaded')
    
    
def B43_download(post=SCHEDULE_B43_POST,years = [2007],root_filename = 'SCHEDULE_B43', outdir='C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/'):
    #url for DB1B markets table    
    url = 'http://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=314&Has_Group=0&Is_Zipped=0'    
    #download table for each 
    for year in years:        
        #request variables           
        post_vars={'year':year} 
        print('downloading schedule b43 {year}'.format(**post_vars))
        outfile = root_filename+'_'+str(year)+'.csv'
        post_data = post
        #make request
        status = bts_table_request(url,post_data,post_vars,outfile,outdir)
        if status==1:
            print('Done')
        else:
            print('Error')
    return('All files downloaded')
  

def aotp_download(post=AOTP_POST,years = [2007,2008], months=list(range(1,13)), data_dir='C:/users/d29905p/Documents/airline_competition_paper/code/network_games/bts_data/', root_filename = 'AOTP_CS'):
    '''    
    https://github.com/isaacobezo/get_rita/blob/master/get_transtat_data.py
    https://public.tableau.com/s/blog/2013/08/data-scraping-part-iii-python
    http://docs.python-requests.org/en/latest/user/quickstart/
    '''    
    url='http://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=236&Has_Group=0&Is_Zipped=0'
    months_str = ['January','February','March','April','May','June','July','August','September','October','November','December']
  
    for year in years:
        for month in months:
            month_str = months_str[month-1]
            year = str(year)
            ########request variables           
            post_vars={'year':year,'month':month_str,'month_num':month,'frequency':"1"}
            print(post_vars)
            print('downloading aotp {year} {month}'.format(**post_vars))
            outfile = root_filename+'_'+str(year)+'_'+str(month)+'.csv'
            post_data = post
            #make request
            status = bts_table_request(url,post_data,post_vars,outfile,data_dir)
            if status==1:
                print('Done')
            else:
                print('Error')
          

    time.sleep(.5)
    return('All files downloaded')
    
    
    
    
    
    
    

    
