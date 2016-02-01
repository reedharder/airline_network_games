# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 13:34:15 2016

@author: d29905p
"""

#analysis of markets and market-carrier combinations

import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn.decomposition.pca import PCA
import statsmodels.formula.api as sm



major_carriers_2014 = ['DL','WN','UA','US','AA','B6','NK','AS','F9','VX']



data_dir = "O:/documents/airline_competition_paper/code/network_games/"

t100ranked_mktmod_file = data_dir + "processed_data/nonstop_competitive_markets_mktmod_ope2014.csv"

major_carriers = major_carriers_2014



mdata = pd.read_csv(t100ranked_mktmod_file)
# new featues
mdata['SEATS_PER_FLIGHT'] = mdata['SEATS']/mdata['DAILY_FREQ']
mdata['ADDITIVE_ADJ'] = mdata['new_market']/mdata['MARKET_TOT']
#PERHAPS ADD FEATURE FOR REGIoNAL CARRIER, ALLIANCES....NEED STATISTICAL MEASURES OF
PCA_features = ['DISTANCE','FLIGHT_COST','MARKET_TOT','SEATS_PER_FLIGHT','ADDITIVE_ADJ','MARKET_COMPETITORS']
PCA_features_freq = PCA_features + ['DAILY_FREQ']

pca_data = mdata[mdata['UNIQUE_CARRIER'].isin(major_carriers)]
target_carrier =  pca_data['UNIQUE_CARRIER']
target_market = pca_data['BI_MARKET']
target_frequency = pca_data['DAILY_FREQ']
pca_data = pca_data[PCA_features]

##pd.tools.plotting.scatter_matrix(pca_data)
def airline_pca():
    X = np.array(pca_data)
    pca = PCA(n_components=3)
    pca.fit(X)
    Y=pca.transform(normalize(X))
    
    fig = plt.figure(1, figsize=(8, 6))
    ax = Axes3D(fig, elev=-150, azim=110)
    colordict = {carrier:i for i,carrier in enumerate(major_carriers)}
    pointcolors  = [colordict[carrier] for carrier in target_carrier]
    ax.scatter(Y[:, 0], Y[:, 1], Y[:, 2], c=pointcolors)
    ax.set_title("First three PCA directions")
    ax.set_xlabel("1st eigenvector")
    ax.w_xaxis.set_ticklabels([])
    ax.set_ylabel("2nd eigenvector")
    ax.w_yaxis.set_ticklabels([])
    ax.set_zlabel("3rd eigenvector")
    ax.w_zaxis.set_ticklabels([])
    
def airline_regression():
    #COST PER MILE??? 
    ols_mat =pd.concat([pd.DataFrame(target_frequency),pca_data],axis=1)
    ols_mat['FLIGHT_COST_2'] =ols_mat['FLIGHT_COST']**2
    ols_mat['MARKET_TOT_2'] =ols_mat['MARKET_TOT']**2
    ols_mat['COST_DEMAND'] =ols_mat['FLIGHT_COST']*ols_mat['MARKET_TOT']
    ols_mat['DEMAND_COMPETITORS'] = ols_mat['MARKET_COMPETITORS']*ols_mat['MARKET_TOT']
    ols_mat['COST_COMPETITORS']  = ols_mat['MARKET_COMPETITORS']*ols_mat['FLIGHT_COST']
    fit_base = sm.ols(formula="DAILY_FREQ ~  FLIGHT_COST + FLIGHT_COST_2 + MARKET_TOT + MARKET_TOT_2 + SEATS_PER_FLIGHT +  MARKET_COMPETITORS", data = ols_mat).fit()
    fit_base.summary()
    fit_base = sm.ols(formula="DAILY_FREQ ~  FLIGHT_COST + FLIGHT_COST_2 + MARKET_TOT + DEMAND_COMPETITORS + MARKET_COMPETITORS", data = ols_mat).fit()
    fit_base.summary()
    preds =fit_base.predict()
    MAPE = sum(abs(target_frequency-preds))/sum(target_frequency)
   #.29....we can do better hopefully!
    
