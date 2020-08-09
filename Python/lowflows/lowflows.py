# -*- coding: utf-8 -*-

'''
Functions to process lowflows database bands
'''

import os, pdsql
import pandas as pd
import numpy as np
import datetime as dt


pd.options.display.max_columns = 100

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='August 2020'
############################################################################################

def getBandInfo(config):
    '''
    Get the band information from the database for the number of lowflow sites specified in the config file.
    Function returns a dataframe containing the bands, trig_min, and trig_max for all months for the specified sites.
    '''
    print 'Retrieving lowflow bands information from database'

    #-Get directory where link file to band description can be found
    LF_dir = config.get('LOWFLOWS', 'LF_dir')
    LF_sites = config.get('LOWFLOWS', 'LF_sites').split(',')
    LF_bandNoLinks = config.get('LOWFLOWS', 'LF_bandNoLinks').split(',')
    #-Get the band information from the Hydro database and calculate the months
    LF_df = pdsql.mssql.rd_sql('sql2012test01', 'Hydro', 'LowFlowRestrSiteBand', col_names = ['site', 'date', 'band_num', 'waterway','location', 'min_trig','max_trig'], where_col = {'site': LF_sites})#, 'date': [bandDate]})
    LF_df['month'] = pd.to_datetime(LF_df['date'])
    LF_df['month'] = LF_df['month'].dt.month
    LF_df.drop('date', axis=1, inplace=True)
    #-Group by site, month, and band number, and keep the most recent 
    LF_df = LF_df.groupby(['site', 'month', 'band_num']).last().reset_index()
    #-Create a copy of the final database to be used for merging with the links table
    LF_df_copy = LF_df.copy()
    
    for s in LF_sites:
        siteIndex = LF_sites.index(s)
        #-Get the table that links band numbers to band descriptions and add to the dataframe
        links = pd.read_csv(os.path.join(LF_dir, LF_bandNoLinks[siteIndex]), index_col=False)
        links.columns = ['BandNo', 'BandDesc']
        #-Merge with the band description links
        temp_df = pd.merge(LF_df_copy.loc[LF_df_copy['site']==s], links, how='left', left_on='band_num', right_on='BandNo').drop('BandNo', axis=1)
        #-Fill the final database with the merged copy values of the band description
        LF_df.loc[LF_df['site']==s, 'BandDesc'] = temp_df['BandDesc'].values
    
    #-Drop the indexes for which no description is available. This means only the actual state of the banding system is used for the model 
    LF_df.dropna(inplace=True)
    #-Write to csv-file
    LF_df.to_csv(os.path.join(LF_dir, 'model_bands.csv'), index=False)
    
    #-display some information about the bands
    for s in LF_sites:
        bands = len(pd.unique(LF_df.loc[LF_df['site']==s, 'band_num']))
        site = LF_df.loc[LF_df['site']==s, 'waterway']; site = site.iloc[0]
        print 'Lowflow site ' + site + ' has ' + str(bands) + ' bands'

    #-Create site names by combining the fields 'waterway' with 'location'
    LF_df['LF_site_name'] = LF_df['waterway'] + ' at ' + LF_df['location']

    return LF_df

def addIRFsite(WEAP, config, LF_df, sdate, edate):
    '''
    Add the Irrigation Restriction Flow (IRF) sites to the model. Number of IRF sites corresponds with number of LF sites as specified in the config file.
    '''
    
    #-Get directory where link file to band description can be found
    LF_dir = config.get('LOWFLOWS', 'LF_dir')
    #-set the start date to current accounts year to make sure it gets time-series from currents accounts year till end of simulations
    year = sdate.year-1
    sdate = dt.date(year, sdate.month, sdate.day)
    
    #-if IRF branch does not exist yet under key assumptions, then add it. If it exists, then remove it and update with sites
    ka = WEAP.Branch('\Key Assumptions')
    if WEAP.BranchExists('\Key Assumptions\IRF'):
        print WEAP.Branch('\Key Assumptions\IRF').Name + ' has been deleted'
        WEAP.Branch('\Key Assumptions\IRF').Delete()
    irf = ka.AddChild('IRF')
    print irf.Name + ' has been added as branch to Key Assumptions'

    #-get the IRF sites that need to be added
    siteIDs = list(pd.unique(LF_df['site']))
    #siteNames = list(pd.unique(LF_df['waterway']))
    siteNames = list(pd.unique(LF_df['LF_site_name']))
    #-get the IRF lowflow time-series from the database 
    LF_df = pdsql.mssql.rd_sql('sql2012test01', 'Hydro', 'LowFlowRestrSite', col_names = ['site', 'date', 'waterway','location', 'flow'], where_col = {'site': siteIDs})#, 'date': [bandDate]})
    #-select only period of interest
    LF_df = LF_df.loc[(LF_df['date']>=sdate) & (LF_df['date']<=edate)]
    
    for ID in siteIDs:
        siteName = siteNames[siteIDs.index(ID)]
        print 'Adding lowflows time-series for ' + siteName
        b = WEAP.Branch('\Key Assumptions\IRF').AddChild(siteName)
        #-add database branch to each lowflow site (simulated can be added later on if goal is to compare simulated lowflow with lowflows from database
        b= b.AddChild('database')
        #-get time-series of site, write to csv
        ts = LF_df.loc[LF_df['site']==ID,['date', 'flow']]
        ts['date'] = pd.to_datetime(ts['date']).dt.strftime('%d/%m/%Y')
        ts.set_index('date', inplace=True)
        #-write to csv
        ts.to_csv(os.path.join(LF_dir, ID + '_IRF.csv'))
        #-add time-series to WEAP
        b.Variables('Annual Activity Level').Expression = 'ReadFromFile(' + os.path.join(LF_dir, ID + '_IRF.csv') + ', 1, , , , Interpolate)'


def addBands(WEAP, config, LF_df):
    
    #-if Low Flows branch does not exist yet under key assumptions, then add it. If it exists, then remove it and update with low flow sites and bands
    ka = WEAP.Branch('\Key Assumptions')
    if WEAP.BranchExists('\Key Assumptions\Low Flows'):
        print WEAP.Branch('\Key Assumptions\Low Flows').Name + ' has been deleted'
        WEAP.Branch('\Key Assumptions\Low Flows').Delete()
    lf = ka.AddChild('Low Flows')
    print lf.Name + ' has been added as branch to Key Assumptions'
    #-Create site names by combining the fields 'waterway' with 'location'
    LF_df['LF_site_name'] = LF_df['waterway'] + ' at ' + LF_df['location']
    #-get the IRF sites that need to be added
    siteIDs = list(pd.unique(LF_df['site']))
    siteNames = list(pd.unique(LF_df['LF_site_name']))
    #-IRF source (simulated or database) to use for each of the LF sites
    IRF_sources = config.get('LOWFLOWS', 'IRF_source').split(',')
    for ID in siteIDs:
        siteName = siteNames[siteIDs.index(ID)]
        IRF_source = IRF_sources[siteIDs.index(ID)]
        print 'Adding bands for low flow site: ' + siteName
        b = WEAP.Branch('\Key Assumptions\Low Flows').AddChild(siteName)
        df = LF_df.loc[LF_df['site']==ID]
        bands = pd.unique(df['band_num'])
        #-Loop over the bands and add the min_trig and max_trig and Ballocated
        for band in bands:
            bb = b.AddChild('band_num_' + str(band))
            bmax = bb.AddChild('max_trig')
            bmin = bb.AddChild('min_trig')
            #-Make lookup function for max_trig and add to branch
            lookup_str = 'Lookup(X, Y, Step, Month, '
            for m in range(1,12+1):
                try:
                    bmax_value = df.loc[(df['band_num']==band) & (df['month']==m),'max_trig'].values[0]
                except:
                    bmax_value = ''
                    print 'WARNING: No max_trig value was found for ' + siteName + ' month ' + str(m) + ' band ' + str(band) 
                if m<12:
                    lookup_str = lookup_str + str(m) + ', ' + str(bmax_value) + ', '
                else:
                    lookup_str = lookup_str + str(m) + ', ' + str(bmax_value) + ')'
            bmax.Variables('Annual Activity Level').Expression = lookup_str
            #-Make lookup function for min_trig and add to branch
            lookup_str = 'Lookup(X, Y, Step, Month, '
            for m in range(1,12+1):
                try:
                    bmin_value = df.loc[(df['band_num']==band) & (df['month']==m),'min_trig'].values[0]
                except:
                    bmin_value =''
                    print 'WARNING: No min_trig value was found for ' + siteName + ' month ' + str(m) + ' band ' + str(band)
                if m<12:
                    lookup_str = lookup_str + str(m) + ', ' + str(bmin_value) + ', '
                else:
                    lookup_str = lookup_str + str(m) + ', ' + str(bmin_value) + ')'
            bmin.Variables('Annual Activity Level').Expression = lookup_str

            #-Calculate Ballocated using IRF and max_trig and max_trig
            ballocated = 'If(max_trig=min_trig, If(Key\\IRF\\' + siteName + '\\' + IRF_source + '>=max_trig, 1, 0),Max(0, Min(1, (Key\\IRF\\' + siteName + '\\' + IRF_source + '-min_trig)/(max_trig-min_trig))))'
            ballo = bb.AddChild('Ballocated')
            ballo.Variables('Annual Activity Level').Expression = ballocated
        
#         #-Add no restriction band
#         bb = b.AddChild('No restriction')
#         ballo = bb.AddChild('Ballocated')
#         ballo.Variables('Annual Activity Level').Expression = 1.0
            
            
                
            
            
            
            
            
