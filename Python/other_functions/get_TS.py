# -*- coding: utf-8 -*-

import datetime as dt
import pandas as pd
import pdsql

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='August 2020'
############################################################################################

class getTimeSeries():
     
    def __init__(self, config):
        #-server and database settings
        self.server = 'edwprod01'
        self.database = 'Hydro'
        #-tables
        self.sitesTable = 'ExternalSite'
        self.mTypeTable = 'MeasurementType'
        self.datasetActiveTable = 'vDatasetTypeNamesActive'
        self.dSummaryTable = 'TSDataNumericDailySumm'
        self.tsDailyTable = 'TSDataNumericDaily'
        #-config file
        self.config = config
        
        #-start date
        sdate = self.config.get('TSPROCESSING', 'TS_start').split(',')
        self.sdate = dt.date(int(sdate[0]), int(sdate[1]), int(sdate[2]))
        #-end date
        edate = self.config.get('TSPROCESSING', 'TS_end').split(',')
        self.edate = dt.date(int(edate[0]), int(edate[1]), int(edate[2]))

    def writeQobs(self):
        '''
        Gets observed daily streamflow from HydroDB and writes result to a csv-file that can be read by WEAP
        '''
        
        sites = [self.config.getint('TSPROCESSING','Qobs_ExtSiteID')]
        dtypes = [self.config.getint('TSPROCESSING','DatasetTypeID')]
        csv = self.config.get('TSPROCESSING','Qobs_csv')

        #-days that should be present in the specified period
        t = pdsql.mssql.rd_sql(self.server, self.database, self.tsDailyTable, col_names = ['ExtSiteID', 'DateTime', 'Value','DatasetTypeID', 'QualityCode'], where_in = {'DatasetTypeID': dtypes, 'ExtSiteID': sites})
        #-Remove missing values
        t = t.loc[t['QualityCode']!=100]
        t.sort_values(by=['DateTime', 'QualityCode'], ascending=[True, False], inplace=True)
        t = t.groupby('DateTime').first().reset_index()
        
        
        #-Select records between fmDate and toDate
        t = t.loc[(t['DateTime']>=self.sdate) & (t['DateTime']<=self.edate)]
        t = t[['DateTime', 'Value']]
        
        #-rename and organize columns and write to csv
        t['DateTime'] = pd.to_datetime(t['DateTime'])
        tt = pd.DataFrame()
        tt['Date'] = t['DateTime'].dt.strftime('%d/%m/%Y')
        tt['Q [m3/s]'] = t['Value']
        tt.to_csv(csv, index=False) 






# import pandas as pd
# import os
# import datetime
# import numpy as np
# 
# 
# workDir = r'C:\Active\Projects\Rakaia\MODEL\WEAP'
# 
# #-Hydrotel time-series
# FH_Hydrotel = 'FH.csv'
# SWR_Hydrotel = 'irrigation_release_flow.csv'
# RIRF_Hydrotel = 'RIRF.csv'
# 
# #-Read Fighting Hill Hydrotel csv into df
# FH_Hydrotel = pd.read_csv(os.path.join(workDir, FH_Hydrotel), parse_dates=[[1, 2]], dayfirst=True)
# FH_Hydrotel.drop(['Ident', 'Quality'], axis=1, inplace=True)
# FH_Hydrotel.rename(columns={FH_Hydrotel.columns[0]: 'DateTime' }, inplace=True)
# FH_Hydrotel.rename(columns={FH_Hydrotel.columns[1]: 'FH' }, inplace=True)
# FH_Hydrotel.set_index('DateTime', inplace=True)
# FH_Hydrotel = FH_Hydrotel.resample('15Min').mean()
# 
# #-Read Stored Water Release Hydrotel csv into df
# SWR_Hydrotel = pd.read_csv(os.path.join(workDir, SWR_Hydrotel), parse_dates=[[1, 2]], dayfirst=True)
# SWR_Hydrotel.drop(['Ident', 'Quality'], axis=1, inplace=True)
# SWR_Hydrotel.rename(columns={SWR_Hydrotel.columns[0]: 'DateTime' }, inplace=True)
# SWR_Hydrotel.rename(columns={SWR_Hydrotel.columns[1]: 'SWR' }, inplace=True)
# SWR_Hydrotel.set_index('DateTime', inplace=True)
# 
# #-Read Rakaia Irrigation Restriction Flow Hydrotel csv into df
# RIRF_Hydrotel = pd.read_csv(os.path.join(workDir, RIRF_Hydrotel), parse_dates=[[1, 2]], dayfirst=True)
# RIRF_Hydrotel.drop(['Ident', 'Quality'], axis=1, inplace=True)
# RIRF_Hydrotel.rename(columns={RIRF_Hydrotel.columns[0]: 'DateTime' }, inplace=True)
# RIRF_Hydrotel.rename(columns={RIRF_Hydrotel.columns[1]: 'RIRF' }, inplace=True)
# RIRF_Hydrotel.set_index('DateTime', inplace=True)
# 
# print FH_Hydrotel
# 
# print SWR_Hydrotel.head()
# 
# print RIRF_Hydrotel.head()
# 
# df = pd.concat([FH_Hydrotel, SWR_Hydrotel, RIRF_Hydrotel], axis=1)
# 
# df.to_csv(os.path.join(workDir, 'test.csv'))
