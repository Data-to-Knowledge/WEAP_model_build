# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import pdsql, os, sys
import datetime as dt

from groundwater.stream_depletion import Theis
from other_functions.reproject import reproject

# Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ = 'August 2020'
############################################################################################

pd.options.display.max_columns = 100

'''
***************** TABLES FROM sql02prod - DataWarehouse *********************************************

D_ACC_Act_Water_TakeWaterPermitAuthorisation - RecordNumber, Activity, B1_PER_ID3, ConsentedAnnualVolume_m3year, ComplexAllocations, HasAlowflowRestrictionCondition --> diverts are not part of this table
D_ACC_Act_Water_DivertWater_Water - RecordNumber, Activity, B1_PER_ID3, HasAlowflowRestrictionCondition, WAP, MaxRate_ls, Volume_m3, ConsecutiveDayPeriod --> only for divers. Surface- and Groundwter takes are part of the table above.
D_SW_WellsDetails - WellNo, SWAllocationZone, Depth --> filtering on SWAZ and cutoff depth Z
D_ACC_Act_Water_TakeWaterWAPAllocation - Activity, FromMonth, ToMonth, SWAZ, WAP, MaxRateForWAP_ls, AllocationRate_ls, CustomVol_m3', CustomPeriodDays, IncludeInSWAllocation, FirstStreamDepletionRate
D_ACC_Act_Water_TakeWaterPermitUse - MaxRate_ls, Volume_m3, ConsecutiveDayPeriod, WaterUse --> on consent level
D_ACC_Act_Discharge_ContaminantToWater - Discharge Rate (l/s), Volume (m3)
F_ACC_Permit - B1_ALT_ID, fmDate ,toDate ,toDateText, Given Effect To, Expires, OriginalRecord, ParentAuthorisations, ChildAuthorisations, HolderAddressFullName
D_ACC_Act_Water_AssociatedPermits - Combined Annual Volume and associated consents.

***************** TABLES FROM sql03prod - Wells *********************************************

Well_StreamDepletion_Locations - Well_No, NZTMX, NZTMY, Distance, T_Estimate, S
SCREEN_DETAILS - WELL_NO, TOP_SCREEN --> Details about screen depths (used to filter out topscreens with depth >  Z m)

***************** TABLES FROM edwprod01 - Hydro *********************************************

ExternalSite - ExtSiteID (WAP number), NZTMX, NZTMY
CrcAllo - crc, take_type, use_type
TSDataNumericDaily - daily time-series values (e.g. abstraction data)
'''



def get_CRC_DB(config):
    '''
    Returns two dataframes with:
        - All required consent data for WEAP model building for consents being active between sdate and edate, and located in the list of SWAZs.
    Returns a list with logged messages, which can be related to errors/warning when extracting the consents/WAP data
    Writes three csv-files as output:
        - All required consent data for WEAP model building for consents being active between sdate and edate, and located in the list of SWAZs.
        - Time-series of metered WAPs for the consents that have been selected in the dataframe above.
    '''

    # Extract period from config file for which to select the consents (active during that period)
    syear = config.getint('TIMINGS', 'syear') - 1
    smonth = config.getint('TIMINGS', 'smonth')
    sday = config.getint('TIMINGS', 'sday')
    sdate = dt.date(syear, smonth, sday)

    eyear = config.getint('TIMINGS', 'eyear')
    emonth = config.getint('TIMINGS', 'emonth')
    eday = config.getint('TIMINGS', 'eday')
    edate = dt.date(eyear, emonth, eday)

    # Get directory in which consent info resides
    crc_dir = config.get('CONSENTS', 'crc_dir')
    # Get SWAZs for which to select consents
    SWAZs = config.get('CONSENTS', 'SWAZS').split(',')
    # Get discharge consents from csv-file
    discharge_consents_csv = os.path.join(crc_dir, config.get('CONSENTS', 'discharge_csv'))
    # Get csv-file to write detailed consent information to
    crc_csv_out = os.path.join(crc_dir, config.get('CONSENTS', 'crc_csv_out'))
    # Get csv-file to write metered WAP time-series to
    wapTS_csv_out = os.path.join(crc_dir, config.get('CONSENTS', 'wapTS_csv_out'))

#     #-Get csv-file to write time-series with consent being active or inactive for each day during the simulation period
#     crc_active_csv = os.path.join(crc_dir, config.get('CONSENTS', 'crc_active_csv'))

    # Dictionary to convert string from month and to month to numbers
    month_dict = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
    # Dictionary to replace Yes/No values with 1,0
    yes_no_dict = {'Yes': 1, 'No': 0, 'yes': 1, 'no': 0, 'YES': 1, 'NO': 0, 'Complex': 1}
    # list to add warnings/erros
    lMessageList = []

    # Get consent numbers that are only related to takes and diverts
    print('Getting all surface- and groundwater take consents...')
    all_take_consents = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_TakeWaterPermitAuthorisation', col_names = ['RecordNumber'])
    all_take_consents.drop_duplicates(inplace=True)
    all_divert_consents = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_DivertWater_Water', col_names = ['RecordNumber'])
    all_divert_consents.drop_duplicates(inplace=True)
    all_take_divert_consents = pd.concat([all_take_consents, all_divert_consents])
    all_take_divert_consents.drop_duplicates(inplace=True)
    all_take_consents = None; all_divert_consents = None;

    #-Get all the WAPs that are within one of the selected SWAZs from the D_SW_WellsDetails table
    print('Filtering WAPs located within the selected Surface Water Allocation Zones...')
    SWAZ_WAPs = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_SW_WellsDetails', col_names = ['WellNo', 'SWAllocationZone', 'Depth'], where_in={'SWAllocationZone': SWAZs})
    SWAZ_WAPs.rename(columns={'SWAllocationZone':'SWAZ'}, inplace=True)

    # Filter out WAPs that OR have a screen depth <=Z m or a bore depth of <=Z (the or condition is needed because not all Wells have screens).
    # All Depths with NaNs are also considered because these could be WAPs related to diverts
    well_cutoff_depth = config.getfloat('CONSENTS', 'well_cutoff_depth')
    print('Filtering WAPs that have a screen depth or bore depth <= %.2f meter...' %well_cutoff_depth)
    #-First get the wells that have a depth <=Z m
    WAP_depth_Zm = SWAZ_WAPs.loc[(SWAZ_WAPs['Depth']<=well_cutoff_depth) | (pd.isna(SWAZ_WAPs['Depth'])), ['WellNo']]
    #-Get Wells with top_screen <=Z m
    WAP_screens = pdsql.mssql.rd_sql('sql03prod', 'Wells', 'SCREEN_DETAILS', col_names = ['WELL_NO', 'TOP_SCREEN'], where_in={'WELL_NO': SWAZ_WAPs['WellNo'].tolist()})
    WAP_screens = WAP_screens.groupby('WELL_NO')['TOP_SCREEN'].min().reset_index()
    WAP_screens = WAP_screens.loc[WAP_screens['TOP_SCREEN'] <= well_cutoff_depth]
    WAP_screens.rename(columns={'WELL_NO': 'WellNo'}, inplace=True)
    WAP_screens.drop('TOP_SCREEN', axis=1, inplace=True)
    #-Concat the two and only keep unique WAP numbers
    WAP_Zm = pd.concat([WAP_depth_Zm, WAP_screens])
    WAP_Zm = pd.unique(WAP_Zm['WellNo'])
    #-Keep only the SWAZ WAPs that have a depth<=Z m or a top screen that is <=Zm
    SWAZ_WAPs = SWAZ_WAPs.loc[SWAZ_WAPs['WellNo'].isin(WAP_Zm)]
    SWAZ_WAPs.drop_duplicates(inplace=True)
    WAP_depth_Zm = None; WAP_screens = None; WAP_Zm = None; del WAP_depth_Zm, WAP_screens, WAP_Zm

    #-Get all the consents related to the WAPs within the selected SWAZs
    SWAZ_WAP_consents1 = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_TakeWaterWAPAllocation', col_names = ['RecordNumber', 'WAP'], where_in={'WAP': list(SWAZ_WAPs['WellNo'])})
    SWAZ_WAP_consents2 = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_DivertWater_Water', col_names = ['RecordNumber', 'WAP'], where_in={'WAP': list(SWAZ_WAPs['WellNo'])})
    SWAZ_WAP_consents = pd.concat([SWAZ_WAP_consents1, SWAZ_WAP_consents2])
    SWAZ_WAP_consents.drop_duplicates(subset='RecordNumber', inplace=True)
    SWAZ_WAP_consents1 = None; SWAZ_WAP_consents2 = None
    #-Keep only the consents that are a divert or take
    SWAZ_WAP_consents = SWAZ_WAP_consents.loc[SWAZ_WAP_consents['RecordNumber'].isin(all_take_divert_consents['RecordNumber'])]

    #-Get all discharge consents located in the catchment of interest
    print('Get discharge consents from csv-file...')
    discharge_consents = pd.read_csv(discharge_consents_csv)
    discharge_consents.rename(columns={discharge_consents.columns[0]: 'RecordNumber', 'GIS_SWAllo':'SWAZ','NZTMX':'discharge_NZTMX', 'NZTMY':'discharge_NZTMY'}, inplace=True)
    all_take_divert_consents = None; del all_take_divert_consents

    #-merge consents into list of interested activity types (takes, diverts, and discharges)
    print('Merging take and divert consents with list of discharge consents...')
    all_consents = pd.concat([SWAZ_WAP_consents['RecordNumber'], discharge_consents['RecordNumber']])
    all_consents.drop_duplicates(inplace=True)
    SWAZ_WAP_consents = None; del SWAZ_WAP_consents

    #-Get all the consents from the F_ACC_Permit table from the DataWarehouse that are part of the all_consents selection
    df = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'F_ACC_Permit', col_names = ['B1_ALT_ID','fmDate','toDate','toDateText','Given Effect To','Expires','OriginalRecord','ParentAuthorisations','ChildAuthorisations','HolderAddressFullName'], where_in={'B1_ALT_ID': all_consents.tolist()})
    df['toDate'] = pd.to_datetime(df['toDate'], errors='coerce')
    df['fmDate'] = pd.to_datetime(df['fmDate'], errors='coerce')
    df['Given Effect To'] = pd.to_datetime(df['Given Effect To'], errors='coerce')
    df['Expires'] = pd.to_datetime(df['Expires'], errors='coerce')

    #-Select consents that were active between sdate and edate
    print('Filter consents that were active between %s and %s...' %(sdate.strftime('%d-%m-%Y'), edate.strftime('%d-%m-%Y')))
    df1 = df.loc[(df['toDate']>pd.Timestamp(sdate)) & (df['fmDate']<=pd.Timestamp(edate))]
    #-If 'Given Effect To' date is later than 'toDate', then consent was never active in between the fmDate-toDate period, and is therefore removed from the dataframe
    df1.loc[(df1['Given Effect To'] > df1['toDate']),:]=np.nan
    df2 = df1.dropna(how='all')
    df2 = df2.loc[pd.notna(df2['Given Effect To'])]
    #-If 'Given Effect To' date is later than 'fmDate', then the 'fmDate' field is set to 'Given Effect To'
    df2.loc[(df2['fmDate'] < df2['Given Effect To']),'fmDate']=  df2['Given Effect To']

    #-Unique consent numbers of 'OriginalRecord'
    ori_records = pd.unique(df2['OriginalRecord'])
    df2_columns = list(df2.columns)
    fmDate_index = df2_columns.index('fmDate')
    toDate_index = df2_columns.index('toDate')
    #-Make sure toDate is always 1 day before the fmDate of the child consent. Required to make sure that a consent isn't active twice on one day
    for c in ori_records:
        #-select the consents that belong to the same group (have same parent so to speak)
        df_short = df2.loc[df2['OriginalRecord']==c]
        for i in range(0,len(df_short)-1):
            toDate = df_short.iloc[i,toDate_index] #-toDate of current record
            fmDate = df_short.iloc[i+1,fmDate_index] #-fromDate of child record
            if toDate == fmDate: #-cannot be equal. If so, then decrease the todate of the current record with one day
                df_short.iloc[i, toDate_index] = toDate - dt.timedelta(days=1)
        df2.loc[df2['OriginalRecord']==c] = df_short
    #-get rid of old dataframes
    df = df2.copy()
    df1 = None; df2 = None; del df1, df2

    #-For consents that are active for one day, the toDate may now (because of extracting one day from toDate) be smaller than fmDate. Those records are removed
    df = df.loc[df['toDate']>=df['fmDate']]

    df = df[['B1_ALT_ID','fmDate','toDate','Given Effect To','HolderAddressFullName']] #-This dataframe contains all take, divert, and discharge consents for the specified period within the selected SWAZs
    df.rename(columns={'B1_ALT_ID': 'crc'}, inplace=True)

    #-it may be the case that given effect is empty because consent was never activated. This results in empty cells for 'Given Effect To'. These are dropped.
    df.dropna(inplace=True)
    #-drop discharge consents not part of the df
    discharge_consents = discharge_consents.loc[discharge_consents['RecordNumber'].isin(df['crc'])]

    #-Get dataframe of all water takes and diverts on consent level
    print('Retrieve take and divert info on consent level...')
    # Get the surface and groundwater takes for the selected consent numbers
    crcAllo1 = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_TakeWaterPermitAuthorisation', 
                                 col_names = ['RecordNumber', 'Activity', 'B1_PER_ID3', 'ConsentedAnnualVolume_m3year', 'ComplexAllocations',
                                              'HasAlowflowRestrictionCondition'], where_in={'RecordNumber': df['crc'].tolist()})
    crcAllo1.drop_duplicates(inplace=True)

    # Do the same for the diversions. For the diversions also the wap maximum rate (MaxRate_ls), wap max volume pro rata (Volume_m3), and wap return period (ConsecutiveDayPeriod) are extracted.
    crcAllo2 = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_DivertWater_Water', col_names = ['RecordNumber', 'Activity', 'B1_PER_ID3', 'HasAlowflowRestrictionCondition', 'WAP', 'MaxRate_ls', 'Volume_m3', 'ConsecutiveDayPeriod'], where_in = {'RecordNumber': df['crc'].tolist()})
    crcAllo2.drop_duplicates(inplace=True)

    # Concat together
    crcAllo = pd.concat([crcAllo1, crcAllo2], axis=0)
    crcAllo1 = None; crcAllo2 = None;
    # Get combined annual volume
    combVol = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_AssociatedPermits', col_names = ['RecordNumber', 'CombinedAnnualVol_m3'], where_in={'RecordNumber': df['crc'].tolist()})
    crcAllo = pd.merge(crcAllo, combVol, how='left', on='RecordNumber')
    combVol = None
    # Cleanup
    crcAllo.drop_duplicates(inplace=True)
    crcAllo.loc[crcAllo['ConsentedAnnualVolume_m3year'] == 0, 'ConsentedAnnualVolume_m3year'] = np.nan
    crcAllo.loc[crcAllo['CombinedAnnualVol_m3'] == 0, 'CombinedAnnualVol_m3'] = np.nan
    crcAllo.rename(columns={'RecordNumber': 'crc', 'ConsentedAnnualVolume_m3year': 'crc_ann_vol [m3]', 'CombinedAnnualVol_m3': 'crc_ann_vol_combined [m3]',
                            'ComplexAllocations': 'complex_allo', 'HasAlowflowRestrictionCondition': 'lowflow_restriction', 'MaxRate_ls': 'wap_max_rate [l/s]',
                            'Volume_m3': 'wap_max_vol_pro_rata [m3]', 'ConsecutiveDayPeriod': 'wap_return_period [d]', 'WAP': 'wap'}, inplace=True)
    # change yes/no, to 1/0
    crcAllo.replace({'complex_allo': yes_no_dict, 'lowflow_restriction': yes_no_dict}, inplace=True)
    crcAllo['complex_allo_comment'] = crcAllo['complex_allo']
    crcAllo.loc[crcAllo['complex_allo'] != 0, 'complex_allo'] = 1
    crcAllo.loc[crcAllo['complex_allo_comment'] == 0, 'complex_allo_comment'] = np.nan

    # consents for which complex_allo and lowflow_restriction have no value specified, it is assumed that these conditions are false and therefore set to 0.
    crcAllo.loc[pd.isna(crcAllo['complex_allo']),'complex_allo'] = 0
    crcAllo.loc[pd.isna(crcAllo['lowflow_restriction']),'lowflow_restriction'] = 0
    crcAllo['wap'] = crcAllo['wap'].str.upper()

    #-Get dataframe of all water takes and diverts on WAP level
    print('Retrieve take and divert info on WAP level...')
    crcWapAllo = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_TakeWaterWAPAllocation',
                                    col_names = ['RecordNumber', 'Activity', 'FromMonth', 'ToMonth', 'SWAllocationBlock', 'WAP', 'MaxRateForWAP_ls', 
                                                 'AllocationRate_ls', 'CustomVol_m3', 'CustomPeriodDays', 'IncludeInSWAllocation', 'FirstStreamDepletionRate'],
                                    where_in={'RecordNumber': df['crc'].tolist(), 'WAP': SWAZ_WAPs['WellNo'].tolist()})

    crcWapAllo.rename(columns={'RecordNumber': 'crc', 'SWAllocationBlock': 'allo_block', 'WAP': 'wap', 'MaxRateForWAP_ls': 'wap_max_rate [l/s]', 'FromMonth': 'from_month',
                               'ToMonth': 'to_month', 'AllocationRate_ls': 'wap_max_rate_pro_rata [l/s]', 'CustomVol_m3': 'wap_max_vol_pro_rata [m3]',
                               'CustomPeriodDays': 'wap_return_period [d]', 'IncludeInSWAllocation': 'in_sw_allo', 'FirstStreamDepletionRate': 'first_sd_rate [l/s]'}, inplace=True)

    #-A few waps were not in capitals, which results in errors in joins later on. Therefore all waps were capitalized
    crcWapAllo['wap'] = crcWapAllo['wap'].str.upper()
    crcWapAllo.replace({'from_month': month_dict, 'to_month': month_dict},inplace=True)
    #-if wap max pro rata volume is specified, but the return period itself not, then assume return period equals 1
    crcWapAllo.loc[(crcWapAllo['wap_max_vol_pro_rata [m3]']>0) & pd.isna(crcWapAllo['wap_return_period [d]']), 'wap_return_period [d]'] = 1
    #-WAPs with wap "wap_max_rate [l/s]" and "wap_max_rate_pro_rata [l/s]" both being zero do not have water take/divert related consent conditions and are therefore dropped
    crcWapAllo.loc[(crcWapAllo['wap_max_rate [l/s]']==0) & (crcWapAllo['wap_max_rate_pro_rata [l/s]']==0),:] = np.nan
    #-WAPs where wap_max_vol_pro_rata and wap_return_period are 0 are set to NaN
    crcWapAllo.loc[(crcWapAllo['wap_max_vol_pro_rata [m3]']==0) & (crcWapAllo['wap_return_period [d]']==0),['wap_max_vol_pro_rata [m3]', 'wap_return_period [d]']] = np.nan
    crcWapAllo.dropna(how='all', inplace=True)
    #-Replace yes/no in in_sw_allo with 1/0
    crcWapAllo.replace({'in_sw_allo': yes_no_dict}, inplace=True)

    print('Retrieve max rate, volume, and return period on consent level...')
    crcActWaterUse = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_TakeWaterPermitUse',
                                        col_names=['RecordNumber', 'MaxRate_ls', 'Volume_m3', 'ConsecutiveDayPeriod', 'WaterUse'],
                                        where_in = {'RecordNumber': df['crc'].tolist()})
    crcActWaterUse.rename(columns={'RecordNumber': 'crc', 'MaxRate_ls': 'crc_max_rate [l/s]', 'Volume_m3': 'crc_vol_return_period [m3]', 'ConsecutiveDayPeriod': 'crc_return_period [d]', 'WaterUse': 'Use'}, inplace=True)
    #-some consents have a crc_max_rate of '0', which is not possible. These are set to NaN
    crcActWaterUse.loc[crcActWaterUse['crc_max_rate [l/s]']=='0', 'crc_max_rate [l/s]'] = np.nan
    #-if return period volume is specified, but the return period itself not, then assume return period equals 1
    crcActWaterUse.loc[(crcActWaterUse['crc_vol_return_period [m3]']>0) & pd.isna(crcActWaterUse['crc_return_period [d]']), 'crc_return_period [d]'] = 1

    #-merge selected consents with WAPs (takes and diverts)
    print('Merge details on WAP level...')
    df1 = pd.merge(df, crcWapAllo, how='left', on='crc')
    #-Assign discharge to the blank take_types
    df1.loc[pd.isna(df1['Activity']),'Activity'] = 'Discharge water to water'
    #-from_month and to_month for discharge consents are set from 1 to 12
    df1.loc[df1['Activity']=='Discharge water to water','from_month'] = 1
    df1.loc[df1['Activity']=='Discharge water to water','to_month'] = 12
    crcWapAllo = None; del crcWapAllo
    df1.loc[df1['Activity'] == 'Take Surface Water', 'first_sd_rate [l/s]'] = np.nan

    #-get discharge consent conditions and merge
    print('Get discharge consent details and merge...')
    df_discharge = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Discharge_ContaminantToWater', col_names = ['RecordNo', 'Discharge Rate (l/s)', 'Volume (m3)'], where_in = {'RecordNo': discharge_consents['RecordNumber'].tolist()})
    df_discharge.rename(columns={'Discharge Rate (l/s)': 'discharge_rate [l/s]', 'Volume (m3)': 'discharge_volume [m3]'}, inplace=True)
    #-if rates and/or volumes are zero or missing, then drop rows

    df_discharge.loc[((df_discharge['discharge_rate [l/s]']==0) & (df_discharge['discharge_volume [m3]']==0)) | (pd.isna(df_discharge['discharge_rate [l/s]']) & (df_discharge['discharge_volume [m3]']==0)) |
                     ((df_discharge['discharge_rate [l/s]']==0) & pd.isna(df_discharge['discharge_volume [m3]'])),:] = np.nan
    #-if rates is specified, but volume not, or visa versa, then calculate volume based on rate or rate based on volume
    df_discharge.loc[((df_discharge['discharge_rate [l/s]']==0) | pd.isna(df_discharge['discharge_rate [l/s]'])) & (df_discharge['discharge_volume [m3]']>0),'discharge_rate [l/s]'] = df_discharge['discharge_volume [m3]'] / 86.4
    df_discharge.loc[((df_discharge['discharge_volume [m3]']==0) | pd.isna(df_discharge['discharge_volume [m3]'])) & (df_discharge['discharge_rate [l/s]']>0),'discharge_volume [m3]'] = df_discharge['discharge_rate [l/s]'] * 86.4
    df_discharge.loc[df_discharge['discharge_volume [m3]']<0, 'discharge_volume [m3]'] = np.nan
    df_discharge.loc[df_discharge['discharge_rate [l/s]']<0, 'discharge_rate [l/s]'] = np.nan
    df_discharge = df_discharge.loc[df_discharge['discharge_rate [l/s]']!=0]
    df_discharge.dropna(how='all', inplace=True)
    #-merge with discharge consent locations and WAPs df
    df_discharge = pd.merge(df_discharge, discharge_consents, how='left', left_on='RecordNo', right_on='RecordNumber')
    df_discharge.drop_duplicates(subset=['RecordNo', 'discharge_rate [l/s]', 'discharge_volume [m3]'], inplace=True)
    df_discharge.drop('RecordNo', axis=1, inplace=True)

    #-merge df1 with discharge consents
    df1 = pd.merge(df1, df_discharge, how='left', left_on='crc', right_on='RecordNumber')
    discharge_consents = None; df_discharge = None; del discharge_consents, df_discharge
    df1.drop('RecordNumber', axis=1, inplace=True)
    df1.loc[(pd.isna(df1['discharge_NZTMX'])) & (pd.isna(df1['discharge_NZTMX'])) & (df1['Activity']=='Discharge water to water'),:] = np.nan
    df1.dropna(how='all', inplace=True)

    # merge consent level dataframe that includes the diversion data
    diversions = crcAllo.loc[crcAllo.Activity == 'Divert Surface Water']
    #diversions.to_csv(r'C:\Active\Projects\Ahuriri_Jen\model\data\consents\test\diversions.csv')
    df1 = pd.concat([df1, diversions[['crc', 'Activity', 'wap', 'wap_max_rate [l/s]', 'wap_max_vol_pro_rata [m3]', 'wap_return_period [d]']]], axis=0)
    # all diverts should have in_sw_allo = 0
    df1.loc[df1.Activity == 'Divert Surface Water', 'in_sw_allo'] = 0

    diversions = None
    crcAllo.drop(['wap', 'wap_max_rate [l/s]', 'wap_max_vol_pro_rata [m3]', 'wap_return_period [d]'], axis=1, inplace=True)
    # copy missing blanks
    crc_unique = pd.unique(df1.crc).tolist()
    for c in crc_unique:
        df_sel = df.loc[(df.crc == c) & pd.notna(df.fmDate)]
        df1.loc[(df1.crc == c) & (df1.Activity == 'Divert Surface Water'), 'fmDate'] = df_sel.fmDate.iloc[0]
        df1.loc[(df1.crc == c) & (df1.Activity == 'Divert Surface Water'), 'toDate'] = df_sel.toDate.iloc[0]
        df1.loc[(df1.crc == c) & (df1.Activity == 'Divert Surface Water'), 'Given Effect To'] = df_sel['Given Effect To'].iloc[0]
        df1.loc[(df1.crc == c) & (df1.Activity == 'Divert Surface Water'), 'HolderAddressFullName'] = df_sel['HolderAddressFullName'].iloc[0]
    # Assume empty fields for from_month and to_month are from 1 to 12
    df1.loc[pd.isna(df1.from_month), 'from_month'] = 1
    df1.loc[pd.isna(df1.to_month), 'to_month'] = 12
    df = None; del df

    #-merge take and divert consents SWAZs
    df1 = pd.merge(df1, SWAZ_WAPs[['WellNo','SWAZ']], how='left', left_on='wap', right_on='WellNo')
    df1.drop('WellNo', axis=1, inplace=True)
    df1['SWAZ'] = np.nan
    df1.loc[pd.isna(df1['SWAZ_x']), 'SWAZ'] = df1['SWAZ_y']
    df1.loc[pd.isna(df1['SWAZ_y']), 'SWAZ'] = df1['SWAZ_x']
    df1.drop(['SWAZ_x', 'SWAZ_y'], axis=1, inplace=True)
    df1['wap'] = df1['wap'].astype(str)
    waps = pd.unique(df1['wap'])

    #-add the WAP NZTMX and NZTMY
    extsite_df = pdsql.mssql.rd_sql('edwprod01', 'Hydro', 'ExternalSite', col_names = ['ExtSiteID', 'NZTMX', 'NZTMY'], where_in = {'ExtSiteID': waps.tolist()})
    extsite_df.rename(columns={'NZTMX': 'wap_NZTMX', 'NZTMY': 'wap_NZTMY'}, inplace=True)
    extsite_df.drop_duplicates(inplace=True)
    df1 = pd.merge(df1, extsite_df, how='left', left_on='wap', right_on='ExtSiteID')
    df1.drop('ExtSiteID', axis=1, inplace=True)
    extsite_df = None; del extsite_df
    df1.loc[df1.wap =='nan', 'wap'] = np.nan

    #-get stream depletion info and merge with df1
    print('Get stream depletion details and merge...')
    waps = pd.unique(df1.loc[df1['Activity']=='Take Groundwater','wap'])
    sd_df = pdsql.mssql.rd_sql('sql03prod', 'Wells', 'Well_StreamDepletion_Locations', col_names = ['Well_No', 'NZTMX', 'NZTMY','Distance','T_Estimate','S'], where_in = {'Well_No': waps.tolist()})
    waps = None; del waps
    sd_df.rename(columns={'Well_No': 'wap', 'NZTMX': 'wap_sd_NZTMX', 'NZTMY': 'wap_sd_NZTMY'}, inplace=True)

    sd_df.set_index('wap', inplace=True)
    #-calculate Connectivity using one day of pumping (because model runs on a day-to-day basis)
    pump_days = 1
    sd_df['Connection'] = np.nan
    for i in sd_df.iterrows():
        distance, T, S = i[1]['Distance'], i[1]['T_Estimate'], i[1]['S']
        y = Theis(T, S, distance, 0, pump_days)
        connection = y[1]
        sd_df.loc[i[0],'Connection'] = connection
    sd_df.reset_index(inplace=True)
    df1 = pd.merge(df1, sd_df[['wap', 'wap_sd_NZTMX', 'wap_sd_NZTMY', 'Distance','T_Estimate','S', 'Connection']], how='left', on='wap')
    sd_df = None; del sd_df

    #-merge consent level dataframe
    df1 = pd.merge(df1, crcAllo, how='left', on=['crc','Activity'])
    crcAllo = None; del crcAllo

    #-check for associated consents and add to df1
    print('Get associated consents, and add these as a list of comma-separated consent numbers...')
    df1['B1_PER_ID3'] = df1['B1_PER_ID3'].astype(str)
    unique_B1_PER_ID3 = pd.unique(df1['B1_PER_ID3'])
    ass_crc_df = pdsql.mssql.rd_sql('sql02prod', 'DataWarehouse', 'D_ACC_Act_Water_AssociatedPermits', col_names = ['B1_PER_ID3', 'RecordNumber','RecordNumberASIT'], where_in = {'B1_PER_ID3': unique_B1_PER_ID3.tolist()})
    df1['associated_crcs'] = np.nan
    for i in unique_B1_PER_ID3:
        crc_list = ''
        ass_crc = ass_crc_df.loc[ass_crc_df['B1_PER_ID3'] == i, 'RecordNumberASIT'].values
        if len(ass_crc) > 0:
            for j in ass_crc:
                crc_list += j
                if j != ass_crc[-1]:
                    crc_list += ', '
            df1.loc[df1['B1_PER_ID3'] == i, 'associated_crcs'] = crc_list
    unique_B1_PER_ID3 = None; ass_crc = None; del unique_B1_PER_ID3, ass_crc

    #-add crcActWaterUse
    df1 = pd.merge(df1, crcActWaterUse, how='left', on='crc')
    crcActWaterUse = None; del crcActWaterUse

    #-Mike's allo table for water use
    print('Merging water use type and irrigated area...')
    hydro_crc_allo_df = pdsql.mssql.rd_sql('edwprod01', 'Hydro', 'CrcAllo', col_names = ['crc', 'take_type', 'use_type'], where_in = {'crc': df1['crc'].tolist()})
    df1 = pd.merge(df1, hydro_crc_allo_df, how='left', left_on=['crc', 'Activity'], right_on=['crc', 'take_type'])
    df1.drop('take_type', axis=1, inplace=True)
    hydro_crc_allo_df = None; del hydro_crc_allo_df
    df1.drop_duplicates(inplace=True)
    df1.loc[pd.isna(df1['use_type']), 'use_type'] = df1['Use']
    df1.drop('Use', axis=1, inplace=True)

    #-get metered data in [m3]
    print('Merging info on water meters...')
    df1['metered'] = np.nan
    df1['metered_fmDate'] = np.nan
    df1['metered_toDate'] = np.nan
    df1['wap'] = df1['wap'].astype(str)
    waps = list(pd.unique(df1['wap']))
    new_waps = []
    for w in waps:
        if '/' in w:
            new_waps.append(w)
    waps = new_waps; new_waps = None; 
    nrWaps = len(waps)
    df_meter = pd.DataFrame(index=pd.date_range(sdate, edate, freq='D'), columns=waps)
    df_meter.rename_axis('Date', inplace=True)
    #-get the wap abstraction data for rivers (9) and aquifer (12) for the waps present in the df1
    df = pdsql.mssql.rd_sql('edwprod01', 'Hydro', 'TSDataNumericDaily', col_names = ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_in = {'ExtSiteID': waps, 'DatasetTypeID': [9, 12]})
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    df = df.loc[(df['DateTime']>=pd.Timestamp(sdate)) & (df['DateTime']<=pd.Timestamp(edate))]
    df.rename(columns={'DateTime': 'Date'}, inplace=True)
    #-fill df_meter with abstraction data
    nrMeteredWaps = 0.
    for w in waps:
        #print w
        df_sel = df.loc[df['ExtSiteID']==w,['Date', 'Value']]
        df_sel.set_index('Date', inplace=True)
        #-Set non-reliable valuues (<0) to NaN
        df_sel.loc[df_sel['Value']<0] = np.nan
        df_sel.rename(columns={'Value': w}, inplace=True)
        if len(df_sel)>0:
            nrMeteredWaps+=1
            df1.loc[df1['wap']==w, 'metered'] = 1
            d = df_sel.loc[df_sel[w]>=0]
            sd = d.index[0].strftime('%d/%m/%Y')
            ed = d.index[-1].strftime('%d/%m/%Y')
            df1.loc[df1['wap']==w, 'metered_fmDate'] = sd 
            df1.loc[df1['wap']==w, 'metered_toDate'] = ed
        else:
            df1.loc[df1['wap']==w, 'metered'] = 0
        try:
            df_meter[[w]] = df_sel
        except:
            lMessage = w + ' has multiple DatasetTypeIDs assigned to it. This is not possible and needs to be checked; i.e. a WAP can not be a divert and surface water take at the same time!!'
            lMessageList.append(lMessage) 
            print(lMessage)
            #-use the first occuring entry two prevent double datasettypeids
            df_sel.reset_index(inplace=True)
            df_sel = df_sel.groupby('Date').first()
            df_meter[[w]] = df_sel
    percMetered = (nrMeteredWaps / nrWaps) * 100
    lMessage = '%.2f%% of the WAPs is metered.' %(percMetered)
    print(lMessage)
    lMessageList.append(lMessage)
    
    #-write WAP meter time-series to csv-file
    print('Writing metered time-series of WAPs to csv-file...')
    df_meter.reset_index(inplace=True)
    df_meter['Date'] = df_meter['Date'].dt.strftime('%d/%m/%Y')
    df_meter.set_index('Date', inplace=True)
    df_meter = df_meter.loc[:, df_meter.columns.notnull()]
    df_meter.to_csv(wapTS_csv_out)

    #-write final dataframe to csv-file
    print('Writing final consent/WAP/details dataframe to csv-file...')
    df1.drop_duplicates(inplace=True)

    ###-re-organize order of columns
    df_final = df1[['crc', 'fmDate', 'toDate', 'Given Effect To', 'HolderAddressFullName', 'Activity', 'use_type', 'from_month', 'to_month', 'SWAZ',
                'in_sw_allo', 'allo_block', 'complex_allo', 'complex_allo_comment', 'crc_ann_vol [m3]', 'crc_ann_vol_combined [m3]', 'crc_vol_return_period [m3]', 'crc_return_period [d]',
                'crc_max_rate [l/s]', 'associated_crcs', 'wap', 'wap_max_rate [l/s]', 'wap_max_rate_pro_rata [l/s]', 'wap_max_vol_pro_rata [m3]',
                'wap_return_period [d]', 'wap_NZTMX', 'wap_NZTMY', 'wap_sd_NZTMX', 'wap_sd_NZTMY', 'Distance', 'T_Estimate','S', 'Connection', 'discharge_rate [l/s]',
                'discharge_volume [m3]', 'discharge_NZTMX', 'discharge_NZTMY', 'metered', 'metered_fmDate', 'metered_toDate', 'lowflow_restriction']]
    df_final.loc[pd.isna(df_final['complex_allo']),'complex_allo'] = 0
    df_final.loc[pd.isna(df_final['lowflow_restriction']),'lowflow_restriction'] = 0
    df_final.loc[df_final.Activity == 'Discharge water to water', 'wap'] = np.nan

    df1 = None; del df1;
    df_final['fmDate'] = df_final['fmDate'].dt.strftime('%d/%m/%Y')
    df_final['toDate'] = df_final['toDate'].dt.strftime('%d/%m/%Y')
    df_final['Given Effect To'] = df_final['Given Effect To'].dt.strftime('%d/%m/%Y')
    df_final.to_csv(crc_csv_out, index=False)

    print('Finished filtering consent and WAP info.')

    return df_final, lMessageList

def get_CRC_CSV(self):    
    '''
    This function does the same as 'get_CRC_DB' except that it reads from a csv instead of from the databases.
    
    Returns two dataframes with:
        - All required consent data for WEAP model building for consents being active between sdate and edate, and located in the list of SWAZs.
    '''
    
    #-Get csv-file with detailed consent information
    crc_csv = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'crc_csv_out'))
    
    crc_df = pd.read_csv(crc_csv, parse_dates = [1,2,3,39,40], dayfirst=True)

    return crc_df


def filter_gw_sd_waps(self):
    '''
    Only keep the Groundwater Take WAPs that are specified in a csv-file under the variable: GW_SD_locations_csv
    Returns a shorter version of crc_df where the Groundwater Take WAPs that are not part of GW_SD_locations_csv have been removed
    '''
    
    #-Get csv-file with the Groundwater WAPs that should be implemented and read into dataframe
    GW_SD_locations_csv = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'GW_SD_locations_csv'))
    GW_SD_locations_df = pd.read_csv(GW_SD_locations_csv)
    #-keep only the groundwater waps in crc_df that are part of the gw_sd_locations.csv
    waps = pd.unique(GW_SD_locations_df['wap'])
    #-Select the WAPs/consents and concat
    df1 = self.crc_df.loc[(self.crc_df['Activity']=='Take Groundwater') & self.crc_df['wap'].isin(waps)]
    df2 = self.crc_df.loc[self.crc_df['Activity']!='Take Groundwater']
    self.crc_df = pd.concat([df1, df2])
    
    
def filter_sw_waps(self):
    '''
    Only keep the Surface Water Take WAPs that are specified in a csv-file under the variable: SW_locations_csv
    Returns a shorter version of crc_df where the Surface Water Take WAPs that are not part of SW_locations_csv have been removed
    '''
    #-Get csv-file with the Surface Water Take WAPs that should be implemented and read into dataframe
    SW_locations_csv = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'SW_locations_csv'))
    SW_locations_df = pd.read_csv(SW_locations_csv)
    #-keep only the surface water take waps in crc_df that are part of the SW_locations_csv
    waps = pd.unique(SW_locations_df['wap'])
    #-Select the WAPs/consents and concat
    df1 = self.crc_df.loc[(self.crc_df['Activity']=='Take Surface Water') & self.crc_df['wap'].isin(waps)]
    df2 = self.crc_df.loc[self.crc_df['Activity']!='Take Surface Water']
    self.crc_df = pd.concat([df1, df2])
    
def filter_divert_waps(self):
    '''
    Only keep the Divert WAPs that are specified in a csv-file under the variable: Divert_locations_csv
    Returns a shorter version of crc_df where the Divert WAPs that are not part of Divert_locations_csv have been removed
    '''
    #-Get csv-file with the Divert WAPs that should be implemented and read into dataframe
    Divert_locations_csv = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'Divert_locations_csv'))
    Divert_locations_df = pd.read_csv(Divert_locations_csv)
    #-keep only the divert waps in crc_df that are part of the Divert_locations_csv
    waps = pd.unique(Divert_locations_df['wap'])
    #-Select the WAPs/consents and concat
    df1 = self.crc_df.loc[(self.crc_df['Activity']=='Divert Surface Water') & self.crc_df['wap'].isin(waps)]
    df2 = self.crc_df.loc[self.crc_df['Activity']!='Divert Surface Water']
    self.crc_df = pd.concat([df1, df2])
    
    
def filter_discharge_consents(self):
    '''
    Only keep the discharge consents that are specified in a csv-file under the variable: Discharge_locations_csv
    Returns a shorter version of crc_df where the discharge consents that are not part of Discharge_locations_csv have been removed
    '''
    #-Get csv-file with the discharge consents that should be implemented and read into dataframe
    Discharge_locations_csv = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'Discharge_locations_csv'))
    Discharge_locations_df = pd.read_csv(Discharge_locations_csv)
    #-keep only the discharge consents in crc_df that are part of the Discharge_locations_csv
    crcs = pd.unique(Discharge_locations_df['crc'])
    #-Select the consents and concat
    df1 = self.crc_df.loc[(self.crc_df['Activity']=='Discharge water to water') & self.crc_df['crc'].isin(crcs)]
    df2 = self.crc_df.loc[self.crc_df['Activity']!='Discharge water to water']
    self.crc_df = pd.concat([df1, df2])         

def cleanup_crc_df(self):
    '''
    Check for some errors in the consenst/wap dataframe and fix those using reasonable assumptions. Returns a corrected dataframe and
    writes same dataframe to a csv-file. All consent/wap model data is based on the returned crc_df
    '''
    csv_file = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'crc_csv_out_final'))
    
    
    #-if max_max_vol_pro_rata is specified, but wap_return_period not, then assume return period = 1
    self.crc_df.loc[(pd.isna(self.crc_df['wap_return_period [d]'])) & (pd.notna(self.crc_df['wap_max_vol_pro_rata [m3]'])), 'wap_return_period [d]'] = 1
    #-if wap_return_period is specified, but the max_max_vol_pro_rata not, then set both to NaN
    self.crc_df.loc[(pd.isna(self.crc_df['wap_max_vol_pro_rata [m3]'])) & (pd.notna(self.crc_df['wap_return_period [d]'])), 'wap_return_period [d]'] = np.nan

    #-if crc_vol_return_period [m3] is specified, but crc_return_period [d] not, then assume return period = 1
    self.crc_df.loc[(pd.isna(self.crc_df['crc_return_period [d]'])) & (pd.notna(self.crc_df['crc_vol_return_period [m3]'])), 'crc_return_period [d]'] = 1
    #-if crc_return_period [d] is specified, but the crc_vol_return_period [m3] not, then set both to NaN
    self.crc_df.loc[(pd.isna(self.crc_df['crc_vol_return_period [m3]'])) & (pd.notna(self.crc_df['crc_return_period [d]'])), 'crc_return_period [d]'] = np.nan
    
    self.crc_df.drop_duplicates(inplace=True)
    
    #-check if there are WAPs within the same consent that have the same activity assigned more than once (rare situation, but found a few). E.g. this may happen
    #-when a WAP has a e.g. Take Surface Water as activity with different volumes for three different periods throughout the year.
    df = self.crc_df.loc[self.crc_df['Activity']!='Discharge water to water']
    crc_unique = pd.unique(df['crc'])
    for c in crc_unique:
        waps = df.loc[df['crc']==c, 'wap_name']
        waps_unique = pd.unique(waps)
        if len(waps)>len(waps_unique):
            for w in waps_unique:
                ww = df.loc[(df['crc']==c) & (df['wap_name']==w), ['wap_name']]
                if len(ww)>1:
                    j = 0
                    for i in ww.iterrows():
                        j+=1
                        lname = i[1]['wap_name'] + '_' + str(j) 
                        self.crc_df.loc[i[0], 'wap_name_long'] = lname
    
    #-create a "use_type_renamed" field that is basically a reclassification of the use_type field using the same reclassification as done for the analyse_demand_meters.py
    use_type_dict = {'Irrigation - Pasture': 'Irrigation', 'Irrigation - Mixed': 'Irrigation', 'Aquaculture': 'Other', 'Community Water Supply': 'Domestic', 'Cooling Water (non HVAC)': 'Hydropower',
                     'Recreation/Sport': 'Other', 'Domestic Use': 'Domestic', 'Irrigation - Arable (Cropping)': 'Irrigation', 'Firefighting': 'Other',
                     'Industrial Use - Other': 'Other', 'Construction': 'Other', 'Augment Flow/Wetland': 'Other', 'Power Generation': 'Hydropower', 'Viticulture': 'Other'}
    self.crc_df['use_type_renamed'] = self.crc_df['use_type']
    self.crc_df.replace({'use_type_renamed': use_type_dict},inplace=True)
    
    #-link consumption to consent/wap based on use_type_renamed
    try:
        consumptionF = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'consumption'))
        cons_df = pd.read_csv(consumptionF)
        cons_df.columns = ['use_type', 'consumption']
        self.crc_df = pd.merge(self.crc_df, cons_df, how='left', left_on='use_type_renamed', right_on='use_type')
        self.crc_df.drop('use_type_y', axis=1, inplace=True)
        self.crc_df.rename(columns={'use_type_x': 'use_type' }, inplace=True)
    except:
        print('Warning: consumption csv-file not found or not specified. Consumption cannot be set for demand nodes!')
    #-cleanup
    self.crc_df.drop_duplicates(inplace=True)
    self.crc_df.sort_values(by='crc', inplace=True)
    self.crc_df.to_csv(csv_file, index=False)
    
    

#def crc_wap_active_ts(config, crc_df):
def crc_wap_active_ts(self):    
    '''
    Returns:
        - Time-series with flag indicating whether consent was active (Yes=1, No=0) for each date. This is based on fmDate and toDate.
        - Time-series with flag indicating whether consent/WAP combination was active (Yes=1, No=0) for each date. This considers the fmDate and toDate as well as from_month and to_month
    Writes csv-files:
        - Time-series with flag indicating whether consent was active (Yes=1, No=0) for each date. This is based on fmDate and toDate.
        - Time-series with flag indicating whether consent/WAP combination was active (Yes=1, No=0) for each date. This considers the fmDate and toDate as well as from_month and to_month
    '''
    #####-UNCOMMENT BELOW
    #-Set the period for which to create the time-series
    syear = self.config.getint('TIMINGS', 'syear') - 1
    smonth = self.config.getint('TIMINGS', 'smonth')
    sday = self.config.getint('TIMINGS', 'sday')
    sdate = dt.date(syear, smonth, sday)
    eyear = self.config.getint('TIMINGS', 'eyear')
    emonth = self.config.getint('TIMINGS', 'emonth')
    eday = self.config.getint('TIMINGS', 'eday')
    edate = dt.date(eyear, emonth, eday)

    #-Get csv-files to write to
    self.crc_active_csv = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'crc_active_csv'))
    self.crc_wap_active_csv = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'crc_wap_active_csv'))

    #-Create Active/Inactive (1,0) time-series for consents    
    crc_unique = pd.unique(self.crc_df['crc']); crc_unique = crc_unique[~pd.isnull(crc_unique)]
    i = pd.date_range(sdate, edate, freq='D')
    crc_active = pd.DataFrame(columns=crc_unique, index=i)
    crc_active.rename_axis('Date', inplace=True)
    df_group = self.crc_df.groupby('crc')['fmDate', 'toDate'].first()
    for c in crc_unique:
        i = df_group.loc[c, ['fmDate', 'toDate']].values
        df_sel = pd.DataFrame(columns=[c], index=pd.date_range(i[0], i[1], freq='D'))
        df_sel.rename_axis('Date', inplace=True)
        df_sel[c] = 1
        crc_active[[c]] = df_sel
    crc_active.fillna(0, inplace=True)
    crc_active.reset_index(inplace=True)
    crc_active['Date'] = crc_active['Date'].dt.strftime('%d/%m/%Y')
    crc_active.set_index('Date', inplace=True)
    df_sel = None; df_group = None; del df_sel, df_group
    #-write to csv
    crc_active.to_csv(self.crc_active_csv)
     
    #-Create Active/Inactive (1,0) for each of the records in crc_df (combination of crc and wap) that is not a discharge consent
    #-for WAPs only look at the non-discharge records
    #crc_df = self.crc_df.loc[self.crc_df['Activity']!='Discharge water to water',['crc', 'wap', 'fmDate', 'toDate', 'from_month', 'to_month']]
    crc_df = self.crc_df.loc[self.crc_df['Activity']!='Discharge water to water',['crc', 'wap_name_long', 'fmDate', 'toDate', 'from_month', 'to_month']]
    i = pd.date_range(sdate, edate, freq='D')
    crc_wap_active = pd.DataFrame(index=i)
    crc_wap_active.rename_axis('Date', inplace=True)
    for i in crc_df.iterrows():
        df_sel = pd.DataFrame()
        #v = i[1]['crc'] + '_' + i[1]['wap']
        v = i[1]['crc'] + '_' + i[1]['wap_name_long']
        df_sel['Date'] = pd.date_range(i[1]['fmDate'], i[1]['toDate'], freq='D')
        df_sel['Month'] = df_sel['Date'].dt.strftime('%m')
        df_sel['Month'] = df_sel['Month'].astype(np.int) 
        df_sel['from_month'] = i[1]['from_month']
        df_sel['to_month'] = i[1]['to_month']
        df_sel['active'] = 0

        mlist = []
        if i[1]['from_month'] < i[1]['to_month']:
            for m in range(int(i[1]['from_month']), int(i[1]['to_month'])+1):
                mlist.append(m)
        elif i[1]['from_month'] > i[1]['to_month']:
            for m in range(int(i[1]['from_month']), 12+1):
                mlist.append(m)
            for m in range(1, int(i[1]['to_month']) + 1):
                mlist.append(m)
        else:
            mlist.append(int(i[1]['from_month']))
        df_sel.loc[df_sel['Month'].isin(mlist), 'active'] = 1

        df_sel.set_index('Date', inplace=True)
        crc_wap_active[v] = df_sel['active']
    crc_wap_active.fillna(0, inplace=True)    
    crc_wap_active.reset_index(inplace=True)
    crc_wap_active['Date'] = crc_wap_active['Date'].dt.strftime('%d/%m/%Y')
    crc_wap_active.set_index('Date', inplace=True)
    #-write to csv
    crc_wap_active.to_csv(self.crc_wap_active_csv)


def add_latlon_coordinates(crc_df):
    '''
    Add Lat Lon columns to the consents dataframe for:
        - wap_NZTMX
        - wap_NZTMY
        - wap_sd_NZTMX
        - wap_sd_NZTMY
        - discharge_NZTMX
        - discharge_NZTMY
    And returns the dataframe with these columns added
    '''
    
    #-add empty columns
    crc_df['wap_name'] = np.nan
    #-The name below is needed because of some stupid consent conditions there may be a consent that has e.g. twice the activity 'Take Surface Water' on the same WAP number.
    #-This causes conflicts, and there for a _1, _2, etc. is added to wap_name if WAP has more than one same activity.
    crc_df['wap_name_long'] = np.nan     
    crc_df['wap_lat'] = np.nan
    crc_df['wap_lon'] = np.nan
    crc_df['wap_sd_lat'] = np.nan
    crc_df['wap_sd_lon'] = np.nan
    crc_df['discharge_name'] = np.nan
    crc_df['discharge_lat'] = np.nan
    crc_df['discharge_lon'] = np.nan
    #-fill the columns with lat/lon coordinates
    for i in crc_df.iterrows():
        activity = i[1]['Activity']
        if activity != 'Discharge water to water':
            x = i[1]['wap_NZTMX']
            y = i[1]['wap_NZTMY']
            x, y = reproject(2193, 4326, x, y)
            crc_df.loc[i[0], 'wap_lat'] = y
            crc_df.loc[i[0], 'wap_lon'] = x
            wap = i[1]['wap']
            #print(activity, i[1]['crc'], wap)
            wap_name = wap.split('/')
            wap_name = wap_name[0] + '_' + wap_name[1]
            if activity == 'Take Groundwater':
                wap_name = wap_name + '_GW'
                x = i[1]['wap_sd_NZTMX']
                y = i[1]['wap_sd_NZTMY']
                x, y = reproject(2193, 4326, x, y) 
                crc_df.loc[i[0], 'wap_sd_lat'] = y
                crc_df.loc[i[0], 'wap_sd_lon'] = x
            elif activity == 'Take Surface Water':
                wap_name = wap_name + '_SW'
            else:
                wap_name = wap_name + '_Divert'
            crc_df.loc[i[0], 'wap_name'] = wap_name
            crc_df.loc[i[0], 'wap_name_long'] = wap_name
        else:
            crc = i[1]['crc']
            x = i[1]['discharge_NZTMX']      
            y = i[1]['discharge_NZTMY']
            x, y = reproject(2193, 4326, x, y) 
            crc_df.loc[i[0], 'discharge_lat'] = y
            crc_df.loc[i[0], 'discharge_lon'] = x
            crc_df.loc[i[0], 'discharge_name'] = crc + '_discharge'

    return crc_df
            
            
def add_WAPs_as_nodes(WEAP, crc_df, activity):
    '''
    Add WAPs as demand nodes to the model based on their lat & lon coordinates. It only add WAPs for the activity (e.g. 'Take Surface Water').
    If activity == 'Take Groundwater', then also the corresponding Stream Depletion (SD) points will be added as demand node. Diverts are not added
    as a demand node, but should be added later manually or automated (--> still needs some thinking). 
    '''
    
    WEAP.Verbose=0
    WEAP.View = 'Schematic'
    
    crc_df = crc_df.loc[crc_df['Activity']==activity, ['wap_name', 'wap_lat', 'wap_lon', 'wap_sd_lat', 'wap_sd_lon']]
    crc_df = crc_df.groupby('wap_name').first().reset_index()
    #waps = pd.unique(crc_df['wap_name'])
    
    selBranches = WEAP.Branch('\Demand Sites and Catchments').Children
    #-clean all existing waps
    print('Removing existing WAPs for %s...' %activity)
    for i in selBranches:
        if i.TypeName == 'Demand Site':
            if (activity == 'Take Surface Water') & ('_SW' in i.Name):
                print('Deleted %s' %i.Name)
                i.Delete()
            elif (activity == 'Take Groundwater') & ('_GW' in i.Name):
                print('Deleted %s' %i.Name)
                i.Delete()
             
    #-add WAPs (for groundwater take or surface water take)
    print('Adding WAPs for %s as demand nodes...' %activity)
    c= 0
    for i in crc_df.iterrows():
        ID = i[1]['wap_name']
        print(ID)
        lon = i[1]['wap_lon']
        lat = i[1]['wap_lat']
        q = None
        z=0
        c+=1
        while q == None:
            z+=1
            q = WEAP.CreateNode('Demand Site', lon, lat, ID)
            lon+= 0.001
            lat+= 0.001
            #-break the loop if catchment cannot be added for some reason
            if z==50:
                break
#         if c==5:
#             break
    #-if activity == 'Take Groundwater', then also SD points should be added
    if activity == 'Take Groundwater':
        c=0
        for i in crc_df.iterrows():
            ID = i[1]['wap_name'] + '_SD'
            print(ID)
            lon = i[1]['wap_sd_lon']
            lat = i[1]['wap_sd_lat']
            q = None
            z=0
            c+=1
            while q == None:
                z+=1
                q = WEAP.CreateNode('Demand Site', lon, lat, ID)
                lon+= 0.001
                lat+= 0.001
                #-break the loop if catchment cannot be added for some reason
                if z==50:
                    break
#             if c==5:
#                 break
            
    selBranches = WEAP.Branch('\Demand Sites and Catchments').Children
    demandSites = []
    for i in selBranches:
        if i.TypeName == 'Demand Site':
            if (activity == 'Take Surface Water') & ('_SW' in i.Name):
                demandSites.append(i.Name)
                print('%s was added ' %i.Name)
            elif (activity == 'Take Groundwater') & ('_GW' in i.Name):
                demandSites.append(i.Name)
                print('%s was added ' %i.Name)
    ids = list(crc_df['wap_name'])
    if activity == 'Take Groundwater':
        ids1 = list(crc_df['wap_name'] + '_SD')
        ids2 = ids + ids1
        ids = ids2
    for i in ids:
        ID = i
        if ID not in demandSites:
            print('ERROR: %s was not added as Demand node. Needs to be fixed manually.' %ID)
            
    WEAP.SaveArea()            
    WEAP.Verbose=1


def removeWAPs(WEAP, activity):
    '''
    Remove WAP demand nodes from the model for the specified activity.
    '''
    WEAP.View = 'Schematic'
    
    selBranches = WEAP.Branch('\Demand Sites and Catchments').Children
    #-clean all existing waps for activity
    print('Removing existing WAPs for %s...' %activity)
    for i in selBranches:
        if i.TypeName == 'Demand Site':
            if (activity == 'Take Surface Water') & ('_SW' in i.Name):
                print('Deleted %s' %i.Name)
                i.Delete()
            elif (activity == 'Take Groundwater') & ('_GW' in i.Name):
                print('Deleted %s' %i.Name)
                i.Delete()
    print('WAPs for %s have been removed.' %activity)
    WEAP.SaveArea()
    

def set_consumption(self):
    '''
    Set consumption to demand nodes. Consumption is set in the csv-file under the "consumption" variable in the config file. If different use types act on the demand node, then consumption %
    is averaged according to max_rate ratios.
    '''    
    flag = True
    try:
        branches = self.WEAP.Branch('\\Demand Sites and Catchments').Children
        for br in branches:
            if br.TypeName == 'Demand Site':
                print('Setting consumption for %s' %br.Name)
                if '_SD' in br.Name: #-for surface water depletion nodes the consumption is 0%; i.e. 100% is returned to groundwater
                    br.Variables('Consumption').Expression = 0
                else:
                    sel_df = self.crc_df.loc[self.crc_df['wap_name']==br.Name, ['wap_name', 'wap_max_rate [l/s]', 'consumption']]
                    #-calculate weighted average of consumption rate using wap_max_rate
                    sum_rate = sel_df['wap_max_rate [l/s]'].sum()
                    sel_df['consumption_scaled'] = sel_df['consumption'] * sel_df['wap_max_rate [l/s]'] / sum_rate
                    sum_scaled = round(sel_df['consumption_scaled'].sum(),2)
                    br.Variables('Consumption').Expression = sum_scaled 
    except:
        flag = False
        
    return flag
    
def init_WAP_KeyAssumptions(self):
    '''
    Set-up the blanco framework for the WAPs under the Key Assumptions. If there's already a WAPs Branch under the Key Assumptions, then this will be deleted first before adding the new branch.
    Also set the scale unit to m3.    
    '''
    #-List with children that need to be added to the 'WAPs/Groundwater' and 'WAPs/Surface Water' 'Key Assumptions'
    ####-LINE BELOW IS COMMENTED BECAUSE IT WAS FOUND LATER THAT THERE IS NO KEY ASSUMPTION NEEDED FOR STREAM DEPLETION; STREAM DEPLETION IS ADDED TO THE STREAM DEPLETION NODE DIRECTLY INSTEAD OF USING KEY ASSUMPTION 
    #self.keyAssumpBranchWAPsGWChildList = ['Demand', 'Stream depletion','Max daily rate', 'Restriction daily volume', 'Supplied daily volume', 'Non_compliance daily volume']
    self.keyAssumpBranchWAPsGWChildList = ['Demand', 'Max daily rate', 'Restriction daily volume', 'Supplied daily volume', 'Non_compliance daily volume']
    self.keyAssumpBranchWAPsSWChildList = ['Demand', 'Max daily rate', 'Restriction daily volume', 'Supplied daily volume', 'Non_compliance daily volume']
    self.keyAssumpBranchWAPsDivertChildList = ['Max daily rate', 'Restriction daily volume', 'Supplied daily volume', 'Non_compliance daily volume'] #-for diverts there is no supplied volume calculated, so therefore 'Streamflow' should be used in the calculations
    #-If WAPs Key Assumption already exists, then remove it and create it again.
    if self.WEAP.BranchExists('\Key Assumptions\WAPs'):
        print(self.WEAP.Branch('\Key Assumptions\WAPs').FullName + ' has been deleted')
        self.WEAP.Branch('\Key Assumptions\WAPs').Delete()
    self.keyAssumpBranchWAPs = self.keyAssumpBranch.AddChild('WAPs')
    self.keyAssumpBranchWAPs.Variables("Annual Activity Level").ScaleUnit = 'm^3'
    print(self.keyAssumpBranchWAPs.FullName + ' branch has been added')
    #-Add Groundwater branch
    self.keyAssumpBranchWAPsGW = self.keyAssumpBranchWAPs.AddChild('Take Groundwater')
    self.keyAssumpBranchWAPsGW.Variables("Annual Activity Level").ScaleUnit = 'm^3'
    print(self.keyAssumpBranchWAPsGW.FullName + ' branch has been added')
    #-Add Surface Water branch 
    self.keyAssumpBranchWAPsSW = self.keyAssumpBranchWAPs.AddChild('Take Surface Water')
    self.keyAssumpBranchWAPsSW.Variables("Annual Activity Level").ScaleUnit = 'm^3'
    print(self.keyAssumpBranchWAPsSW.FullName + ' branch has been added')
    #-Add Divert Surface Water branch 
    self.keyAssumpBranchWAPsDivert = self.keyAssumpBranchWAPs.AddChild('Divert Surface Water')
    self.keyAssumpBranchWAPsDivert.Variables("Annual Activity Level").ScaleUnit = 'm^3'
    print(self.keyAssumpBranchWAPsDivert.FullName + ' branch has been added')
    
    
    #-Adding WAPs under the Groundwater Water branch
    df = self.crc_df.loc[self.crc_df['Activity']=='Take Groundwater']
    df = df.groupby('wap').first()
    df.reset_index(inplace=True)
    for k in self.keyAssumpBranchWAPsGWChildList:
        b = self.keyAssumpBranchWAPsGW.AddChild(k)
        b.Variables("Annual Activity Level").ScaleUnit = 'm^3'
        print('Adding WAPs under ' + b.FullName)
        if k != 'Stream depletion':
            count=0
            for d in df.iterrows():
                bb = b.AddChild(d[1]['wap_name'])
                bb.Variables("Annual Activity Level").ScaleUnit = 'm^3'
#                 count+=1
#                 if count==5:
#                     break
        else:
            count=0
            for d in df.iterrows():
                bb = b.AddChild(d[1]['wap_name'] + '_SD')
                bb.Variables("Annual Activity Level").ScaleUnit = 'm^3'
#                 count+=1
#                 if count==5:
#                     break
    #-Adding WAPs under the Surface Water branch
    df = self.crc_df.loc[self.crc_df['Activity']=='Take Surface Water']
    df = df.groupby('wap').first()
    df.reset_index(inplace=True)
    for k in self.keyAssumpBranchWAPsSWChildList:
        b = self.keyAssumpBranchWAPsSW.AddChild(k)
        b.Variables("Annual Activity Level").ScaleUnit = 'm^3'
        print('Adding WAPs under ' + b.FullName)
        count=0
        for d in df.iterrows():
            bb = b.AddChild(d[1]['wap_name'])
            bb.Variables("Annual Activity Level").ScaleUnit = 'm^3'
#             count+=1
#             if count==5:
#                 break
    
    #-Adding WAPs under the Divert Surface Water Branch (important to note that these are not demand nodes in the model as is the case with sw take and gw take, but will be added as diversions).
    df = self.crc_df.loc[self.crc_df['Activity']=='Divert Surface Water']
    df = df.groupby('wap').first()
    df.reset_index(inplace=True)
    for k in self.keyAssumpBranchWAPsDivertChildList:
        b = self.keyAssumpBranchWAPsDivert.AddChild(k)
        b.Variables("Annual Activity Level").ScaleUnit = 'm^3'
        print('Adding WAPs under ' + b.FullName)
        count=0
        for d in df.iterrows():
            bb = b.AddChild(d[1]['wap_name'])
            bb.Variables("Annual Activity Level").ScaleUnit = 'm^3'
#             count+=1
#             if count==5:
#                 break
            
    self.WEAP.SaveArea()
                 
    

 
def init_Consents_OtherAssumptions(self):
    '''
    Set-up the framework for the consents under the Other Assumptions. If there's already a Consents Branch under the Other Assumptions, then this will be deleted first before adding the new branch.
    Also set the scale unit to m3 or day, depending on the type.    
    '''

    #-check if WAPs branch exists under Key Assumptions. This is required before Other Assumptions\Consents can be added
    b = self.WEAP.BranchExists('\\Key Assumptions\\WAPs')
    if b:
        #-Columns of crc active and crc_wap_active dataframes
        df = pd.read_csv(self.crc_active_csv, index_col=0)
        crc_cols = list(df.columns)
        df = pd.read_csv(self.crc_wap_active_csv, index_col=0)
        crc_wap_cols = list(df.columns)
        df = None; del df 
        
        #-All consents except discharge consents
        df = self.crc_df.loc[self.crc_df['Activity']!='Discharge water to water']
        crcs = pd.unique(df['crc'])
            
        #-If Consents Key Assumption already exists, then remove it and create it again.
        if self.WEAP.BranchExists('\Other Assumptions\Consents'):
            print(self.WEAP.Branch('\Other Assumptions\Consents').FullName + ' has been deleted')
            self.WEAP.Branch('\Other Assumptions\Consents').Delete()
        self.otherAssumpBranchConsents = self.otherAssumpBranch.AddChild('Consents')
        print(self.otherAssumpBranchConsents.FullName + ' branch has been added')
        
        #-Adding consents under the 'Other Assumptions\Consents' Branch
        count=0
        for c in crcs:
            print('Adding conditions for %s' %c)
            #-Add the consent nr to the 'Other Assumptions\Consents' Branch 
            b = self.otherAssumpBranchConsents.AddChild(c)
#             b = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c)
            #-Add Active branch to consent 
            c_Active = b.AddChild('Active')
            c_Active.Variables('Annual Activity Level').ScaleUnit = 'No Unit'
            c_Active.Variables('Annual Activity Level').Expression = 'ReadFromFile(' + self.crc_active_csv + ', ' + str(crc_cols.index(c)+1) + ', , , , Interpolate)'
               
            #-Get the WAPs belonging to the conent
            df_short = df.loc[df['crc']==c]
            #-Empty list to fill with wap names if they have a wap_max_vol_pro_rata [m3] defined
            wap_max_vol_pro_rata_list = []
            #-List with wap_long_names used later
            wap_name_long_list = []
            for w in df_short.iterrows():
                wap = w[1]['wap']
                wap_name = w[1]['wap_name']
                wap_name_long = w[1]['wap_name_long']
                wap_name_long_list.append(wap_name_long)
                activity = w[1]['Activity']
                crc_wap = c + '_' + wap_name_long
                wap_branch = b.AddChild(wap_name_long)
                #wap_branch = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long)
                #-Add Active branch to WAP
                w_Active = wap_branch.AddChild('Active')
                w_Active.Variables('Annual Activity Level').ScaleUnit = 'No Unit'
                w_Active.Variables('Annual Activity Level').Expression = 'ReadFromFile(' + self.crc_wap_active_csv + ', ' + str(crc_wap_cols.index(crc_wap)+1) + ', , , , Interpolate)'
                #-Add BAllocated variable to WAP
                bbb = wap_branch.AddChild('Ballocated')
                bbb.Variables('Annual Activity Level').ScaleUnit = 'No Unit'
                restriction = w[1]['lowflow_restriction']
                #-if restriction is 0, then there is no restriction, and ballocated is set to 1. Otherwise it's set to zero, and zeros need to be checked manually because consent/wap band information is not stored in database
                if restriction == 0:
                    bbb.Variables('Annual Activity Level').Expression = 1.0
                #-Add Maximum daily rate branch to WAP
                mxDayRate = wap_branch.AddChild('Max daily rate')
                mxDayRate.Variables('Annual Activity Level').ScaleUnit = 'm^3'
                mxDayRate.Variables('Annual Activity Level').Expression = str(w[1]['wap_max_rate [l/s]'] * 86.4) + ' * ' + c_Active.FullName + ' * ' + w_Active.FullName  #-depends on whether consents and/or wap where active yes/no
                #-Add fraction (is the fraction of this consent shares with other consents using the same wap)
                Frac = wap_branch.AddChild('Fraction')
                Frac.Variables('Annual Activity Level').ScaleUnit = 'No Unit'
#                 Frac = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Fraction')
                if activity == 'Take Groundwater':
                    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Groundwater\\Max daily rate\\' + wap_name) #.Variables('Annual Activity Level').Expression
                elif activity == 'Divert Surface Water':
                    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Divert Surface Water\\Max daily rate\\' + wap_name) #.Variables('Annual Activity Level').Expression
                else:
                    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Surface Water\\Max daily rate\\' + wap_name) #.Variables('Annual Activity Level').Expression
                Frac.Variables('Annual Activity Level').Expression = mxDayRate.FullName + ' / ' + br.FullName
                #-Add wap_max_rate_pro_rata if it exists in the consents table
                v = w[1]['wap_max_rate_pro_rata [l/s]'] * 86.4
                if ~np.isnan(v):
                    br = wap_branch.AddChild('Max daily rate pro rata')
                    br.Variables('Annual Activity Level').ScaleUnit = 'm^3'
                    br.Variables('Annual Activity Level').Expression = v
                #-Add wap_max_vol_pro_rata  and return period if it exists in the consents table
                v = w[1]['wap_max_vol_pro_rata [m3]']
                r = w[1]['wap_return_period [d]']
                if ~np.isnan(v) & ~np.isnan(r):
                    br = wap_branch.AddChild('Max volume pro rata')
                    br.Variables('Annual Activity Level').ScaleUnit = 'm^3'
                    br.Variables('Annual Activity Level').Expression = v
                    br = wap_branch.AddChild('Return period')
                    br.Variables('Annual Activity Level').ScaleUnit = 'Day'
                    br.Variables('Annual Activity Level').Expression = r
                    wap_max_vol_pro_rata_list.append(wap_name_long)
                 
                #-Add supplied daily volume as branch
                bb = wap_branch.AddChild('Supplied daily volume')
                bb.Variables('Annual Activity Level').ScaleUnit = 'm^3'
                if activity == 'Take Groundwater':
                    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Groundwater\\Supplied daily volume\\' + wap_name)
                elif activity == 'Divert Surface Water':
                    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Divert Surface Water\\Supplied daily volume\\' + wap_name)
                else:
                    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Surface Water\\Supplied daily volume\\' + wap_name)
                bb.Variables('Annual Activity Level').Expression = br.FullName + ' * ' + Frac.FullName
                 
                #-Add restriction daily volume as branch --> calculation algorithm has to entered later - node or divert is needed for this first !!!!!!!!!!!!!!
                #-This is later calculated using the function: calc_restrict_daily_vol
                bb = wap_branch.AddChild('Restriction daily volume')
                bb.Variables('Annual Activity Level').ScaleUnit = 'm^3'
                   
       
            #-Add sum of wap_max_vol_pro_rata
            if len(wap_max_vol_pro_rata_list)>0: #-check if there is a max volume pro rata for any of the WAPs and if so, then add the sum of those
                bb = b.AddChild('Sum max volume pro rata')
                bb.Variables('Annual Activity Level').ScaleUnit = 'm^3'
                #-empty expression string
                expr_str = ''
                for w in wap_max_vol_pro_rata_list:
                    wap_branch = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + w + '\\Max volume pro rata').FullName
                    if w != wap_max_vol_pro_rata_list[-1]:
                        expr_str = expr_str + wap_branch + ' + '
                    else:
                        expr_str = expr_str + wap_branch
                bb.Variables('Annual Activity Level').Expression = expr_str
                    
            #-Add crc_vol_return_period and crc_return_period if they are defined in the consents table. Just get one value (e.g max) from the list (doesn't matter which one because they should be the same for all records under the one consent)
            v = df_short[['crc_vol_return_period [m3]']].max().values[0]
            r = df_short[['crc_return_period [d]']].max().values[0]
            if ~np.isnan(v) & ~np.isnan(r):
                bb = b.AddChild('Volume return period')
                bb.Variables('Annual Activity Level').ScaleUnit = 'm^3'
                bb.Variables('Annual Activity Level').Expression = v
                bb = b.AddChild('Return period')
                bb.Variables('Annual Activity Level').ScaleUnit = 'Day'
                bb.Variables('Annual Activity Level').Expression = r
                
            #-Add crc_ann_vol if it is defined in the consents table. Just get one value (e.g max) from the list (doesn't matter which one because they should be the same for all records under the one consent)
            v = df_short[['crc_ann_vol [m3]']].max().values[0]
            if ~np.isnan(v):
                bb = b.AddChild('Annual volume')
                bb.Variables('Annual Activity Level').ScaleUnit = 'm^3'
                bb.Variables('Annual Activity Level').Expression = v
                
            #-Add crc_ann_vol_combined if it is defined in the consents table. Just get one value (e.g max) from the list (doesn't matter which one because they should be the same for all records under the one consent)
            v = df_short[['crc_ann_vol_combined [m3]']].max().values[0]
            if ~np.isnan(v):
                bb = b.AddChild('Annual volume combined')
                bb.Variables('Annual Activity Level').ScaleUnit = 'm^3'
                bb.Variables('Annual Activity Level').Expression = v
 
            #-Add Supplied Volume as branch to consent branch
            br = b.AddChild('Supplied daily volume')
            br.Variables('Annual Activity Level').ScaleUnit = 'm^3'
#             br = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\Supplied daily volume')
            expr_str = ''
            for w in wap_name_long_list:
                q = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + w + '\\Supplied daily volume').FullName
                if w != wap_name_long_list[-1]:
                    expr_str = expr_str + q + ' + '
                else:
                    expr_str = expr_str + q
            br.Variables('Annual Activity Level').Expression = expr_str
               
            #-Add Non_compliance daily volume as branch to consent branch
            bb = b.AddChild('Non_compliance daily volume')
            bb.Variables('Annual Activity Level').ScaleUnit = 'm^3'
            expr_str = 'Max(0, ' + br.FullName + ' - ('
            for w in wap_name_long_list:
                br = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + w + '\\Restriction daily volume').FullName
                if w != wap_name_long_list[-1]:
                    expr_str = expr_str + br + ' + '
                else:
                    expr_str = expr_str + br + '))'
            bb.Variables('Annual Activity Level').Expression = expr_str
         
        #-Go back to the Key Assumptions-########################
        activity_list = ['Take Groundwater', 'Take Surface Water', 'Divert Surface Water']
        for a in activity_list:
            #-After all consents have been added, the Maximum daily rate can be calculated for each WAP by summing the Max daily rates of the consents the WAP belongs to (e.g. if 3 consents have the same wap, then sum these)
            br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\' + a + '\\Max daily rate')
            for w in br.Children:
                wap_name = w.Name
                print('Calculating max daily rate for ' + wap_name)
                df_sel = self.crc_df.loc[(self.crc_df['wap_name']==wap_name) & (self.crc_df['Activity']==a), ['crc', 'wap_name_long']]
                expr_str = ''
                count = 0
                for i in df_sel.iterrows():
                    count+=1
                    crc = i[1]['crc']
                    wap_name_long = i[1]['wap_name_long']
                    bb = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + crc + '\\' + wap_name_long + '\\' + 'Max daily rate').FullName
                    if count!=len(df_sel):
                        expr_str = expr_str + bb + ' + '
                    else:
                        expr_str = expr_str + bb
                self.WEAP.Branch(w.FullName).Variables('Annual Activity Level').Expression = expr_str
              
            #-After all consents have been added, the Restriction daily volume can be calculated for each WAP by summing the Restriction daily volume of the consents the WAP belongs to (e.g. if 3 consents have the same wap, then sum these)
            br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\' + a + '\\Restriction daily volume')
            for w in br.Children:
                wap_name = w.Name
                print('Calculating restriction daily volume for ' + wap_name)
                df_sel = self.crc_df.loc[(self.crc_df['wap_name']==wap_name) & (self.crc_df['Activity']==a), ['crc', 'wap_name_long']]
                expr_str = ''
                count = 0
                for i in df_sel.iterrows():
                    count+=1
                    crc = i[1]['crc']
                    wap_name_long = i[1]['wap_name_long']
                    bb = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + crc + '\\' + wap_name_long + '\\' + 'Restriction daily volume').FullName
                    if count!=len(df_sel):
                        expr_str = expr_str + bb + ' + '
                    else:
                        expr_str = expr_str + bb
                self.WEAP.Branch(w.FullName).Variables('Annual Activity Level').Expression = expr_str
                
            #-The non-compliance daily volume can be calculated by extracting the restriction daily volume from the supplied volume (is mostly negative for compliance, so use Max(0,....))
            br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\' + a + '\\Non_compliance daily volume')
            for w in br.Children:
                wap_name = w.Name
                print('Calculating non-compliance daily volume for ' + wap_name)
                br1 = self.WEAP.Branch('\\Key Assumptions\\WAPs\\' + a + '\\Restriction daily volume\\' + wap_name).FullName
                br2 = self.WEAP.Branch('\\Key Assumptions\\WAPs\\' + a + '\\Supplied daily volume\\' + wap_name).FullName
                self.WEAP.Branch(w.FullName).Variables('Annual Activity Level').Expression = 'Max(0, ' + br2 + '-' + br1 + ')'
                
        #-Link key assumptions/waps/ demand time-series to demand nodes                
        selBranches = self.WEAP.Branch('\Demand Sites and Catchments').Children
        for i in selBranches:
            if (i.TypeName == 'Demand Site') & ('_SD' not in i.Name):
                print('Linking demand for node %s' %i.Name)
                i.Variables('Method').Expression = 'Specify daily demand'
                if '_SW' in i.Name:
                    i.Variables('Daily Demand').Expression = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Surface Water\\Demand\\' + i.Name).FullName
                else:
                    i.Variables('Daily Demand').Expression = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Groundwater\\Demand\\' + i.Name).FullName
            if (i.TypeName == 'Demand Site') & ('_SD' in i.Name):
                i.Variables('Method').Expression = 'Specify daily demand'
                i.Variables('Daily Demand').ScaleUnit = 'm^3'
                    

    else:
        print('The branch "Key Assumptions\WAPs" and its children does not exist.')
        print('This needs to be added before the "Other Assumptions\Consents" branch') 
        print('and its children can be added. Set "create_blanco_WAPs_KA=1" in the')
        print('config file.')
          
    #         count+=1
    #         if count==5:
    #             break
    #     print count
    self.WEAP.SaveArea()
    
######-FUNCTIONS BELOW CAN ONLY BE CALLED AFTER THE TRANSMISSION LINKS, RETURN LINKS, AND DIVERTS HAVE BEEN MANUALLY ADDED TO THE SYSTEM
    
def add_supply_delivered(self):
    '''
    After the diverts, transmission links, and return flow links are added manually, the "supply delivered" to each node can be added as an expression to the waps under key assumptions.
    Supply delivered is the amount of water delivered to the demand node in the previous time-step. For diverts it is not "supply delivered", but "streamflow" instead. E.g.:
    PrevTSValue(Supply and Resources\River\L36_2365_Divert\Reaches\Below L36_2365_Divert Diverted Inflow:Streamflow[m^3])
    '''
    activity_list = ['Take Groundwater', 'Take Surface Water', 'Divert Surface Water']
    for a in activity_list:
        waps = self.WEAP.Branch('\\Key Assumptions\\WAPs\\' + a + '\\Supplied daily volume').Children
        for w in waps:
            print('Adding supply delivered for %s' %(w.Name))
            br = self.WEAP.Branch(w.FullName)
            if a == 'Divert Surface Water':
                #br.Variables('Annual Activity Level').Expression = 'PrevTSValue(Supply and Resources\\River\\' + w.Name + ':Streamflow[m^3])'
                #-run script with line below instead of line above (03-03-2020)
                br.Variables('Annual Activity Level').Expression = 'PrevTSValue(Supply and Resources\\River\\' + w.Name + '\\Reaches\\Below '+ w.Name + ' Diverted Inflow:Streamflow[m^3])'
            else:
                br.Variables('Annual Activity Level').Expression = 'PrevTSValue(Demand Sites and Catchments\\' + w.Name + ':Supply Delivered[m^3])'
    
    self.WEAP.SaveArea()
                
def calc_restrict_daily_vol(self):
    '''
    Adds the algorithm to calculate the restriction daily volume for each unique combination of consent/wap under the 'Other Assumptions\Consents\crc-x\wap-y\Restriction daily volume' branch
    '''
    
    self.WEAP.Verbose=0
    
    #-All consents except discharge consents
    df = self.crc_df.loc[self.crc_df['Activity']!='Discharge water to water']
    crcs = pd.unique(df['crc'])
    
    for c in crcs:
        #-Get the WAPs belonging to the conent
        df_short = df.loc[df['crc']==c]
        
        #-consented volume return period only applies to takes (this is assumed). Therefore Diverts are left out of the equation for calculating restriction flow for return period volume, annual volume, and combined annual volume.
        crc_vol_return_period = self.WEAP.BranchExists('\\Other Assumptions\\Consents\\' + c + '\\Volume return period')
        crc_return_period = self.WEAP.BranchExists('\\Other Assumptions\\Consents\\' + c + '\\Return period')
        
        #-consented annual volume only applies to takes (this is assumed)
        crc_ann_vol = self.WEAP.BranchExists('\\Other Assumptions\\Consents\\' + c + '\\Annual volume')
        
        #-combined annual volume only applies to takes (this is assumed)
        crc_ann_vol_combined = self.WEAP.BranchExists('\\Other Assumptions\\Consents\\' + c + '\\Annual volume combined')

        for w in df_short.iterrows():
            wap = w[1]['wap']
            wap_name_long = w[1]['wap_name_long']

            print('Restriction string for %s and %s' %(c, wap))
            
            wap_max_vol_pro_rata = self.WEAP.BranchExists('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Max volume pro rata')
            wap_return_period = self.WEAP.BranchExists('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Return period')
            
            #-start expression string for restriction daily volume
            wap_branch = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Max daily rate').FullName
            restr_str = wap_branch
            
            if crc_return_period & crc_vol_return_period:
#                 restr_str = 'Min(' + restr_str
                crc_return_period_value = np.int(self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\Return period').Variables('Annual Activity Level').Expression)
                if crc_return_period_value>1:
                    restr_str = 'Min(' + restr_str
                    #-consented volume return period only applies to takes. Therefore Diverts are left out of the equation for calculating restriction flow.
                    df_short_copy = df_short[['Activity','wap_name', 'wap_name_long']].copy()
                    df_short_copy = df_short_copy.loc[df_short_copy['Activity']!='Divert Surface Water']
                    restr_str = restr_str + ', ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\Volume return period').FullName + ' - ('
                    count = 0
                    for ww in df_short_copy.iterrows():
                        count+=1
                        wap_name_temp = ww[1]['wap_name']
                        wap_name_long_temp = ww[1]['wap_name_long']
                        br = '(PrevTSValue(Demand Sites and Catchments\\' + wap_name_temp + ':Supply Delivered[m^3], 1, ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\Return period').FullName + ' - 1, Sum) * ' + \
                            self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long_temp + '\\Fraction').FullName + ')'
                        if count!=len(df_short_copy):
                            restr_str = restr_str + br + ' + '
                        else:
                            restr_str = restr_str + br + ')'
#                 else:  #-the "Max daily rate pro rate should always be part of the condition, and is therefore moved all the way to the bottom of this function (changed on 22 April 2020).
#                     restr_str = restr_str + ', ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Max daily rate pro rata').FullName
                    restr_str = restr_str + ')'
            
            #-wap_return_period and wap_max_vol_pro_rata: applies to takes and diverts
            if wap_max_vol_pro_rata & wap_return_period:
                restr_str = 'Min(' + restr_str
                df_short_copy = df_short[['Activity','wap_name', 'wap_name_long']].copy()
                wap_return_period_value = np.int(self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Return period').Variables('Annual Activity Level').Expression)
                if wap_return_period_value>1:
                    df_short_copy = df_short[['Activity','wap_name', 'wap_name_long']].copy()
                    restr_str = restr_str + ', ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\Sum max volume pro rata').FullName + ' - ('
                    count = 0
                    for ww in df_short_copy.iterrows():
                        count+=1
                        wap_name_temp = ww[1]['wap_name']
                        wap_name_long_temp = ww[1]['wap_name_long']
                        
                        #-sometimes only one of the waps (e.g. the take only and the divert not) has a return period and volume specified, so we need to make sure that 
                        #-it's not trying to obtain a return period if it's not there.
                        if self.WEAP.BranchExists('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long_temp + '\\Return period') & \
                            self.WEAP.BranchExists('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long_temp + '\\Max volume pro rata'):
                            if ('_SW' in wap_name_temp) or ('_GW' in wap_name_temp):
                                br = '(PrevTSValue(Demand Sites and Catchments\\' + wap_name_temp + ':Supply Delivered[m^3], 1, ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long_temp + '\\Return period').FullName + ' - 1, Sum) * ' + \
                                    self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long_temp + '\\Fraction').FullName + ')'
                            else: 
                                br = '(PrevTSValue(Supply and Resources\\River\\' + wap_name_temp + ':Streamflow[m^3], 1, ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long_temp + '\\Return period').FullName + ' - 1, Sum) * ' + \
                                    self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long_temp + '\\Fraction').FullName + ')'
                        if count!=len(df_short_copy):
                            restr_str = restr_str + br + ' + '
                        else:
                            restr_str = restr_str + br + ')'
                else:
                    restr_str = restr_str + ', ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Max volume pro rata').FullName
                restr_str = restr_str + ')' #-today added
                    
            #-check if consent has annual volume
            if crc_ann_vol:
                crc_ann_vol_value = int(float(self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\Annual volume').Variables('Annual Activity Level').Expression))
                if crc_ann_vol_value>0:
                    restr_str = 'Min(' + restr_str
                    #-consented annual volume only applies to takes. Therefore Diverts are left out of the equation for calculating restriction flow.
                    df_short_copy = df_short[['Activity','wap_name', 'wap_name_long']].copy()
                    df_short_copy = df_short_copy.loc[df_short_copy['Activity']!='Divert Surface Water']
                    restr_str = restr_str + ', ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\Annual volume').FullName + ' - ('
                    count = 0
                    for ww in df_short_copy.iterrows():
                        count+=1
                        wap_name_temp = ww[1]['wap_name']
                        wap_name_long_temp = ww[1]['wap_name_long']
                        br = '(PrevTSValue(Demand Sites and Catchments\\' + wap_name_temp + ':Supply Delivered[m^3], 1, 366-1, Sum) * ' + \
                            self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long_temp + '\\Fraction').FullName + ')'
                        if count!=len(df_short_copy):
                            restr_str = restr_str + br + ' + '
                        else:
                            restr_str = restr_str + br + ')'
                    restr_str = restr_str + ')'
            
            #-check if annual volume combined
            if crc_ann_vol_combined:
                crc_ann_vol_combined_value = np.int(self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\Annual volume combined').Variables('Annual Activity Level').Expression)
                if crc_ann_vol_combined_value>0:
                    try:
                    
                        #-get list of associated consent numbers that share the same combined annual volume
                        associated_crcs = list(pd.unique(df_short['associated_crcs']))[0]
                        associated_crcs = associated_crcs.split(',')
                        temp_list = []
                        for c_ass in associated_crcs:
                            temp_list.append(c_ass.strip()) #-remove white space
                        associated_crcs = temp_list; temp_list = None; del temp_list
                        
                        restr_str = 'Min(' + restr_str
                        restr_str = restr_str + ', ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\Annual volume combined').FullName + ' - ('
                        
                        #-loop over the associated consents
                        for c_ass in associated_crcs:
                            try:
                                self.WEAP.Verbose=0
                                crc_ann_vol_combined_value_check = np.int(self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c_ass + '\\Annual volume combined').Variables('Annual Activity Level').Expression)
                                self.WEAP.Verbose=1
                                #-check if the combined annual volume of the associated consent is the same as under the main consent. Otherwise the associated consent may not be associated based on
                                #-annual shared volume, but other weird consent conditions
                                if crc_ann_vol_combined_value_check == crc_ann_vol_combined_value:
                                    df_sel = df.loc[df['crc']==c_ass, ['Activity','wap_name', 'wap_name_long']]
                                    df_sel = df_sel.loc[df_sel['Activity']!='Divert Surface Water']
                                    #-loop over the waps of the associated consents
                                    for ww in df_sel.iterrows():
                                        wap_name_temp = ww[1]['wap_name']
                                        wap_name_long_temp = ww[1]['wap_name_long']
                                        br = '(PrevTSValue(Demand Sites and Catchments\\' + wap_name_temp + ':Supply Delivered[m^3], 1, 366-1, Sum) * ' + \
                                            self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c_ass + '\\' + wap_name_long_temp + '\\Fraction').FullName + ') + '
                                        restr_str = restr_str + br
                            except:
                                print('Associated consent %s has no combined annual volume and is therefore not added to the equation' %c_ass)
                                    
                        #-loop over the waps of the main consent
                        #-combined annual volume only applies to takes. Therefore Diverts are left out of the equation for calculating restriction flow.
                        df_short_copy = df_short[['Activity','wap_name', 'wap_name_long']].copy()
                        df_short_copy = df_short_copy.loc[df_short_copy['Activity']!='Divert Surface Water']
                        count = 0
                        for ww in df_short_copy.iterrows():
                            count+=1
                            wap_name_temp = ww[1]['wap_name']
                            wap_name_long_temp = ww[1]['wap_name_long']
                            br = '(PrevTSValue(Demand Sites and Catchments\\' + wap_name_temp + ':Supply Delivered[m^3], 1, 366-1, Sum) * ' + \
                                self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long_temp + '\\Fraction').FullName + ')'
                            if count!=len(df_short_copy):
                                restr_str = restr_str + br + ' + '
                            else:
                                restr_str = restr_str + br + ')'
                        restr_str = restr_str + ')'
                    except:
                        print('No associated consents found for %s' %c)
            
            #-The final restriction string for the wap
            #restr_str = 'Max(0, ' + restr_str + ' * ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Ballocated').FullName + ')'
            ##-Update restriction string carried out on 2 November 2020 --> added "Max daily rate pro rata" to the restriction string
            try:
                pro_rata_str = self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Max daily rate pro rata').FullName
                restr_str = 'Max(0, Min(' + pro_rata_str + ',' + restr_str + ') * ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Ballocated').FullName + ')'
            except:
                restr_str = 'Max(0, ' + restr_str + ' * ' + self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Ballocated').FullName + ')'
            print(restr_str)
            self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Restriction daily volume').Variables('Annual Activity Level').Expression = restr_str

    self.WEAP.Verbose=1
    self.WEAP.SaveArea()
                    
            
def add_bands_to_crc(self):
    '''
    Adds Ballocated using "BandNo" from the crc_df dataframe
    '''
    LF_site = self.config.get('CONSENTS_PART_2', 'LF_site_name')
    
    #-All consents except discharge consents
    df = self.crc_df.loc[self.crc_df['Activity']!='Discharge water to water']
    crcs = pd.unique(df['crc'])
    
    self.WEAP.Verbose=0
     
    for c in crcs:
        #-Get the WAPs belonging to the conent
        df_short = df.loc[df['crc']==c]
        for w in df_short.iterrows():
            wap_name_long = w[1]['wap_name_long']
            try:
                #band = np.int(w[1]['BandNo'])
                band = w[1]['BandNo']
                band = 'band_num_%s' %band
                print('Adding Ballocated for %s %s %s' %(c, wap_name_long, band))
                band_branch = self.WEAP.Branch('\\Key Assumptions\\Low Flows\\' + LF_site + '\\' + band + '\\Ballocated').FullName
                self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Ballocated').Variables('Annual Activity Level').Expression = band_branch
            except:
                print('Ballocated for %s %s %s is set to 1' %(c, wap_name_long, band))
                self.WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + wap_name_long + '\\Ballocated').Variables('Annual Activity Level').Expression = 1
    
    self.WEAP.Verbose=1
    self.WEAP.SaveArea()
    
def set_demand_to_restrict_daily_vol(self):
    '''
    Set maximum consent conditions (restriction daily volume) as the demand on the WAPs. This can be seen as the worst-case scenario.
    '''
    
    #-first groundwater waps
    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Groundwater\\Demand').Children
    for b in br:
        wap = b.Name
        print('Setting demand for %s to restriction daily volume' %wap)
        b.Variables('Annual Activity Level').Expression = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Groundwater\\Restriction daily volume\\' + wap).FullName
        
    #-then surface waps
    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Surface Water\\Demand').Children
    for b in br:
        wap = b.Name
        print('Setting demand for %s to restriction daily volume' %wap)
        b.Variables('Annual Activity Level').Expression = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Surface Water\\Restriction daily volume\\' + wap).FullName
    
    self.WEAP.SaveArea()
        
def set_demand_to_csv_ts(self):
    '''
    Set time-series from a csv-file as the demand for the WAPs. E.g. this could be metered abstraction data.
    '''
    
    csvF = os.path.join(self.crc_dir, self.config.get('CONSENTS_PART_2', 'demand'))
    
    df = pd.read_csv(csvF, index_col=[0], dayfirst=True, parse_dates=True)
    cols = list(df.columns)

    
    #-first groundwater waps
    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Groundwater\\Demand').Children
    for b in br:
        wap = b.Name
        wap_split = wap.split('_GW')[0]
        wap_split = wap_split.replace('_','/')
        b.Variables('Annual Activity Level').Expression = 'ReadFromFile(' + csvF + ', ' + str(cols.index(wap_split)+1) + ', , , , Interpolate)'
        
    #-then surface waps
    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Surface Water\\Demand').Children
    for b in br:
        wap = b.Name
        wap_split = wap.split('_SW')[0]
        wap_split = wap_split.replace('_','/')
        b.Variables('Annual Activity Level').Expression = 'ReadFromFile(' + csvF + ', ' + str(cols.index(wap_split)+1) + ', , , , Interpolate)'
        
    self.WEAP.SaveArea()
    
    
def set_demand_to_zero(self):
    '''
    Set demand for all nodes to zero
    '''
    #-first groundwater waps
    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Groundwater\\Demand').Children
    for b in br:
        wap = b.Name
        print('Setting demand for %s to zero' %wap)
        b.Variables('Annual Activity Level').Expression = 0
        
    #-then surface waps
    br = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Surface Water\\Demand').Children
    for b in br:
        wap = b.Name
        print('Setting demand for %s to zero' %wap)
        b.Variables('Annual Activity Level').Expression = 0
    
    self.WEAP.SaveArea()
    
    
def set_restriction_on_transmission_links(self):
    '''
    Set restrictions on transmission links. Restrictions set are the daily restriction volumes.
    '''

    br = self.WEAP.Branch('\\Supply and Resources\\Transmission Links').Children
    for b in br:
        wap = b.Name.split('to ')[1]
        if '_SW' in wap:
            print('Setting restriction on transmission link to %s' %wap)
            for bb in b.Children:
                #-note extra space in variable expression below: this is likely a bug in WEAP
                bb.Variables('Maximum Flow   Volume').Expression = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Surface Water\\Restriction daily volume\\' + wap).FullName + ' /(24*3600)' 
                #bb.Variables('Maximum Flow   Volume').ScaleUnit = 'm^3'
        elif ('_GW' in wap) & ('_SD' not in wap):
            print('Setting restriction on transmission link to %s' %wap)
            for bb in b.Children:
                #-note extra space in variable expression below: this is likely a bug in WEAP
                bb.Variables('Maximum Flow   Volume').Expression = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Take Groundwater\\Restriction daily volume\\' + wap).FullName  + ' /(24*3600)'
                #bb.Variables('Maximum Flow   Volume').ScaleUnit = 'm^3'
    self.WEAP.SaveArea()

def set_restriction_on_diverts(self):
    '''
    Set restrictions on diverts. Restrictions set are the daily restriction volumes.
    '''
    
    br = self.WEAP.Branch('\\Supply and Resources\\River').Children
    for b in br:
        if '_Divert' in b.Name:
            print('Setting restriction on divert %s' %b.Name)
            b.Variables('Maximum Diversion').Expression = self.WEAP.Branch('\\Key Assumptions\\WAPs\\Divert Surface Water\\Restriction daily volume\\' + b.Name).FullName  + ' /(24*3600)'
    self.WEAP.SaveArea()
    
def remove_restriction_from_transmission_links(self):    
    '''
    Remove restrictions from transmission links.
    '''
    br = self.WEAP.Branch('\\Supply and Resources\\Transmission Links').Children
    for b in br:
        wap = b.Name.split('to ')[1]
        print('Removing restriction from transmission link %s' %wap)
        if '_SW' in wap:
            for bb in b.Children:
                #-note extra space in variable expression below: this is likely a bug in WEAP
                bb.Variables('Maximum Flow   Volume').Expression = 0
        elif ('_GW' in wap) & ('_SD' not in wap):
            for bb in b.Children:
                #-note extra space in variable expression below: this is likely a bug in WEAP
                bb.Variables('Maximum Flow   Volume').Expression = 0
    self.WEAP.SaveArea()
                
def remove_restriction_from_diverts(self):
    '''
    Remove restrictions from diverts.
    '''
    
    br = self.WEAP.Branch('\\Supply and Resources\\River').Children
    for b in br:
        if '_Divert' in b.Name:
            print('Removing restriction from %s' %b.Name)
            b.Variables('Maximum Diversion').Expression = 0
    self.WEAP.SaveArea()                

