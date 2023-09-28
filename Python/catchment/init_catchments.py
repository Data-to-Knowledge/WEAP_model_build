# -*- coding: utf-8 -*-

'''
Functions in this file initialize a sub-catchment properties dataframe, add sub-catchment as nodes to WEAP,
set sub-catchment properties, and post-processes SPHY model output to csv files that can be read by WEAP.
'''
import os
import pandas as pd
import numpy as np
from simpledbf import Dbf5
import datetime as dt

from other_functions.reproject import reproject

#-Authorship information-#######################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='August 2020'
################################################################################

def initCatchmentTable(config):
    '''
    Returns directory that contains catchment information and a dataframe containing
    sub-catchment data. Dataframe contains data about: ID, area km2, upstream area km2, riverbed area km2,
    land area km2, longitude and latitude of sub-catchment center points, and longitude and latitude of 
    riverbed center points. Dataframe can be used to automatically add sub-catchments as nodes to WEAP.
    
    Requires a config.cfg as input argument
    '''

    #-Set paths
    catchmentDir = config.get('CATCHMENTS', 'catchmentDir')
    catchmentDBF = os.path.join(catchmentDir, config.get('CATCHMENTS',
                                                          'catchmentDBF'))
    riverbedDBF = os.path.join(catchmentDir, config.get('CATCHMENTS', 
                                                        'riverbedDBF'))
    catchmentIDs = os.path.join(catchmentDir, config.get('CATCHMENTS',
                                                          'catchmentIDs'))
    
    #-read selected sub-catchment IDs
    id_df = pd.read_csv(catchmentIDs)
    #-read sub-catchment dbf
    print(catchmentDBF)
    dbf = Dbf5(catchmentDBF.replace('.shp','.dbf'))
    df = dbf.to_dataframe()
    col=df.columns[df.columns.str.contains('id',case=False,na=False)]
    df.set_index(col, inplace=True)
    df.sort_index(inplace=True)
    subcatch_df = df; df = None
    
    #-read riverbed dbf
    dbf = Dbf5(riverbedDBF.replace('.shp','.dbf'))
    df = dbf.to_dataframe()
    col=df.columns[df.columns.str.contains('id',case=False,na=False)]
    df.set_index(col, inplace=True)
    df.sort_index(inplace=True)

    #-add center x and center y and riverbed area to subcatch_df
    for i in df.iterrows():
        catch_id = int(i[1]['Catch_ID'])
        subcatch_df.loc[catch_id, 'Riverbed_ID'] = i[0]
        subcatch_df.loc[catch_id, 'CenterX_riverbed'] = i[1]['CenterX']
        subcatch_df.loc[catch_id, 'CenterY_riverbed'] = i[1]['CenterY']
        subcatch_df.loc[catch_id, 'Riverbed area km2'] = i[1]['Area km2']
    #-fill nans with zero
    subcatch_df['Riverbed area km2'].fillna(0., inplace=True)
    subcatch_df['Land area km2'] = subcatch_df['Area km2'] - subcatch_df['Riverbed area km2']
    #-try to drop Area and UpstrA columns if present
    try:
        subcatch_df.drop(['Area', 'UpstrA'], axis=1, inplace=True)
    except:
        pass
    
    #-add lon lat columns for catchment center points
    subcatch_df['Lon'] = np.nan
    subcatch_df['Lat'] = np.nan
    subcatch_df['Lon_riverbed'] = np.nan
    subcatch_df['Lat_riverbed'] = np.nan
    #-convert x and y to lon and lat and remove center point columns
    for i in subcatch_df.iterrows():
        #-catchment x and y centerpoints
        x = i[1]['CenterX']
        y = i[1]['CenterY']
        x, y = reproject(2193, 4326, x, y) 
        subcatch_df.loc[i[0],'Lon'] = x
        subcatch_df.loc[i[0],'Lat'] = y
        #-riverbed x and y centerpoints
        x = i[1]['CenterX_riverbed']
        y = i[1]['CenterY_riverbed']
        x, y = reproject(2193, 4326, x, y) 
        subcatch_df.loc[i[0],'Lon_riverbed'] = x
        subcatch_df.loc[i[0],'Lat_riverbed'] = y

    subcatch_df.drop(['CenterX','CenterY','CenterX_riverbed','CenterY_riverbed'],
                      axis=1, inplace=True)
    #-only keep selected IDs
    subcatch_df = subcatch_df.loc[id_df['ID']]
    subcatch_df.sort_index(inplace=True)
    #-return catchment directory and catchment dataframe
    return catchmentDir, subcatch_df

def addRiverbedCatchments(WEAP, catch_df):
    '''
    Automatically adds riverbed sub-catchments as catchment nodes to WEAP. Uses dataframe as
    input argument. Function removes any existing catchment and then adds all riverbed 
    catchments with area>0 from the dataframe using latitude and longitde as center-point.
     
    WEAP        = WEAP API
    catch_df    = catchment info dataframe created by initCatchmentTable
    '''
 
    WEAP.Verbose=0
    selBranches = WEAP.Branch('\Demand Sites and Catchments').Children
    riverbed_df = catch_df.loc[catch_df['Riverbed area km2']>0]  #-remove 
    # any records with zero area riverbed
    catch_df = None; del catch_df
 
    #-clean all existing catchments
    print('Removing old riverbed catchments...')
    for i in selBranches:
        if i.TypeName == 'Catchment':
            print('Deleted %s' %i.Name)
            i.Delete()

    #-add catchment nodes
    print('Adding riverbed catchments...')
    for i in riverbed_df.iterrows():
        ID = 'RB_Catchment_' + str(i[0])
        print(ID)
#         s = '\\Demand Sites and Catchments\\' + ID
        lon = i[1]['Lon_riverbed']
        lat = i[1]['Lat_riverbed']
        q = None
        z=0
        while q == None:
            z+=1
            q = WEAP.CreateNode('Catchment', lon, lat, ID)
            lon+= 0.001
            lat+= 0.001
            #-break the loop if catchment cannot be added for some reason
            if z==50:
                break

    selBranches = WEAP.Branch('\Demand Sites and Catchments').Children
    catchments = []
    for i in selBranches:
        if i.TypeName == 'Catchment':
            catchments.append(i.Name)
            print('%s was added ' %i.Name)
    ids = list(riverbed_df.index)
    for i in ids:
        ID = 'RB_Catchment_' + str(i)
        if ID not in catchments:
            print('ERROR: %s was not added as Catchment node. Needs to be fixed manually.' %ID) 
            print('Automation procedure was stopped.')
            WEAP.SaveArea()
            WEAP.Verbose=1
            exit(0)

    WEAP.SaveArea()
    WEAP.Verbose=1

def setRiverbedCatchmentProps(WEAP, catch_df, catchmentDir):
    '''
    Set the properties for each of the riverbed sub-catchments. Chosen catchment method is the
    "Rainfall Runoff (simplified coefficient method)". For details about this method see
    Section 4.9.1 of the WEAP user guide.
    
    Properties that need to be set for this method are:
    - riverbed area (Land Use tab)
    - kc (Land Use tab) - time-series of daily riverbed area average kc [-]
    - effective precipitation (Land Use tab) - set to zero because we want all water to go into the river (river losses are accounted for later on)
    - precipitation (Climate tab) - time-series of daily riverbed area average precipitation as calculated by SPHY [mm]
    - ETref (climate tab) - time-series of daily riverbed area average ETref as calculated by SPHY [mm]
    
    WEAP         = WEAP API
    catch_df     = catchment info dataframe created by initCatchmentTable
    catchmentDir = directory that contains catchment information
    '''
    WEAP.Verbose=0
    WEAP.View = 'Schematic'
    
    riverbed_df = catch_df.loc[catch_df['Riverbed area km2']>0]  #-remove
    # any records with zero area riverbed
    catch_df = None; del catch_df
    
    #-Read kc table from csv file into dataframe
    kc_df=pd.read_csv(os.path.join(catchmentDir,'Kc_rb.csv'),parse_dates=True, 
    dayfirst=True, index_col=0)
    #-Calculate average monthly kc
    kc_df_month = kc_df.groupby([kc_df.index.month]).mean()
    kc_df = None; del kc_df
    #-Get the column IDs to loop over and assign values for in WEAP
    ids = list(pd.read_csv(os.path.join(catchmentDir, 'Precipitation_rb.csv'), 
    parse_dates=True, dayfirst=True, index_col=0).columns.astype(np.int))
    for ID in ids:
        csv_column = ids.index(ID)+1
        s = '\\Demand Sites and Catchments\\RB_Catchment_' + str(ID)
        if WEAP.Branch(s):
            #-set the method to 1: 
            print('Setting method for RB_Catchment_%s to "Rainfall Runoff (simplified coefficient method)"' %ID)
            WEAP.Branch(s).Variables('Method').Expression = 1  # "Rainfall
            # Runoff (simplified coefficient method)"
            #-set precipitation csv-file read
            f = os.path.join(catchmentDir, 'Precipitation_rb.csv')
            WEAP.Branch(s).Variables('Precipitation').Expression = 'ReadFromFile(' + f + ', ' + str(csv_column) + ', , , , Interpolate)'
            #-set ETref csv-file read
            f = os.path.join(catchmentDir, 'ETr_rb.csv')
            WEAP.Branch(s).Variables('ETref').Expression = 'ReadFromFile(' + f + ', ' + str(csv_column) + ', , , , Interpolate)'

            #-Set the kc as a lookup function based on the month
            kc_lookup_str = 'Lookup(X, Y, Linear, Month, '
            for m in range(1, 12+1):
                kc = kc_df_month.loc[m, str(ID)]
                kc_lookup_str = kc_lookup_str + str(m) + ','
                if m<12:
                    kc_lookup_str = kc_lookup_str + str(kc) + ','
                else:
                    kc_lookup_str = kc_lookup_str + str(kc) + ')'
            WEAP.Branch(s).Variables('Kc').Expression = kc_lookup_str
            WEAP.Branch(s).Variables('Effective Precipitation').Expression = 0.0
            
            #-Set the catchment area - NOTE: unit has to be set manually 
            # otherwise it doesn't work            
            area = riverbed_df.loc[ID, 'Riverbed area km2']
            WEAP.Branch(s).Variables('Area').Expression = area
            
        else:
            print('ERROR: Catchment_%s is not present, and therefore properties \
                  cannot be set')
            print('Automation procedure was stopped.')
            WEAP.SaveArea()
            WEAP.Verbose=1
            exit(0)

    WEAP.View = 'Schematic'
    WEAP.SaveArea()
    WEAP.Verbose=1        

def postprocessSPHY(config, catchmentDir, catch_df, selSdate, selEdate):
    '''
    Converts a SPHY tss file into a pandas dataframe. A tss file is converted into a csv-file for each of the variables specified in varDict.
    The order of the columns corresponds with the sorted IDs of the catch_df dataframe. CSV-files are created for both the subcatchment area
    (excluding the riverbed), as well as for the riverbed area itself. 
    
    catchmentDir= directory in which catchment information can be found
    catch_df    = catchment info dataframe created by initCatchmentTable
    selSdate    = start date to select a period within the SPHY simulation date range
    selEdate    = end date to select a period within the SPHY simulation date range
    '''
    
    #-number of catchments (columns) for which a time-series has been created 
    # in the tss file    
    nrCatchments = config.getint('CATCHMENTS', 'nrCatchments')
    #-number of riverbed catchments (columns) for which a time-series has
    #  been created in the tss file -> does not have to be equal to nr of Catchments    
    nrRiverbedCatchments = config.getint('CATCHMENTS', 'nrRiverbedCatchments')
    #-first date in the SPHY tss file
    sdate = config.get('CATCHMENTS', 'SPHY_start').split(',')
    sdate = dt.date(int(sdate[0]), int(sdate[1]), int(sdate[2]))
    #-last date in the SPHY tss file
    edate = config.get('CATCHMENTS', 'SPHY_end').split(',')
    edate = dt.date(int(edate[0]), int(edate[1]), int(edate[2]))    
    
    #-set the selSdate to one year before (=baseyear of current accounts)
    year = selSdate.year - 1
    month = selSdate.month
    day = selSdate.day
    selSdate = dt.date(year, month, day)
    
    #-Dictionary with catchment variables to convert time-series for
    #  (non-riverbed area)
    catchmentVarDict = {'Precipitation': 'subcatch_PrecTSS.tss', 
    'ETr': 'subcatch_ETRTSS.tss', 'ETp': 'subcatch_ETPTSS.tss',
    'ETa': 'subcatch_ETATSS.tss','SurfaceRunoff': 'subcatch_RootRunoffTSS.tss',
    'Drainage': 'subcatch_RootDrainTSS.tss',
    'GwRecharge': 'subcatch_GWRechargeTSS.tss',
    'Baseflow': 'subcatch_BaseRTSS.tss','SnowRunoff': 'subcatch_SnowRTSS.tss',
    'GlacierRunoff': 'subcatch_GlacRTSS.tss', 'Kc': 'subcatch_KcTSS.tss'}
    
    #-Dictionary with riverbed catchment variables to concert time-series for
    riverbedVarDict = {'Precipitation_rb': 'riverbed_PrecTSS.tss', 
    'ETr_rb': 'riverbed_ETRTSS.tss', 'ETp_rb': 'riverbed_ETPTSS.tss', 
    'ETa_rb': 'riverbed_ETATSS.tss','Kc_rb': 'riverbed_KcTSS.tss'}

    #-sort index
    catch_df.sort_index(inplace=True)
    #-Extract dictionary keys and sort
    keys = catchmentVarDict.keys()
    keys = sorted(keys)
    #-loop over the variables and create a csv file for each of them
    for k in keys:
        print('Converting %s tss-file into a csv-file' %k)
        #-Read the SPHY tss file for the variable (k) into a pandas dataframe
        f = pd.read_csv(os.path.join(catchmentDir, catchmentVarDict[k]),
        delim_whitespace=True, skipinitialspace=True, skiprows=3+nrCatchments,
        header=None, index_col=False)
        f.drop(0, axis=1, inplace=True)
        #-Set the datetime as index and rename index column
        f.set_index(pd.date_range(sdate, edate), inplace=True)
        f.index.rename('Date', inplace=True)
        #-Select only values for the selected catchments
        f = f.loc[:,catch_df.index.values]
        #-Select a period from the dateframe if selection period is provided
        f = f.loc[selSdate:selEdate]
        #-reset index and re-format Date column to dd/mm/yyyy,
        #  otherwise WEAP doesn't understand it. Day first.
        f.reset_index(inplace=True)
        f['Date'] = f['Date'].dt.strftime('%d/%m/%Y')
        #-Write to csv-file
        f.to_csv(os.path.join(catchmentDir, k + '.csv'), index=False)

    #-Create a csv-file for the specific runoff [MCM per day] of the
    #  non-riverbed catchment area
    surface_runoff = pd.read_csv(os.path.join(catchmentDir,'SurfaceRunoff.csv'),
                                 parse_dates=True, dayfirst=True, index_col=0)
    drainage = pd.read_csv(os.path.join(catchmentDir, 'Drainage.csv'),
                           parse_dates=True, dayfirst=True, index_col=0)
    snowr = pd.read_csv(os.path.join(catchmentDir, 'SnowRunoff.csv'),
                        parse_dates=True, dayfirst=True, index_col=0)
    glacr = pd.read_csv(os.path.join(catchmentDir, 'GlacierRunoff.csv'),
                        parse_dates=True, dayfirst=True, index_col=0)
    basf = pd.read_csv(os.path.join(catchmentDir, 'Baseflow.csv'),
                       parse_dates=True, dayfirst=True, index_col=0)
    Qspec = (surface_runoff + drainage + basf + snowr + glacr) / 1000 #-
    # in m per day
    #-Convert mm/day to MCM/day
    landArea = catch_df[['Land area km2']].transpose() * 1000000  #-Calculate 
    # land area in m2
    Qspec = Qspec.multiply(landArea.iloc[0].values, axis=1) #-specific runoff 
    # in m3/day
    Qspec = Qspec / 1000000 #-MCM
    Qspec.reset_index(inplace=True)
    Qspec['Date'] = Qspec['Date'].dt.strftime('%d/%m/%Y')
    Qspec.to_csv(os.path.join(catchmentDir, 'Qspec_MCM.csv'), index=False)

    #-Create a csv-file for the riverbed fluxes
    riverbed_df = catch_df.loc[catch_df['Riverbed area km2']>0]  #-remove any 
    # records with zero area riverbed
    ids = list(riverbed_df['Riverbed_ID'].values.astype(np.int))
    ids = sorted(ids)
    #-define columns to use the catchment ID that corresponds with the river
    #  bed ID
    coldict = {}
    for ID in ids:
        z = catch_df.loc[catch_df['Riverbed_ID'] == ID].index.values[0]
        coldict[ID]=z
    #-Extract dictionary keys and sort
    keys = riverbedVarDict.keys()
    keys = sorted(keys)
    for k in keys:
        print('Converting %s tss-file into a csv-file' %k)
        #-Read the SPHY tss file for the variable (k) into a pandas dataframe
        f = pd.read_csv(os.path.join(catchmentDir, riverbedVarDict[k]), delim_whitespace=True, skipinitialspace=True, skiprows=3+nrRiverbedCatchments, header=None, index_col=False)
        f.drop(0, axis=1, inplace=True)
        #-Set the datetime as index and rename index column
        f.set_index(pd.date_range(sdate, edate), inplace=True)
        f.index.rename('Date', inplace=True)
        #-Select only values for the selected catchments
        f = f.loc[:,ids]
        #-use catchment ID for columns
        f = f.rename(columns=coldict)
        #-Select a period from the dateframe if selection period is provided
        f = f.loc[selSdate:selEdate]
        #-reset index and re-format Date column to dd/mm/yyyy, otherwise WEAP 
        # doesn't understand it. Day first.
        f.reset_index(inplace=True)
        f['Date'] = f['Date'].dt.strftime('%d/%m/%Y')
        #-Write to csv-file
        f.to_csv(os.path.join(catchmentDir, k + '.csv'), index=False)
