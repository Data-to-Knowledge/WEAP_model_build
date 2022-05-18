# -*- coding: utf-8 -*-

'''
Functions in this file initialize a groundwater node properties dataframe, adds groundwater nodes to WEAP,
and sets groundwater node properties.
'''
import os
import pandas as pd
import numpy as np

from other_functions.reproject import reproject

#-Authorship information-###################################################################
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__date__ ='May 2022'
############################################################################################

def initGWTable(config):
    '''
    Returns directory that contains groundwater information and a dataframe containing
    groundwater node data. Dataframe contains data about: ID, Area km2, Depth [m], Specific yield [-],
    Storage capacity [MCM], Initial storage [MCM], and Maximum withdrawal [MCM], and latitude and 
    longitude of groundwater nodes. Dataframe can be used to automatically add groundwater nodes to WEAP.
    
    Requires a config.cfg as input argument
    '''
    
    #-read groundwater directory
    gw_dir = config.get('GROUNDWATER', 'gw_Dir')
    #-read selected groundwater IDs csv-file into dataframe
    id_csv = os.path.join(gw_dir, config.get('GROUNDWATER', 'gw_IDs')) 
    id_df = pd.read_csv(id_csv)
    #-get groundwater node infromation from csv-file and read into dataframe
    gw_csv = os.path.join(gw_dir, config.get('GROUNDWATER', 'gw_csv'))
    gw_df = pd.read_csv(gw_csv)
    gw_df.set_index('ID', inplace=True)
    gw_df.sort_index(inplace=True)
    #-add lon lat columns for catchment center points
    gw_df['Lon'] = np.nan
    gw_df['Lat'] = np.nan
    #-convert x and y to lon and lat and remove center point columns
    for i in gw_df.iterrows():
        x = i[1]['CenterX']
        y = i[1]['CenterY']
        x, y = reproject(2193, 4326, x, y) 
        gw_df.loc[i[0],'Lon'] = x
        gw_df.loc[i[0],'Lat'] = y
    gw_df.drop(['CenterX', 'CenterY'], axis=1, inplace=True)
    #-only keep selected IDs
    gw_df = gw_df.loc[id_df['ID']]
    gw_df.sort_index(inplace=True)
    #-return groundwater directory and groundwater dataframe
    return gw_dir, gw_df

    
def addGWnodes(WEAP, gw_df):
    '''
    Automatically add groundwater nodes to WEAP. Uses dataframe as input argument.
    Function removes any existing groundwater node and then adds all nodes from the dataframe using
    latitude and longitde as center-point.
     
    WEAP     = WEAP API
    gw_df    = groundwater info dataframe created by initGWTable
    '''
    
    WEAP.Verbose=0
    WEAP.View = 'Schematic'
    selBranches = WEAP.Branch('\Supply and Resources\Groundwater').Children

    #-clean all existing groundwater nodes
    print 'Removing old groundwater nodes...'
    for i in selBranches:
        print 'Deleted %s' %i.Name
        i.Delete()
    #-add groundwater nodes
    print 'Adding groundwater nodes...'
    for i in gw_df.iterrows():
        ID = 'GW_' + str(i[0])
        print ID
        lon = i[1]['Lon']
        lat = i[1]['Lat']
        q = None
        z=0
        while q == None:
            z+=1
            q = WEAP.CreateNode('Groundwater', lon, lat, ID)
            lon+= 0.001
            lat+= 0.001
            #-break the loop if groundwater node cannot be added for some reason
            if z==50:
                break
    selBranches = WEAP.Branch('\Supply and Resources\Groundwater').Children
    gw_nodes = []
    for i in selBranches:
        gw_nodes.append(i.Name)
        print '%s was added ' %i.Name
    ids = list(gw_df.index)
    for i in ids:
        ID = 'GW_' + str(i)
        if ID not in gw_nodes:
            print 'ERROR: %s was not added as Groundwater node. Needs to be fixed manually.' %ID 
            print 'Automation procedure was stopped.'
            WEAP.SaveArea()
            WEAP.Verbose=1
            exit(0)

    WEAP.View = 'Schematic'
    WEAP.SaveArea()
    WEAP.Verbose=1
    
def setGWprops(WEAP, gw_df, config):
    '''
    Set the properties for each of the groundwater nodes. Chosen default method for
    groundwater surface water interactions is the "Specify GW-SW flows" method. For
    details about this method see Section 4.10.5 of the WEAP user guide. It basically
    models groundwater as a bucket with a fixed storage capacity, initial storage
    volume, maximum withdrawl (0 = no limitation, otherwise 0.), and natural recharge.
    Natural recharge is taken from csv file with time-series of recharge from non-riverbed
    catchment area in MCM/day.
    
    WEAP     = WEAP API
    gw_df    = groundwater info dataframe created by initGWTable

    '''
    WEAP.Verbose=0
    WEAP.View = 'Schematic'
    
    #-get csv-file with natural recharge (specific runoff)
    gw_recharge = config.get('GROUNDWATER', 'Qspec')
    ids = list(gw_df.index.values)
    for ID in ids:
        s = '\\Supply and Resources\\Groundwater\\GW_' + str(ID)
        csv_column = ids.index(ID)+1
        if WEAP.Branch(s):
            #-set the method to 1:
            print 'Setting method for GW_%s to "Specify GW-SW flows"' %ID
            WEAP.Branch(s).Variables('Method').Expression = 1 # "Specify GW-SW flows"
            #-Set storage capacity
            scap = gw_df.loc[ID,['Storage capacity [MCM]']].values[0]
            print 'Setting storage capacity for GW_%s to %f MCM' %(ID, scap)
            WEAP.Branch(s).Variables('Storage Capacity').Expression = scap
            #-Set initial storage
            sinit = gw_df.loc[ID,['Initial storage [MCM]']].values[0]
            print 'Setting initial storage for GW_%s to %f MCM' %(ID, sinit)
            WEAP.Branch(s).Variables('Initial Storage').Expression = sinit
            #-Set maximum withdrawal
            mwithdrawal = gw_df.loc[ID,['Maximum withdrawal [MCM]']].values[0]
            print 'Setting maximum withdrawal for GW_%s to %f MCM' %(ID, mwithdrawal)
            WEAP.Branch(s).Variables('Maximum Withdrawal').Expression = mwithdrawal
            #-Set natural recharge
            WEAP.Branch(s).Variables('Natural Recharge').Expression = 'ReadFromFile(' + gw_recharge + ', ' + str(csv_column) + ', , , , Interpolate)'
            
    WEAP.View = 'Schematic'            
    WEAP.SaveArea()
    WEAP.Verbose=1
