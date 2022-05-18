# -*- coding: utf-8 -*-

'''
Functions in this file create different link types between WEAP nodes.
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



def connectSWGW(WEAP, config, catch_df, gw_df):
    '''
    Function that automatically connects a catchment node to a groundwater node by a
    Runoff/Infiltration link.
    
    WEAP     = WEAP API
    config   = configuration file
    catch_df = catchment info dataframe created by initCatchmentTable
    gw_df    = groundwater info dataframe created by initGWTable
    '''
    WEAP.Verbose=0
    WEAP.View = 'Schematic'
    
    print 'Creating Runoff/Infiltration links between catchments and groundwater nodes...'
    
    #-read the csv file containing the links into a dataframe
    link_csv = config.get('GWSW_LINKS', 'gwswLinkCSV')
    link_df = pd.read_csv(link_csv)
    #-get selected ids for which to create links
    ids = list(catch_df.index.values)
    #-narrow down the link_df to only the selected ids    
    link_df = link_df.loc[link_df['Catchment_ID'].isin(ids)]
    print link_df
    exit(0)
    
    #-remove existing links between catchment and gw nodes
    for ID in ids:
        b = '\\Supply and Resources\\Runoff and Infiltration\\from Catchment_' + str(ID)
        if WEAP.Branch(b):
            selBranches = WEAP.Branch(b).Children
            for s in selBranches:
                if 'to GW' in s.Name:  #-only remove links that connect to groundwater nodes
                    s.Delete()
                    print 'Runoff/Infiltration link %s was deleted' %s.Name
    #-add Runoff/Infiltration links between catchment and groundwater nodes
    for ID in ids:
        c = '\\Demand Sites and Catchments\\Catchment_' + str(ID)
        g = '\\Supply and Resources\\Groundwater\\GW_' + str(ID)
        WEAP.CreateLink('Runoff/Infiltration', WEAP.Branch(c), WEAP.Branch(g))
        print 'Created Runoff/Infiltration link between %s and %s' %(WEAP.Branch(c).Name, WEAP.Branch(g).Name)
    #-set the return flow fraction for each of the Runoff/Infiltration links
    for ID in ids:
        #-Obtain the return flow fraction that should go to the groundwater node
        gw_return = catch_df.loc[ID,['Return flow groundwater [%]']].values[0]
        b = '\\Supply and Resources\\Runoff and Infiltration\\from Catchment_' + str(ID)
        if WEAP.Branch(b):
            selBranches = WEAP.Branch(b).Children
            for s in selBranches:
                if 'to GW' in s.Name:  #-only add return flow properties if link goes to gw node
                    s.Variables('Runoff Fraction').Expression = gw_return

    WEAP.View = 'Schematic'
    WEAP.Verbose=1
    WEAP.SaveArea()
