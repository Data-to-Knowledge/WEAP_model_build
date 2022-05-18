# -*- coding: utf-8 -*-

import win32com.client
import ConfigParser
import os, sys
import pandas as pd
import streamflow
import groundwater
import consented
import RIRF_WCO
import coleridge

#-Authorship information-###################################################################
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__date__ ='May 2022'
############################################################################################

'''
This script is runs in combination with the results.cfg configuration file to report WEAP results. This script does not run the model
again, so it is assumed that all results of the last run are correct. It uses the other python scripts under the results folder.
'''

config = ConfigParser.RawConfigParser()
config.read('results.cfg')

area = config.get('GENERAL', 'Area')
scenario = config.get('GENERAL', 'Scenario')
scenarioName = config.get('GENERAL', 'Scenario_name')

WEAP = win32com.client.Dispatch('WEAP.WEAPApplication')

WEAP.ActiveArea = area
WEAP.ActiveScenario = scenario

# Period to analyse
sYear = config.getint('GENERAL', 'syear')
eYear = config.getint('GENERAL', 'eyear')

# Get the consents dataframe and only keep the records that are in the SWAZs below Fighting Hill and only keep surface and groundwater takes
crc_csv = config.get('GENERAL', 'crc_csv')
crc_df = pd.read_csv(crc_csv)
keep_SWAZ = config.get('GENERAL', 'keep_SWAZ').split(',')
crc_df = crc_df.loc[crc_df.SWAZ.isin(keep_SWAZ)]
crc_df = crc_df.loc[crc_df['Activity'].isin(['Take Surface Water', 'Take Groundwater']), ['crc', 'Activity', 'SWAZ', 'wap', 'wap_name_long', 'in_sw_allo']]

# Get time-series of wap/crc active
active_crcwap_ts = pd.read_csv(config.get('GENERAL', 'active_csv'), parse_dates=[0], index_col=0, dayfirst=True)

# Streamflow ####################################################################

# Report streamflow?
report_streamflow = config.getint('STREAMFLOW', 'get_streamflow')
if report_streamflow:
    print('Extracting streamflow simulations...')
    streamflow.getStreamflowTS(WEAP, config, sYear, eYear, scenario, scenarioName)

# SW takes ######################################################################

# Get consented sw takes
sw_consented_flag = config.getint('SW_TAKES', 'sw_consented_flag')
if sw_consented_flag:
    print('Extracting consented surface water takes...')
    consented.getConsented(WEAP, config, sYear, eYear, scenarioName, 'SW', crc_df, active_crcwap_ts)

# Get maximum allowed sw take (restriction daily volume)
sw_restriction_flag = config.getint('SW_TAKES', 'sw_restriction_flag')
if sw_restriction_flag:
    print('Extracting restriction daily volume for surface water takes...')
    consented.getRestrictionVolume(WEAP, config, sYear, eYear, scenario, scenarioName, 'SW', crc_df)

# Get abstracted sw (Supplied daily volume)
sw_abstracted_flag = config.getint('SW_TAKES', 'sw_abstracted_flag')
if sw_abstracted_flag:
    print('Extracting surface water abstractions...')
    consented.getAbstraction(WEAP, config, sYear, eYear, scenario, scenarioName, 'SW', crc_df)

# GW takes ######################################################################

# Get consented gw takes?
gw_consented_flag = config.getint('GW_TAKES', 'gw_consented_flag')
if gw_consented_flag:
    print('Extracting consented groundwater takes...')
    consented.getConsented(WEAP, config, sYear, eYear, scenarioName, 'GW', crc_df, active_crcwap_ts)

# Get maximum allowed gw take (restriction daily volume)
gw_restriction_flag = config.getint('GW_TAKES', 'gw_restriction_flag')
if gw_restriction_flag:
    print('Extracting restriction daily volume for groundwater takes...')
    consented.getRestrictionVolume(WEAP, config, sYear, eYear, scenario, scenarioName, 'GW', crc_df)

# Get abstracted gw (Supplied daily volume)
gw_abstracted_flag = config.getint('GW_TAKES', 'gw_abstracted_flag')
if gw_abstracted_flag:
    print('Extracting groundwater abstractions...')
    consented.getAbstraction(WEAP, config, sYear, eYear, scenario, scenarioName, 'GW', crc_df)

# Get stream depletion
stream_depletion_flag = config.getint('GW_TAKES', 'stream_depletion_flag')
if stream_depletion_flag:
    print('Extracting stream depletion time-series...')
    SD_ts = pd.read_csv(config.get('GW_TAKES', 'SD_results_csv'), parse_dates=[0], index_col=0, dayfirst=True)
    SD_ts.index.name = 'Date'
    consented.getStreamDepletion(config, sYear, eYear, scenarioName, SD_ts)

# Get natural losses from river to groundwater #################################

# Get natural river loss to groundwater?
get_natural_GW_loss_flag = config.get('GW_LOSS', 'get_natural_GW_loss_flag')
if get_natural_GW_loss_flag:
    print('Getting the natural river losses to groundwater...')
    groundwater.getNaturalLossGW(WEAP, config, sYear, eYear, scenario, scenarioName)

# Band allocation ###############################################################

# Get allocated percentage per band for each day?
band_allocated_flag = config.getint('BAND_ALLOCATED', 'band_allocated_flag')
if band_allocated_flag:
    print('Extracting allocated percentage per band...')
    consented.getBandAllocated(WEAP, config, sYear, eYear, scenario, scenarioName)

# RIRF and WCO ###############################################################

# Get RIRF per day
RIRF_flag = config.getint('RIRF_WCO', 'RIRF_flag')
if RIRF_flag:
    print('Extracting RIRF...')
    RIRF_WCO.getRIRF(WEAP, config, sYear, eYear, scenario, scenarioName)

# Get WCO max per day
WCOmax_flag = config.getint('RIRF_WCO', 'WCOmax_flag')
if WCOmax_flag:
    print('Extracting WCOmax...')
    RIRF_WCO.getWCOmax(WEAP, config, sYear, eYear, scenario, scenarioName)

# Compare WCOmin with simulated streamflow at different locations in the river
WCO_streamflow_comparison_flag = config.getint('RIRF_WCO', 'WCO_streamflow_comparison_flag')
if WCO_streamflow_comparison_flag:
    print('Extracting WCOmin and streamflow simulations...')
    RIRF_WCO.getWCOStreamflow(WEAP, config, sYear, eYear, scenario, scenarioName)

# LAKE COLERIDGE #################################################################

# Get time-series of lake coleridge
get_lake_data_flag = config.getint('LAKE', 'get_lake_data_flag')
if get_lake_data_flag:
    print('Extracting time-series for Lake Coleridge...')
    coleridge.getLakeTS(WEAP, config, sYear, eYear, scenario, scenarioName)
    
    
    

