# -*- coding: utf-8 -*-

import pandas as pd
import datetime as dt
import calendar, os

#-Authorship information-###################################################################
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__date__ ='May 2022'
############################################################################################


def getRIRF(WEAP, config, syear, eyear, scenario, scenario_name):

    """
    Get time-series of daily RIRF
    """

    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        outF = os.path.join(resultsDir, scenario_name + '_' + config.get('RIRF_WCO', 'RIRF_csv'))
    else:
        outF = os.path.join(resultsDir, config.get('RIRF_WCO', 'RIRF_csv'))

    RIRF_branch = WEAP.Branch(config.get('RIRF_WCO', 'RIRF_branch')).FullName

    # Final dataframe
    final_df = pd.DataFrame(index=pd.date_range(pd.Timestamp(syear, 1, 1), pd.Timestamp(eyear, 12, 31), freq='D'))
    final_df.index.name = 'Date'

    # Initialize the time-loop
    cDate = dt.date(syear, 1, 1)
    eDate = dt.date(eyear, 12, 31)
    while cDate <= eDate:
        # Calculate the day number of the year
        dayOfYear = cDate.timetuple().tm_yday

        # Is current date not a leap year? If that is true, then dates after 28 February should have a day of the year + 1 day (WEAP always has 366 time-steps)
        leapY = calendar.isleap(cDate.year)
        if not leapY:
            if cDate > dt.date(cDate.year, 2, 28):
                dayOfYear += 1
        # Extract the value and convert to %
        svalue = WEAP.ResultValue(RIRF_branch, cDate.year, dayOfYear, scenario)
        final_df.loc[pd.Timestamp(cDate), 'RIRF [m3/s]'] = svalue

        # Increment date with one day
        cDate += dt.timedelta(days=1)

    final_df.to_csv(outF)


def getWCOmax(WEAP, config, syear, eyear, scenario, scenario_name):

    """
    Get time-series of maximum allowed WCO abstraction for each day
    """

    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        outF = os.path.join(resultsDir, scenario_name + '_' + config.get('RIRF_WCO', 'WCOmax_csv'))
    else:
        outF = os.path.join(resultsDir, config.get('RIRF_WCO', 'WCOmax_csv'))

    WCOmax_branch = WEAP.Branch(config.get('RIRF_WCO', 'WCOmax_branch')).FullName

    # Final dataframe
    final_df = pd.DataFrame(index=pd.date_range(pd.Timestamp(syear, 1, 1), pd.Timestamp(eyear, 12, 31), freq='D'))
    final_df.index.name = 'Date'

    # Initialize the time-loop
    cDate = dt.date(syear, 1, 1)
    eDate = dt.date(eyear, 12, 31)
    while cDate <= eDate:
        # Calculate the day number of the year
        dayOfYear = cDate.timetuple().tm_yday

        # Is current date not a leap year? If that is true, then dates after 28 February should have a day of the year + 1 day (WEAP always has 366 time-steps)
        leapY = calendar.isleap(cDate.year)
        if not leapY:
            if cDate > dt.date(cDate.year, 2, 28):
                dayOfYear += 1
        # Extract the value and convert to %
        svalue = WEAP.ResultValue(WCOmax_branch, cDate.year, dayOfYear, scenario)
        final_df.loc[pd.Timestamp(cDate), 'WCOmax [m3/s]'] = svalue

        # Increment date with one day
        cDate += dt.timedelta(days=1)

    final_df.to_csv(outF)


def getWCOStreamflow(WEAP, config, syear, eyear, scenario, scenario_name):

    """
    Get daily WCO min. and streamflow at certain locations and write to csv file
    """

    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        outF = os.path.join(resultsDir, scenario_name + '_' + config.get('RIRF_WCO', 'WCO_streamflow_csv'))
        inF = os.path.join(resultsDir, scenario_name + '_' + config.get('STREAMFLOW', 'streamflow_csv'))
    else:
        outF = os.path.join(resultsDir, config.get('RIRF_WCO', 'WCO_streamflow_csv'))
        inF = os.path.join(resultsDir, config.get('STREAMFLOW', 'streamflow_csv'))

    WCOmin_branch = WEAP.Branch(config.get('RIRF_WCO', 'WCOmin_branch')).FullName

    # File with streamflow simulations
    streamflow_sim_df = pd.read_csv(inF, parse_dates=[0], index_col=0, dayfirst=True)
    sites = streamflow_sim_df.columns

    # Final dataframe
    final_df = streamflow_sim_df.copy()

    # Initialize the time-loop
    cDate = dt.date(syear, 1, 1)
    eDate = dt.date(eyear, 12, 31)
    while cDate <= eDate:
        # Calculate the day number of the year
        dayOfYear = cDate.timetuple().tm_yday

        # Is current date not a leap year? If that is true, then dates after 28 February should have a day of the year + 1 day (WEAP always has 366 time-steps)
        leapY = calendar.isleap(cDate.year)
        if not leapY:
            if cDate > dt.date(cDate.year, 2, 28):
                dayOfYear += 1
        # Extract the value and convert to %
        svalue = WEAP.ResultValue(WCOmin_branch, cDate.year, dayOfYear, scenario)
        final_df.loc[pd.Timestamp(cDate), 'WCOmin [m3/s]'] = svalue

        # Increment date with one day
        cDate += dt.timedelta(days=1)

    # Calculate the difference between the simualated streamflow and WCOmin
    for s in sites:
        colName = s + ' - WCOmin [m3/s]'
        final_df[colName] = final_df[s] - final_df['WCOmin [m3/s]']

    final_df.to_csv(outF)


