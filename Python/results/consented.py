# -*- coding: utf-8 -*-

import pandas as pd
import datetime as dt
import calendar, os

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='August 2020'
############################################################################################


def getConsented(WEAP, config, syear, eyear, scenario_name, take_type, crc_df, active_ts):

    """
    Get time-series of consented volume for each day for take_type. Consented volume for each day is calculated as
    the sum of all WAP max pro rata rates of that take_type multiplied with the Active for each WAP.
    """

    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        if take_type == 'SW':
            outF = os.path.join(resultsDir, scenario_name + '_' + config.get('SW_TAKES', 'sw_consented_csv'))
        else:
            outF = os.path.join(resultsDir, scenario_name + '_' + config.get('GW_TAKES', 'gw_consented_csv'))
    else:
        if take_type == 'SW':
            outF = os.path.join(resultsDir, config.get('SW_TAKES', 'sw_consented_csv'))
        else:
            outF = os.path.join(resultsDir, config.get('GW_TAKES', 'gw_consented_csv'))

    active_ts = active_ts.loc[(active_ts.index >= pd.Timestamp(syear, 1, 1)) & (active_ts.index <= pd.Timestamp(eyear, 12, 31))]

    # Select crc & wap based on take_type
    if take_type == 'SW':
        crc_wap_df = crc_df.loc[crc_df.Activity == 'Take Surface Water']
    else:
        crc_wap_df = crc_df.loc[crc_df.Activity == 'Take Groundwater']
    crc_df = None
    del crc_df

    # Final dataframe
    final_df = pd.DataFrame(index=active_ts.index)

    # Calculate the allocated for only "in_sw_allo" and not in sw_allo
    for k in range(2):
        if k == 0:
            # First only the ones that are in surface water allocation
            crc_wap_df_sel = crc_wap_df.loc[crc_wap_df.in_sw_allo != 0]
        else:
            # Then the ones that are not in sw allo
            crc_wap_df_sel = crc_wap_df.loc[crc_wap_df.in_sw_allo == 0]

        # Dataframe to be filled
        df = pd.DataFrame(index=active_ts.index)

        # Get the proRata for the crc/wap
        for i in crc_wap_df_sel.iterrows():
            crc = i[1]['crc']
            wap = i[1]['wap_name_long']
            proRata = WEAP.Branch('\Other Assumptions\Consents\%s\%s\Max daily rate pro rata' % (crc, wap)).Variables('Annual Activity Level').Expression
            crc_wap = crc + '_' + wap
            df[[crc_wap]] = active_ts[[crc_wap]] * float(proRata)

        # Convert to l/s
        df['Sum [l/s]'] = df.sum(axis=1)
        df = df['Sum [l/s]'] / 86.4

        if k == 0:
            final_df['Sum in_sw_allo [l/s]'] = df
        else:
            final_df['Sum not in_sw_allo [l/s]'] = df

    final_df['Sum all [l/s'] = final_df['Sum in_sw_allo [l/s]'] + final_df['Sum not in_sw_allo [l/s]']
    final_df.to_csv(outF, header=True)


def getRestrictionVolume(WEAP, config, syear, eyear, scenario, scenario_name, take_type, crc_df):

    """
    Get time-series of restriction daily volume for each day for take_type.
    """

    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        if take_type == 'SW':
            outF = os.path.join(resultsDir, scenario_name + '_' + config.get('SW_TAKES', 'sw_restriction_csv'))
        else:
            outF = os.path.join(resultsDir, scenario_name + '_' + config.get('GW_TAKES', 'gw_restriction_csv'))
    else:
        if take_type == 'SW':
            outF = os.path.join(resultsDir, config.get('SW_TAKES', 'sw_restriction_csv'))
        else:
            outF = os.path.join(resultsDir, config.get('GW_TAKES', 'gw_restriction_csv'))

    # Select crc & wap based on take_type
    if take_type == 'SW':
        crc_wap_df = crc_df.loc[crc_df.Activity == 'Take Surface Water']
    else:
        crc_wap_df = crc_df.loc[crc_df.Activity == 'Take Groundwater']
    crc_df = None
    del crc_df

    # Final dataframe
    final_df = pd.DataFrame(index=pd.date_range(pd.Timestamp(syear, 1, 1), pd.Timestamp(eyear, 12, 31), freq='D'))
    final_df.index.name = 'Date'

    # Get the restriction daily volume for the crc/wap
    for i in crc_wap_df.iterrows():
        crc = i[1]['crc']
        wap = i[1]['wap_name_long']
        crc_wap = crc + '_' + wap
        br = WEAP.Branch('\Other Assumptions\Consents\%s\%s\Restriction daily volume' % (crc, wap)).FullName
        print('Getting maximum allowed for %s' % crc_wap)

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
            # Extract the value and convert from m3/d to l/s
            svalue = WEAP.ResultValue(br, cDate.year, dayOfYear, scenario) / 86.4
            final_df.loc[pd.Timestamp(cDate), crc_wap] = svalue

            # Increment date with one day
            cDate += dt.timedelta(days=1)

    final_df['Sum [l/s]'] = final_df.sum(axis=1)
    final_df.to_csv(outF)


def getAbstraction(WEAP, config, syear, eyear, scenario, scenario_name, take_type, crc_df):

    """
    Get time-series of daily abstractions for take_type.
    """

    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        if take_type == 'SW':
            outF = os.path.join(resultsDir, scenario_name + '_' + config.get('SW_TAKES', 'sw_abstracted_csv'))
        else:
            outF = os.path.join(resultsDir, scenario_name + '_' + config.get('GW_TAKES', 'gw_abstracted_csv'))
    else:
        if take_type == 'SW':
            outF = os.path.join(resultsDir, config.get('SW_TAKES', 'sw_abstracted_csv'))
        else:
            outF = os.path.join(resultsDir, config.get('GW_TAKES', 'gw_abstracted_csv'))

    # Select crc & wap based on take_type
    if take_type == 'SW':
        crc_wap_df = crc_df.loc[crc_df.Activity == 'Take Surface Water']
    else:
        crc_wap_df = crc_df.loc[crc_df.Activity == 'Take Groundwater']
    crc_df = None
    del crc_df

    # Final dataframe
    final_df = pd.DataFrame(index=pd.date_range(pd.Timestamp(syear, 1, 1), pd.Timestamp(eyear, 12, 31), freq='D'))
    final_df.index.name = 'Date'

    # Get the abstraction daily volume for the crc/wap
    for i in crc_wap_df.iterrows():
        crc = i[1]['crc']
        wap = i[1]['wap_name_long']
        crc_wap = crc + '_' + wap
        br = WEAP.Branch('\Other Assumptions\Consents\%s\%s\Supplied daily volume' % (crc, wap)).FullName
        print('Getting abstraction for %s' % crc_wap)

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
            # Extract the value and convert from m3/d to l/s
            svalue = WEAP.ResultValue(br, cDate.year, dayOfYear, scenario) / 86.4
            final_df.loc[pd.Timestamp(cDate), crc_wap] = svalue

            # Increment date with one day
            cDate += dt.timedelta(days=1)

    final_df['Sum [l/s]'] = final_df.sum(axis=1)
    final_df.to_csv(outF)


def getStreamDepletion(config, syear, eyear, scenario_name, sdTS):

    """
    Get time-series of stream depletion from model created SD file.
    """

    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        outF = os.path.join(resultsDir, scenario_name + '_' + config.get('GW_TAKES', 'stream_depletion_csv'))
    else:
        outF = os.path.join(resultsDir, config.get('GW_TAKES', 'stream_depletion_csv'))

    sdTS = sdTS.loc[(sdTS.index >= pd.Timestamp(syear, 1, 1)) & (sdTS.index <= pd.Timestamp(eyear, 12, 31))]

    sdTS = sdTS / 86.4
    sdTS['Sum [l/s]'] = sdTS.sum(axis=1)
    sdTS.to_csv(outF)


def getBandAllocated(WEAP, config, syear, eyear, scenario, scenario_name):

    """
    Get the allocated percentage per lowflow band for each time-steo.
    """

    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        outF = os.path.join(resultsDir, scenario_name + '_' + config.get('BAND_ALLOCATED', 'band_allocated_csv'))
    else:
        outF = os.path.join(resultsDir, config.get('BAND_ALLOCATED', 'band_allocated_csv'))

    lf_site_name = config.get('BAND_ALLOCATED', 'lf_site_name')

    # Final dataframe
    final_df = pd.DataFrame(index=pd.date_range(pd.Timestamp(syear, 1, 1), pd.Timestamp(eyear, 12, 31), freq='D'))
    final_df.index.name = 'Date'

    # Loop over all the bands belonging to the lowflow site
    br = WEAP.Branch('\Key Assumptions\Low Flows\%s' % lf_site_name).Children
    for b in br:
        print('Getting allocated percentage for %s' % b.Name)
        ballocated = b.FullName + '\\' + 'Ballocated'

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
            svalue = WEAP.ResultValue(ballocated, cDate.year, dayOfYear, scenario) * 100
            final_df.loc[pd.Timestamp(cDate), b.Name] = svalue

            # Increment date with one day
            cDate += dt.timedelta(days=1)

    final_df.to_csv(outF)
