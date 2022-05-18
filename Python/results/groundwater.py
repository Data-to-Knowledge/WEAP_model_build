# -*- coding: utf-8 -*-

import pandas as pd
import datetime as dt
import calendar, os

#-Authorship information-###################################################################
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__date__ ='May 2022'
############################################################################################


def getNaturalLossGW(WEAP, config, syear, eyear, scenario, scenario_name):

    """
    Get time-series of natural river losses to groundwater
    """

    inF = config.get('GW_LOSS', 'locations_csv')
    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        outF = os.path.join(resultsDir, scenario_name + '_' + config.get('GW_LOSS', 'natural_losses_to_gw_csv'))
    else:
        outF = os.path.join(resultsDir, config.get('GW_LOSS', 'natural_losses_to_gw_csv'))

    # Get the locations
    locations_df = pd.read_csv(inF)
    gw_perc_locations = locations_df['WEAP resultvalue outflow percentage'].tolist()
    location_names = locations_df['Name'].tolist()

    date_range = pd.date_range(pd.Timestamp(syear, 1, 1), pd.Timestamp(eyear, 12, 31), freq='D')
    df_final = pd.DataFrame(index=date_range, columns=location_names)
    df_final.index.name = 'Date'

    # Loop over the locations for which streamflow time-series are required
    for i in locations_df.iterrows():
        gw_perc_loc = i[1]['WEAP resultvalue outflow percentage']
        streamflow_loc = i[1]['WEAP resultvalue streamflow']

        # Initialize the loop
        cDate = dt.date(syear, 1, 1)
        eDate = dt.date(eyear, 12, 31)
        while cDate <= eDate:
            print(cDate)
            # Calculate the day number of the year
            dayOfYear = cDate.timetuple().tm_yday

            # Is current date not a leap year? If that is true, then dates after 28 February should have a day of the year + 1 day (WEAP always has 366 time-steps)
            leapY = calendar.isleap(cDate.year)
            if not leapY:
                if cDate > dt.date(cDate.year, 2, 28):
                    dayOfYear += 1

            perc_outflow = WEAP.ResultValue(gw_perc_loc, cDate.year, dayOfYear, scenario)
            streamflow = WEAP.ResultValue(streamflow_loc, cDate.year, dayOfYear, scenario)

            # Add the value to the dataframe
            df_final.loc[pd.Timestamp(cDate), location_names[gw_perc_locations.index(gw_perc_loc)]] = streamflow * perc_outflow * 0.01

            # Increment date with one day
            cDate += dt.timedelta(days=1)

    df_final['Sum [m3/s]'] = df_final.sum(axis=1)
    df_final.to_csv(outF)
