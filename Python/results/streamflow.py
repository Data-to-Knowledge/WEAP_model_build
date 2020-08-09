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


def getStreamflowTS(WEAP, config, syear, eyear, scenario, scenario_name):
    '''
    Function is imported in Get_WEAP_results.py
    '''

    inF = config.get('STREAMFLOW', 'locations_csv')
    resultsDir = config.get('GENERAL', 'resultsDir')

    if len(scenario_name) > 0:
        outF = os.path.join(resultsDir, scenario_name + '_' + config.get('STREAMFLOW', 'streamflow_csv'))
    else:
        outF = os.path.join(resultsDir, config.get('STREAMFLOW', 'streamflow_csv'))

    result_val_col = config.get('STREAMFLOW', 'resultvalue_column_name')
    label_val_col = config.get('STREAMFLOW', 'label_column_name')

    locations_df = pd.read_csv(inF)
    # drop records where results value is missing
    locations_df = locations_df.loc[pd.notna(locations_df[result_val_col])]
    # get the labels that should be used for each resultvalue
    labels_df = locations_df[label_val_col].tolist()

    date_range = pd.date_range(pd.Timestamp(syear, 1, 1), pd.Timestamp(eyear, 12, 31), freq='D')
    df_final = pd.DataFrame(index=date_range, columns=labels_df)
    df_final.index.name = 'Date'

    # Initialize the loop
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

        # Loop over the locations for which streamflow time-series are required
        for i in locations_df.iterrows():
            v = i[1][result_val_col]
            l = i[1][label_val_col]
            svalue = WEAP.ResultValue(v, cDate.year, dayOfYear, scenario)
            # Add the value to the dataframe
            df_final.loc[df_final.index == pd.Timestamp(cDate), l] = svalue

        # Increment date with one day
        cDate += dt.timedelta(days=1)

    print('Writing results to %s' % outF)
    df_final.to_csv(outF)
    print('Streamflow simulations succesfully written.')
