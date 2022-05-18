# -*- coding: utf-8 -*-

import pandas as pd
import datetime as dt
import calendar, os

#-Authorship information-###################################################################
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__date__ ='May 2022'
############################################################################################


def getLakeTS(WEAP, config, syear, eyear, scenario, scenario_name):

    """
    Get time-series from Lake Coleridge:
        Lake inflow
        Lake outflow (Normal Water Release + Stored Water Release)
        Lake level
    """

    resultsDir = config.get('GENERAL', 'resultsDir')
    
    SWR = config.getint('LAKE', 'SWR')

    if len(scenario_name) > 0:
        outF = os.path.join(resultsDir, scenario_name + '_' + config.get('LAKE', 'lakeTS_csv'))
    else:
        outF = os.path.join(resultsDir, config.get('LAKE', 'lakeTS_csv'))

    # Branch containing lake inflow time-series
    infl_br = '\Supply and Resources\Local Reservoirs\Lake Coleridge:Inflow[CMS]'

    if SWR:
        # Branch containing Stored Water Release
        swr_br = '\Key Assumptions\Lake Coleridge\Flows\Stored Water Release'
        swr_br = WEAP.Branch(swr_br).FullName

    # Branch containing Normal Water Release
    nwr_br = '\Key Assumptions\Lake Coleridge\Flows\Normal Water Release'
    nwr_br = WEAP.Branch(nwr_br).FullName

    # Branch with Storage Volume
    storage_br = '\Supply and Resources\Local Reservoirs\Lake Coleridge:Storage Volume[Million Cubic Meter]'

    # Branch with lake level
    lakelevel_br = '\Supply and Resources\Local Reservoirs\Lake Coleridge:Storage Elevation[Meter]'

    if SWR:
        # Branch with Accessible Stored Water by the end of the day
        ASW_br = '\Key Assumptions\Lake Coleridge\Stored Water\End accessible stored water'
    
        # Branch with Warehouse Stored Water by the end of the day
        WSW_br = '\Key Assumptions\Lake Coleridge\Stored Water\End warehouse stored water'

    # Final dataframe
    final_df = pd.DataFrame(index=pd.date_range(pd.Timestamp(syear, 1, 1), pd.Timestamp(eyear, 12, 31), freq='D'))
    final_df.index.name = 'Date'

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

        # Inflow
        svalue = WEAP.ResultValue(infl_br, cDate.year, dayOfYear, scenario)
        final_df.loc[pd.Timestamp(cDate), 'Lake inflow [m3/s]'] = svalue

        # SWR
        if SWR:
            svalue = WEAP.ResultValue(swr_br, cDate.year, dayOfYear, scenario)
        else:
            svalue = 0
        final_df.loc[pd.Timestamp(cDate), 'SWR [m3/s]'] = svalue

        # NWR
        svalue = WEAP.ResultValue(nwr_br, cDate.year, dayOfYear, scenario)
        final_df.loc[pd.Timestamp(cDate), 'NWR [m3/s]'] = svalue

        # Actual lake storage
        svalue = WEAP.ResultValue(storage_br, cDate.year, dayOfYear, scenario)
        final_df.loc[pd.Timestamp(cDate), 'Lake Storage [MCM]'] = svalue

        # Lake level
        svalue = WEAP.ResultValue(lakelevel_br, cDate.year, dayOfYear, scenario)
        final_df.loc[pd.Timestamp(cDate), 'Lake level [masl]'] = svalue

        if SWR:
            # ASW
            svalue = WEAP.ResultValue(ASW_br, cDate.year, dayOfYear, scenario)
            final_df.loc[pd.Timestamp(cDate), 'ASW [MCM]'] = svalue

            # WSW
            svalue = WEAP.ResultValue(WSW_br, cDate.year, dayOfYear, scenario)
            final_df.loc[pd.Timestamp(cDate), 'WSW [MCM]'] = svalue

        else:
            final_df.loc[pd.Timestamp(cDate), 'ASW [MCM]'] = 0
            final_df.loc[pd.Timestamp(cDate), 'WSW [MCM]'] = 0

        # Increment date with one day
        cDate += dt.timedelta(days=1)

    final_df.to_csv(outF)
