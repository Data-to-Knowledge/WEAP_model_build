import matplotlib.pyplot as plt
from matplotlib import rcParams
import datetime as dt
import pandas as pd
import numpy as np
#from scipy import stats
from matplotlib.ticker import MaxNLocator
rcParams.update({'font.size': 8})
import os


pd.options.display.max_columns = 100

#-Authorship information-###################################################################
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__date__ ='May 2022'
############################################################################################

#-Statistics from matlab documentary:
# https://au.mathworks.com/help/curvefit/evaluating-goodness-of-fit.html#bq_5kwr-3

#-a plotting function for confidence intervals on fitted function
def plot_ci_manual(t, s_err, n, x, x2, y2, ax=None):
    """Return an axes of confidence bands using a simple approach.

    Notes
    -----
    .. math:: \left| \: \hat{\mu}_{y|x0} - \mu_{y|x0} \: \right| \; \leq \; T_{n-2}^{.975} \; \hat{\sigma} \; \sqrt{\frac{1}{n}+\frac{(x_0-\bar{x})^2}{\sum_{i=1}^n{(x_i-\bar{x})^2}}}
    .. math:: \hat{\sigma} = \sqrt{\sum_{i=1}^n{\frac{(y_i-\hat{y})^2}{n-2}}}

    References
    ----------
    .. [1]: M. Duarte.  "Curve fitting," JUpyter Notebook.
       http://nbviewer.ipython.org/github/demotu/BMC/blob/master/notebooks/CurveFitting.ipynb

    """
    if ax is None:
        ax = plt.gca()

    ci = t*s_err*np.sqrt(1/n + (x2-np.mean(x))**2/np.sum((x-np.mean(x))**2))
    ax.fill_between(x2, y2+ci, y2-ci, color="#b9cfe7", edgecolor="")

    return ax

def demand_estimation_matrix(crc_csv, wap_csv, irf_csv, etr_csv, eta_csv, etp_csv, p_csv, catchment_csv):
    '''
    This function returns a pandas dataframe that can be used to establish relationships between (metered abstraction/maximum allowed abstraction ratios)
    and climate fluxes, such as e.g. the reference evapotranspiration or precipitation. The output of this function is used in matlab to correlate usage ratios
    with reference evapotranspiration (demand_analysis.m) 
    
    Output:
         The dataframe contains the following columns:
        - Date: daily for the period as can be found in the wap_ts_csv file
        - Month: date column converted to months
        - crc: consent number
        - wap: wap number
        - Activity: only 'Take Surface Water', 'Take Groundwater', and 'Divert Surface Water' are selected
        - use_type: all original use_types being re-categorized to 6 classes: Irrigation, Irrigation Scheme, Domestic, Hydropower, Stockwater, Other
        - complex_allo: column indicating whether it's a complex (1) or non-complex (0) consent/wap
        - lowflow_restriction: column indicating whether it has lowflow restrictions (yes=1, no=0)
        - crc_wap_max_vol [m3]: maximum consented wap volume for 'date, crc, wap, Activity, use_type' combination. Records where the maximum allocated volume per day is zero or missing have been removed.
        - crc_wap_metered_abstraction [m3]: volume of water that has been measured by a meter for that crc/wap combination. Records where no metered abstraction data is available have also been removed.
        - IRF: irrigation restriction flow in cumecs
        - ETr: reference evapotranspiration in mm
        - ETa: actual evapotranspiration in mm
        - ETp: potential evapotranspiration in mm
        - P: precipitation in mm
        - Year: date column converted to years

    Input:
        - crc_csv = csv-file with all detailed consent information for the period of interest and SWAZs. Is a result from the scrip consents.py
        - wap_csv = csv-file with a time-series of abstractions for each of the WAPs that can be found in the crc_csv. This file serves as a base for the definition of the start and end date to base analysis on.
        - irf_csv = csv-file with irrigation restriction flow
        - etr_csv = csv-file with each column containing the time-series of reference evapotranspiration for the catchment IDs specified in the header of catchment_csv 
        - eta_csv = csv-file with each column containing the time-series of actual evapotranspiration for the catchment IDs specified in the header of catchment_csv
        - etp_csv = csv-file with each column containing the time-series of potential evapotranspiration for the catchment IDs specified in the header of catchment_csv
        - p_csv = csv-file with each column containing the time-series of precipitation for the catchment IDs specified in the header of catchment_csv
        - catchment_csv = csv-file that links each WAP to the sub-catchment ID it resides in
    '''

    #-Read the csv files with time-series into dataframes
    wap_ts_df = pd.read_csv(wap_csv, parse_dates = [0], index_col=0, dayfirst=True)
    IRF_ts_df = pd.read_csv(irf_csv, parse_dates = [0], dayfirst=True)
    IRF_ts_df.columns=['Date', 'IRF']
    ETr_ts_df = pd.read_csv(etr_csv, parse_dates = [0], index_col=0, dayfirst=True)
    ETa_ts_df = pd.read_csv(eta_csv, parse_dates = [0], index_col=0, dayfirst=True)
    ETp_ts_df = pd.read_csv(etp_csv, parse_dates = [0], index_col=0, dayfirst=True)
    P_ts_df = pd.read_csv(p_csv, parse_dates = [0], index_col=0, dayfirst=True)
    #-Read the detailed consent information and sub-catchment the WAPs belong to into a dataframe
    crc_df = pd.read_csv(crc_csv, parse_dates=[1,2, 39, 40],dayfirst=True)
    WAP_subcatchment_df = pd.read_csv(catchment_csv)
    
    #-Get start and end date for which to extract the data
    sdate = wap_ts_df.index[0]; sdate = sdate.to_pydatetime().date()
    sdate = dt.date(sdate.year+1, sdate.month, sdate.day)
    edate = wap_ts_df.index[-1]; edate = edate.to_pydatetime().date()
    
    crc_df = crc_df[['crc', 'wap', 'fmDate', 'toDate', 'from_month', 'to_month', 'Activity', 'use_type', 'complex_allo', 'metered', 'lowflow_restriction',
                     'wap_max_rate [l/s]', 'wap_max_rate_pro_rata [l/s]', 'wap_max_vol_pro_rata [m3]', 'wap_return_period [d]']]
    
    #-rename use_type to 6 categories (Irrigation, Irrigation Scheme, Domestic, Hydropower, Stockwater, Other)
    use_type_dict = {'Irrigation - Pasture': 'Irrigation', 'Irrigation - Mixed': 'Irrigation', 'Aquaculture': 'Other', 'Community Water Supply': 'Domestic', 'Cooling Water (non HVAC)': 'Hydropower',
                     'Recreation/Sport': 'Other', 'Domestic Use': 'Domestic', 'Irrigation - Arable (Cropping)': 'Irrigation', 'Firefighting': 'Other',
                     'Industrial Use - Other': 'Other', 'Construction': 'Other', 'Augment Flow/Wetland': 'Other', 'Power Generation': 'Hydropower', 'Viticulture': 'Other'}
    crc_df.replace({'use_type': use_type_dict},inplace=True)
    #-replace nan use_type with 'Other'
    crc_df.loc[pd.isna(crc_df['use_type']), 'use_type'] = 'Other'
    
    #-only keep records with Activity not being equal to discharge, waps that have a meter
    crc_df = crc_df.loc[(crc_df['Activity'] != 'Discharge water to water') & (crc_df['metered']==1)]
    
    #-calculate maximum daily allowed volumes for each wap
    crc_df['wap_max_rate [m3]'] = crc_df['wap_max_rate [l/s]'] * 86.4
    crc_df['wap_max_rate_pro_rata [m3]'] = crc_df['wap_max_rate_pro_rata [l/s]'] * 86.4
    crc_df['wap_max_vol_pro_rata [m3]'] = crc_df['wap_max_vol_pro_rata [m3]'] / crc_df['wap_return_period [d]']
    #-if one of the above is zero, then replace with nan
    crc_df.loc[crc_df['wap_max_rate [m3]']==0, 'wap_max_rate [m3]'] = np.nan
    crc_df.loc[crc_df['wap_max_rate_pro_rata [m3]']==0, 'wap_max_rate_pro_rata [m3]'] = np.nan
    crc_df.loc[crc_df['wap_max_vol_pro_rata [m3]']==0, 'wap_max_vol_pro_rata [m3]'] = np.nan
    
    #-The 'wap_max_vol_pro_rata [m3]' field is the most reliable allocation volume, so this one is assigned to a new column named 'wap_max_vol [m3]' 
    crc_df['wap_max_vol [m3]'] = crc_df['wap_max_vol_pro_rata [m3]']
    
    #-If 'wap_max_vol [m3]' is empty, then fill with 'wap_max_rate_pro_rata [m3]'
    crc_df.loc[pd.isna(crc_df['wap_max_vol [m3]']),'wap_max_vol [m3]'] = crc_df.loc[pd.isna(crc_df['wap_max_vol [m3]']),'wap_max_rate_pro_rata [m3]']
    #-If it is still empty, then fill with 'wap_max_rate [m3]'
    crc_df.loc[pd.isna(crc_df['wap_max_vol [m3]']),'wap_max_vol [m3]'] = crc_df.loc[pd.isna(crc_df['wap_max_vol [m3]']),'wap_max_rate [m3]']
    #-drop unncessary columns
    crc_df.drop(['wap_max_rate [l/s]', 'wap_max_rate_pro_rata [l/s]', 'wap_max_vol_pro_rata [m3]', 'wap_return_period [d]', 'wap_max_rate [m3]', 'wap_max_rate_pro_rata [m3]', 'metered'],axis=1, inplace=True)
    crc_df.dropna(how='all', inplace=True)
    
    #-Define columns of dataframe to be created
    cols = list(crc_df.columns.drop(['fmDate', 'toDate']))
    cols.insert(0, 'Date')
    cols.insert(1, 'Month')
    cols.insert(2,'active')

    #-Initialize dataframe
    df = pd.DataFrame(columns=cols)
    dates_range = pd.date_range(sdate, edate, freq='D')
    df_full = df.copy()
    #-Convert dates to pandas datetime format
    sdate = pd.to_datetime(sdate)
    edate = pd.to_datetime(edate)
    #-Loop over all the consents/waps in the table, make a dataframe for the date range in which they were active, and add it to the final dataframe
    for c in crc_df.iterrows():
        fmDate = c[1]['fmDate']
        toDate = c[1]['toDate']
        if (fmDate<= edate) & (toDate>= sdate):
            print(c[1]['wap'])
            #-Temporaray dataframe for current consent/wap
            df_temp = df.copy()
            df_temp['Date'] = pd.date_range(c[1]['fmDate'], c[1]['toDate'], freq='D')
            df_temp['Month'] = df_temp['Date'].dt.strftime('%m')
            df_temp['Month'] = df_temp['Month'].astype(np.int) 
            df_temp['crc'] = c[1]['crc']
            df_temp['wap'] = c[1]['wap']
            df_temp['Activity'] = c[1]['Activity']
            df_temp['use_type'] = c[1]['use_type']
            df_temp['complex_allo'] = c[1]['complex_allo']
            df_temp['lowflow_restriction'] = c[1]['lowflow_restriction']
            df_temp['from_month'] = c[1]['from_month']
            df_temp['to_month'] = c[1]['to_month']
            df_temp['wap_max_vol [m3]'] =   c[1]['wap_max_vol [m3]']
            df_temp['active'] = 0
            mlist = []
            if c[1]['from_month'] < c[1]['to_month']:
                for m in range(int(c[1]['from_month']), int(c[1]['to_month'])+1):
                    mlist.append(m)
            elif c[1]['from_month'] > c[1]['to_month']:
                for m in range(int(c[1]['from_month']), 12+1):
                    mlist.append(m)
                for m in range(1, int(c[1]['to_month']) + 1):
                    mlist.append(m)
            else:
                mlist.append(int(c[1]['from_month']))
            df_temp.loc[df_temp['Month'].isin(mlist), 'active'] = 1
                              
            #-Concat temporary dataframe to final dataframe
            df_full = pd.concat([df_full, df_temp])
            df_temp = None; del df_temp
    crc_df = None; del crc_df
    
    #-Merge for the period of interest only
    df_final = pd.DataFrame(columns=['Date'])
    df_final['Date'] = dates_range
    df_final = pd.merge(df_final, df_full, how='left', on='Date')
    df_full = None; del df_full
    #-Drop records where consent is not executed
    df_final.loc[df_final['active']!=1] = np.nan
    df_final.dropna(how='all', inplace=True)
    df_final.drop(['from_month', 'to_month', 'active'], axis=1, inplace=True)
    df_final.drop_duplicates(inplace=True)
    
    #-Group by date and wap to calculate the maximum volume that may be extracted from a wap on a specific date
    df_wap_max = df_final.groupby(['Date', 'wap'])['wap_max_vol [m3]'].sum().reset_index()
    df_wap_max.set_index('Date', inplace=True)
    #-Merge the metered time-series
    df_wap_max['Metered Abstraction [m3]'] = np.nan
    for i in list(wap_ts_df.columns):
        #-Get the WAP ts
        df = wap_ts_df[[i]]
        df.columns = ['Metered Abstraction [m3]']
        df_wap_max.loc[df_wap_max['wap']==i,['Metered Abstraction [m3]']] = df
    wap_ts_df = None; del wap_ts_df
    df_wap_max.reset_index(inplace=True)
    #-Merge df_wap_max to df_final
    df_final = pd.merge(df_final, df_wap_max, how='left', on=['Date', 'wap'])
    df_final.rename(columns={'wap_max_vol [m3]_x': 'crc_wap_max_vol [m3]', 'wap_max_vol [m3]_y': 'wap_max_vol [m3]'}, inplace=True)
    df_final['crc_wap_max_vol_ratio [-]'] = df_final['crc_wap_max_vol [m3]'] / df_final['wap_max_vol [m3]']
    df_final['crc_wap_metered_abstraction [m3]'] = df_final['Metered Abstraction [m3]'] * df_final['crc_wap_max_vol_ratio [-]']
    df_wap_max = None; del df_wap_max
    df_final.drop(['wap_max_vol [m3]','Metered Abstraction [m3]','crc_wap_max_vol_ratio [-]'], axis=1, inplace=True)
    
    #-Remove records where no metered data is available
    df_final.loc[pd.isna(df_final['crc_wap_metered_abstraction [m3]'])] = np.nan
    df_final.dropna(how='all', inplace=True)
    
    #-merge the IRF
    df_final = pd.merge(df_final, IRF_ts_df, how='left', on='Date')
    
    #-merge the catchment IDs
    df_final = pd.merge(df_final, WAP_subcatchment_df, how='left', on='wap')
    df_final.loc[pd.isna(df_final['Catchment_ID'])] = np.nan
    df_final.dropna(how='all', inplace=True)
    df_final['Catchment_ID'] = df_final['Catchment_ID'].astype(int)
    IRF_ts_df = None; WAP_subcatchment_df = None; del IRF_ts_df, WAP_subcatchment_df
    
    #-Set date as index, which is needed to add time-series of ETr, and ETa/ETp
    df_final.set_index('Date', inplace=True)
    df_final['ETr'] = np.nan
    df_final['ETa'] = np.nan
    df_final['ETp'] = np.nan
    # df_final['ETa/ETp'] = np.nan
    df_final['P'] = np.nan
    #-Merge ETr and ETa/ETp with final dataframe
    for i in list(ETr_ts_df.columns):
        #-Get the catchment ETr
        df = ETr_ts_df[[i]]
        df.columns = ['ETr']
        df_final.loc[df_final['Catchment_ID']==int(i),['ETr']] = df
        #-Get the catchment ETa
        df = ETa_ts_df[[i]]
        df.columns = ['ETa']
        df_final.loc[df_final['Catchment_ID']==int(i),['ETa']] = df
        #-Get the catchment ETp
        df = ETp_ts_df[[i]]
        df.columns = ['ETp']
        df_final.loc[df_final['Catchment_ID']==int(i),['ETp']] = df
        #-Get the catchment P
        df = P_ts_df[[i]]
        df.columns = ['P']
        df_final.loc[df_final['Catchment_ID']==int(i),['P']] = df
    df_final.drop('Catchment_ID', axis=1, inplace=True)
    ETr_ts_df = None; ETa_ts_df = None; ETp_ts_df = None; P_ts_df = None; del ETr_ts_df, ETa_ts_df, ETp_ts_df, P_ts_df 
    
    #-Add years and months as columns
    df_final.reset_index(inplace=True)
    df_final['Month'] = (df_final['Date'].dt.strftime('%m')).astype(int)
    df_final['Year'] = (df_final['Date'].dt.strftime('%Y')).astype(int)
    
    #-Drop records where no max vol is availble or is zero (unreliable records)
    df_final.loc[(df_final['crc_wap_max_vol [m3]']==0) | pd.isna(df_final['crc_wap_max_vol [m3]'])] = np.nan
    df_final.dropna(inplace=True, how='all')
    
    return df_final

def get_WAP_alloc_metered_ts(crc_csv, wap_ts_csv):
    ''''
    This function returns a pandas dataframe with the following columns:
    - Date: daily for the period as can be found in the wap_ts_csv file
    - crc: all consents found in crc_csv
    - wap: all waps found in crc_csv
    - Activity: only 'Take Surface Water', 'Take Groundwater', and 'Divert Surface Water' are selected
    - use_type: all original use_types being re-categorized to 6 classes: Irrigation, Irrigation Scheme, Domestic, Hydropower, Stockwater, Other
    - crc_wap_max_vol [m3]: maximum consented wap volume for 'date, crc, wap, Activity, use_type' combination. Records where the maximum consented volume is missing or zero are removed (unreliable)
    - crc_wap_metered_abstraction [m3]: volume of water that has been measured by a meter for that crc/wap combination. A value of zero in this field does not necessarily mean there was zero abstraction.
                                        It may be that there was no metered value for that date. In that case the flag in the field 'metered [yes/no]' should be 0.
    - metered [yes/no]: flag saying if there was a metered value for that date and crc/wap combination.
    - ratio: fraction of metered volume (crc_wap_metered_abstraction [m3]) over maximum allowed volume (crc_wap_max_vol [m3]). Ideally, this value should range between 0 and 1.

    Input:
        - crc_csv = csv-file with all detailed consent information for the period of interest and SWAZs. Is a result from the scrip consents.py
        - wap_ts_csv = csv-file with a time-series for each of the WAPs that can be found in the crc_csv. Period of interest is equal to the start and end date as set in the WEAP model config.cfg file
    '''
    
    #-Read the csv files with time-series into dataframes
    wap_ts_df = pd.read_csv(wap_ts_csv, parse_dates = [0], index_col=0, dayfirst=True)
    #-Get start and end date for which to extract the data
    sdate = wap_ts_df.index[0]; sdate = sdate.to_pydatetime().date()
    #sdate = dt.date(sdate.year+1, sdate.month, sdate.day)
    edate = wap_ts_df.index[-1]; edate = edate.to_pydatetime().date()

    #-Read the detailed consent information and sub-catchment the WAPs belong to into a dataframe
    crc_df = pd.read_csv(crc_csv, parse_dates=[1,2, 39, 40],dayfirst=True)
    
    crc_df = crc_df[['crc', 'wap', 'fmDate', 'toDate', 'from_month', 'to_month', 'Activity', 'use_type', 'complex_allo', 'metered', 'lowflow_restriction',
                     'wap_max_rate [l/s]', 'wap_max_rate_pro_rata [l/s]', 'wap_max_vol_pro_rata [m3]', 'wap_return_period [d]']]
    
    #-rename use_type to 6 categories (Irrigation, Irrigation Scheme, Domestic, Hydropower, Stockwater, Other)
    use_type_dict = {'Irrigation - Pasture': 'Irrigation', 'Irrigation - Mixed': 'Irrigation', 'Aquaculture': 'Other', 'Community Water Supply': 'Domestic', 'Cooling Water (non HVAC)': 'Hydropower',
                     'Recreation/Sport': 'Other', 'Domestic Use': 'Domestic', 'Irrigation - Arable (Cropping)': 'Irrigation', 'Firefighting': 'Other',
                     'Industrial Use - Other': 'Other', 'Construction': 'Other', 'Augment Flow/Wetland': 'Other', 'Power Generation': 'Hydropower', 'Viticulture': 'Other'}
    crc_df.replace({'use_type': use_type_dict},inplace=True)
    #-replace nan use_type with 'Other'
    crc_df.loc[pd.isna(crc_df['use_type']), 'use_type'] = 'Other'
    
    #-only keep records with Activity not being equal to discharge
    crc_df = crc_df.loc[crc_df['Activity'] != 'Discharge water to water']
    
    #-calculate maximum daily allowed volumes for each wap
    crc_df['wap_max_rate [m3]'] = crc_df['wap_max_rate [l/s]'] * 86.4
    crc_df['wap_max_rate_pro_rata [m3]'] = crc_df['wap_max_rate_pro_rata [l/s]'] * 86.4
    crc_df['wap_max_vol_pro_rata [m3]'] = crc_df['wap_max_vol_pro_rata [m3]'] / crc_df['wap_return_period [d]']
    #-if one of the above is zero, then replace with nan
    crc_df.loc[crc_df['wap_max_rate [m3]']==0, 'wap_max_rate [m3]'] = np.nan
    crc_df.loc[crc_df['wap_max_rate_pro_rata [m3]']==0, 'wap_max_rate_pro_rata [m3]'] = np.nan
    crc_df.loc[crc_df['wap_max_vol_pro_rata [m3]']==0, 'wap_max_vol_pro_rata [m3]'] = np.nan
    
    #-The 'wap_max_vol_pro_rata [m3]' field is the most reliable allocation volume, so this one is assigned to a new column named 'wap_max_vol [m3]' 
    crc_df['wap_max_vol [m3]'] = crc_df['wap_max_vol_pro_rata [m3]']
    #-If 'wap_max_vol [m3]' is empty, then fill with 'wap_max_rate_pro_rata [m3]'
    crc_df.loc[pd.isna(crc_df['wap_max_vol [m3]']),'wap_max_vol [m3]'] = crc_df.loc[pd.isna(crc_df['wap_max_vol [m3]']),'wap_max_rate_pro_rata [m3]']
    #-If it is still empty, then fill with 'wap_max_rate [m3]'
    crc_df.loc[pd.isna(crc_df['wap_max_vol [m3]']),'wap_max_vol [m3]'] = crc_df.loc[pd.isna(crc_df['wap_max_vol [m3]']),'wap_max_rate [m3]']
    #-drop unncessary columns
    crc_df.drop(['wap_max_rate [l/s]', 'wap_max_rate_pro_rata [l/s]', 'wap_max_vol_pro_rata [m3]', 'wap_return_period [d]', 'wap_max_rate [m3]', 'wap_max_rate_pro_rata [m3]', 'metered'],axis=1, inplace=True)
    
    #-drop records where 'wap_max_vol [m3]' is zero or nan !!!!!!!!!!!!!!! These are dropped because it is unreliable can cannot be trusted
    crc_df.loc[(crc_df['wap_max_vol [m3]']==0) | pd.isna(crc_df['wap_max_vol [m3]'])] = np.nan
    crc_df.dropna(how='all', inplace=True)
    
    #-Define columns of dataframe to be created 
    cols = list(crc_df.columns.drop(['fmDate', 'toDate']))
    cols.insert(0, 'Date')
    cols.insert(1, 'Month')
    cols.insert(2,'active')
    
    #-Initialize dataframe
    df = pd.DataFrame(columns=cols)
    dates_range = pd.date_range(sdate, edate, freq='D')
    df_full = df.copy()
    #-Convert dates to pandas datetime format
    sdate = pd.to_datetime(sdate)
    edate = pd.to_datetime(edate)
    #-Loop over all the consents/waps in the table, make a dataframe for the date range in which they were active, and add it to the final dataframe
    for c in crc_df.iterrows():
        fmDate = c[1]['fmDate']
        toDate = c[1]['toDate']
        if (fmDate<= edate) & (toDate>= sdate):
            print(c[1]['wap'])
            #-Temporaray dataframe for current consent/wap
            df_temp = df.copy()
            df_temp['Date'] = pd.date_range(c[1]['fmDate'], c[1]['toDate'], freq='D')
            df_temp['Month'] = df_temp['Date'].dt.strftime('%m')
            df_temp['Month'] = df_temp['Month'].astype(np.int) 
            df_temp['crc'] = c[1]['crc']
            df_temp['wap'] = c[1]['wap']
            df_temp['Activity'] = c[1]['Activity']
            df_temp['use_type'] = c[1]['use_type']
            df_temp['complex_allo'] = c[1]['complex_allo']
            df_temp['lowflow_restriction'] = c[1]['lowflow_restriction']
            df_temp['from_month'] = c[1]['from_month']
            df_temp['to_month'] = c[1]['to_month']
            df_temp['wap_max_vol [m3]'] =   c[1]['wap_max_vol [m3]']
            df_temp['active'] = 0
            mlist = []
            if c[1]['from_month'] < c[1]['to_month']:
                for m in range(int(c[1]['from_month']), int(c[1]['to_month'])+1):
                    mlist.append(m)
            elif c[1]['from_month'] > c[1]['to_month']:
                for m in range(int(c[1]['from_month']), 12+1):
                    mlist.append(m)
                for m in range(1, int(c[1]['to_month']) + 1):
                    mlist.append(m)
            else:
                mlist.append(int(c[1]['from_month']))
            df_temp.loc[df_temp['Month'].isin(mlist), 'active'] = 1
            
            #-Concat temporary dataframe to final dataframe
            df_full = pd.concat([df_full, df_temp])
            df_temp = None; del df_temp
    crc_df = None; del crc_df
    
    #-Merge for the period of interest only
    df_final = pd.DataFrame(columns=['Date'])
    df_final['Date'] = dates_range
    df_final = pd.merge(df_final, df_full, how='left', on='Date')
    df_full = None; del df_full
    
    #-Drop records where consent is not executed
    df_final.loc[df_final['active']!=1] = np.nan
    df_final.dropna(how='all', inplace=True)
    df_final.drop(['Month', 'from_month', 'to_month', 'active'], axis=1, inplace=True)
    df_final.drop_duplicates(inplace=True)
    
    #-Group by date and wap to calculate the maximum volume that may be extracted from a wap on a specific date
    df_wap_max = df_final.groupby(['Date', 'wap'])['wap_max_vol [m3]'].sum().reset_index()
    df_wap_max.set_index('Date', inplace=True)
    #-Merge the metered time-series
    df_wap_max['Metered Abstraction [m3]'] = np.nan
    for i in list(wap_ts_df.columns):
        #-Get the WAP ts
        df = wap_ts_df[[i]]
        df.columns = ['Metered Abstraction [m3]']
        df_wap_max.loc[df_wap_max['wap']==i,['Metered Abstraction [m3]']] = df
    wap_ts_df = None; del wap_ts_df
    df_wap_max.reset_index(inplace=True)
    #-Merge df_wap_max to df_final
    df_final = pd.merge(df_final, df_wap_max, how='left', on=['Date', 'wap'])
    df_final.rename(columns={'wap_max_vol [m3]_x': 'crc_wap_max_vol [m3]', 'wap_max_vol [m3]_y': 'wap_max_vol [m3]'}, inplace=True)
    df_final['crc_wap_max_vol_ratio [-]'] = df_final['crc_wap_max_vol [m3]'] / df_final['wap_max_vol [m3]']
    df_final['crc_wap_metered_abstraction [m3]'] = df_final['Metered Abstraction [m3]'] * df_final['crc_wap_max_vol_ratio [-]']
    df_wap_max = None; del df_wap_max
    df_final.drop(['wap_max_vol [m3]','Metered Abstraction [m3]','crc_wap_max_vol_ratio [-]', 'complex_allo', 'lowflow_restriction'], axis=1, inplace=True)
    
    #-Add column with ones if record is measured for that crc/wap/date yes (1) or no (0)
    df_final['metered [yes/no]'] = np.nan
    df_final.loc[pd.isna(df_final['crc_wap_metered_abstraction [m3]']), 'metered [yes/no]'] = 0
    df_final.loc[pd.notna(df_final['crc_wap_metered_abstraction [m3]']), 'metered [yes/no]'] = 1
    #-Fill 'crc_wap_metered_abstraction [m3]' with 0 if field is nan ('metered [yes/no]' = 0)
    df_final.loc[pd.isna(df_final['crc_wap_metered_abstraction [m3]']), 'crc_wap_metered_abstraction [m3]'] = 0
    
    #-Calculate abstraction ratio
    df_final['ratio'] = df_final['crc_wap_metered_abstraction [m3]'] / df_final['crc_wap_max_vol [m3]']
    
    return df_final
    

def create_allo_metered_summary_tables(df, syear, remove_waps=False, ratio_threshold=None):
    '''
    This function returns two dataframes:
        - year_df: contains volumes aggregated by Year, Activity, and use_type. It has the following columns:
            - Year
            - Activity
            - use_type
            - Nr. of crcs: number of consents activte for that year, activity, and use_type
            - Nr. of waps: number of waps activte for that year, activity, and use_type
            - crc_wap_max_vol [MCM]
            - meter coverage [MCM]: this is the volume of crc_wap_max_vol that can potentially be measured with the meters that are in place
            - metered abstraction [MCM] this is the volume that has been metered
            - meter coverage [%]: this is the meter coverage expressed as %
            - metered abstraction [%] this is the % metered abstraction of the metered covered volume
        - avg_df: same as above, but then averaged over all years in the simulation period
    
    Input:
        - pandas dataframe returned by the function 'get_WAP_alloc_metered_ts'
        - start year to start analysis on (until end of records)
    Options:
        - remove_waps: if True, then remove waps that have an abstraction ratio > ratio_threshold
        - ratio_threshold: ratio_threshold that should be set if remove_waps is True. E.g. a value of 1.5 means that all records where the abstraction ratio > 1.5 
    '''
    #-copy of original dataframe
    df1 = df.copy()
    
    #-add year and month columns
    df1['Month'] = (df1['Date'].dt.strftime('%m')).astype(int)
    df1['Year'] = (df1['Date'].dt.strftime('%Y')).astype(int)
    df1.drop('Date', inplace=True, axis=1)
    df1 = df1.loc[df1['Year']>=syear]
    
    #-Check if outlier waps should be removed
    if remove_waps:
        outlier_waps = pd.unique(df1.loc[df1['ratio']>ratio_threshold, 'wap'])
        df1 = df1.loc[df1['ratio']<=ratio_threshold]
        print('These waps have days where the metered abstraction that is ' + str(ratio_threshold) + ' larger than their maximum allowed abstraction:')
        print(outlier_waps)

    ####-AGGREGATE BY YEAR        
    #-Groupby 'Year', 'Activity', 'use_type' to get the maximum allocated volume and metered volumes aggregated over those catgories
    year_df = df1.groupby(['Year', 'Activity', 'use_type']).sum().reset_index()
    year_df.drop(['metered [yes/no]', 'ratio', 'Month', 'crc_wap_metered_abstraction [m3]'], axis=1, inplace=True)
    #-Get the number of consents and waps per 'Year','Activity','use_type'
    year_df['Nr. of crcs'] = np.nan
    year_df['Nr. of waps'] = np.nan
    t = df1.groupby(['Year', 'Activity', 'use_type','crc','wap']).count().reset_index()
    for y in pd.unique(t['Year']):
        for a in pd.unique(t['Activity']):
            for u in pd.unique(t['use_type']):
                sel_df = t.loc[(t['Year']==y) & (t['Activity']==a) & (t['use_type']==u)]
                sel_crc = len(pd.unique(sel_df['crc']))
                sel_wap = len(pd.unique(sel_df['wap']))
                sel_df = None; del sel_df
                year_df.loc[(year_df['Year']==y) & (year_df['Activity']==a) & (year_df['use_type']==u), 'Nr. of crcs'] = sel_crc
                year_df.loc[(year_df['Year']==y) & (year_df['Activity']==a) & (year_df['use_type']==u), 'Nr. of waps'] = sel_wap

    #-Group as above, but then distinct volumes between metered and non-metered
    year_split_df = df1.groupby(['Year', 'Activity', 'use_type', 'metered [yes/no]']).sum().reset_index()
    year_split_df = year_split_df.loc[year_split_df['metered [yes/no]']==1]
    year_split_df.drop(['ratio', 'Month', 'metered [yes/no]'], axis=1, inplace=True)
    year_split_df.rename(columns={'crc_wap_max_vol [m3]': 'meter coverage [m3]'}, inplace=True)
    #-merge
    year_df = pd.merge(year_df, year_split_df, how='left', on=['Year', 'Activity', 'use_type']); year_split_df = None; del year_split_df
    year_df['meter coverage [%]'] = (year_df['meter coverage [m3]'] / year_df['crc_wap_max_vol [m3]'])*100
    year_df.rename(columns={'crc_wap_metered_abstraction [m3]': 'metered abstraction [m3]'}, inplace=True)
    year_df['metered abstraction [%]'] = (year_df['metered abstraction [m3]'] / year_df['meter coverage [m3]'])*100
    year_df.fillna(0, inplace=True)
    
    #-convert m3 to MCM
    year_df.rename(columns={'crc_wap_max_vol [m3]': 'crc_wap_max_vol [MCM]', 'meter coverage [m3]': 'meter coverage [MCM]', 'metered abstraction [m3]': 'metered abstraction [MCM]'}, inplace=True)
    year_df['crc_wap_max_vol [MCM]'] = year_df['crc_wap_max_vol [MCM]'] / 1000000
    year_df['meter coverage [MCM]'] = year_df['meter coverage [MCM]'] / 1000000
    year_df['metered abstraction [MCM]'] = year_df['metered abstraction [MCM]'] / 1000000
    
    ####-AVERAGE OVER ENTIRE PERIOD
    avg_df = year_df.groupby(['Activity', 'use_type']).mean().reset_index()
    avg_df['meter coverage [%]'] = (avg_df['meter coverage [MCM]'] / avg_df['crc_wap_max_vol [MCM]'])*100
    avg_df['metered abstraction [%]'] = (avg_df['metered abstraction [MCM]'] / avg_df['meter coverage [MCM]'])*100
    #-Get the number of consents and waps per 'Activity','use_type'
    t = df1.groupby(['Activity', 'use_type', 'crc', 'wap']).count().reset_index()
    avg_df['Nr. of crcs'] = np.nan
    avg_df['Nr. of waps'] = np.nan
    for a in pd.unique(t['Activity']):
        for u in pd.unique(t['use_type']):
            sel_df = t.loc[(t['Activity']==a) & (t['use_type']==u)]
            sel_crc = len(pd.unique(sel_df['crc']))
            sel_wap = len(pd.unique(sel_df['wap']))
            sel_df = None; del sel_df
            avg_df.loc[(avg_df['Activity']==a) & (avg_df['use_type']==u), 'Nr. of crcs'] = sel_crc
            avg_df.loc[(avg_df['Activity']==a) & (avg_df['use_type']==u), 'Nr. of waps'] = sel_wap
    avg_df.drop(['Year'], axis=1, inplace=True)
    
    return year_df, avg_df


def plot_allo_metered(df_analyse, fig_dir):
    '''
    Returns a plot containing for each unique combination of Activity and use_type:
        - consented volumes [MCM]
        - metered coverage [MCM]: volume of consented volume that can potentially be measured by the meters installed
        - metered coverage [%]: idem, but in percentage
        - metered abstraction [MCM]: volume of abstraction measured by the installed meters
    
    Results are plotted as summed values for each year in the simulation period
    
    Input:
        - df_analyse: pandas dataframe with annual values resulting from the 'create_allo_metered_summary_tables' function
        - fig_dir: directory where to save the figures
    '''
    #-color maps for plotting
    colors = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]
    for i in range(len(colors)):
        r, g, b = colors[i]
        colors[i] = (r / 255., g / 255., b / 255.)
    #-unique activities and use_types to loop over
    activities = pd.unique(df_analyse['Activity'])
    use_types = pd.unique(df_analyse['use_type'])
    for a in activities:
        for u in use_types:
            sel_df = df_analyse.loc[(df_analyse['Activity']==a) & (df_analyse['use_type']==u)]
            x = sel_df['Year']
            if len(x)>0:
                fig, ax1 = plt.subplots()
                ax1.set_xlabel('Year')
                ax1.set_ylabel('Volume [MCM]')
                h1, = ax1.plot(x, sel_df['crc_wap_max_vol [MCM]'], color='red')  
                h2, = ax1.plot(x, sel_df['meter coverage [MCM]'], color=colors[4]) # 5
                h3, = ax1.plot(x, sel_df['metered abstraction [MCM]'], color=colors[1])
                ax1.tick_params(axis='y', labelcolor='black')
                try:
                    ax1.set_xticks(x)
                    ax1.set_xlim([x.values[0], x.values[-1]])
                except:
                    pass
                try:
                    m = np.maximum(sel_df['crc_wap_max_vol [MCM]'], sel_df['meter coverage [MCM]'])
                    m = np.maximum(m, sel_df['metered abstraction [MCM]'])
                    m = np.nanmax(m)
                    m = m + m/100
                    ax1.set_ylim([0., m])
                except:
                    pass
                ax2 = ax1.twinx()
                h4, = ax2.plot(x, sel_df['meter coverage [%]'], color='green', linestyle='dashed')
                ax2.set_ylabel('Volume [%]')
                ax2.tick_params(axis='y', labelcolor='black')
                ax2.set_yticks(np.arange(0,110,10))
                ax2.set_ylim([0, 100])
                plt.legend([h1, h2, h4, h3],['Consented volume [MCM]', 'Meter coverage [MCM]', 'Meter coverage [%]', 'metered abstraction [MCM]'], loc='upper left', prop={'size': 8})
                plt.title(a + ' - ' + u)
                fig.tight_layout()
                plt.show()
                fname = os.path.join(fig_dir, 'meter_coverage_' + a + '_' + u + '.png')
#                 fname = os.path.join(fig_dir, 'meter_coverage_' + a + '_' + u + '_filtered.png')
                plt.savefig(fname, dpi=300.) 


def plot_crc_wap_numbers(df_analyse, fig_dir):
    '''
    Returns a plot for each unique combination of activity and use_type, showing the number of consents and waps for each year
    Input:
        - df_analyse: pandas dataframe with annual values resulting from the 'create_allo_metered_summary_tables' function
        - fig_dir: directory where to save the figures
    '''
    
    
    #-color maps for plotting
    colors = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]
    for i in range(len(colors)):
        r, g, b = colors[i]
        colors[i] = (r / 255., g / 255., b / 255.)
    #-unique activities and use_types to loop over
    activities = pd.unique(df_analyse['Activity'])
    use_types = pd.unique(df_analyse['use_type'])
    for a in activities:
        for u in use_types:
            sel_df = df_analyse.loc[(df_analyse['Activity']==a) & (df_analyse['use_type']==u)]
            x = sel_df['Year']
            y1 = sel_df['Nr. of crcs']
            y2 = sel_df['Nr. of waps']

            if len(y1)>0:
                fig, ax = plt.subplots()
                width=0.35
                ind = np.arange(len(x))
                p1 = ax.bar(ind, y1, width, color=colors[0])
                p2 = ax.bar(ind+width, y2, width, color=colors[1])
                ax.set_xticks(ind+width/2)
                ax.set_xticklabels((x))
                ax.set_xlabel('Year')
                ax.set_ylabel('Total number')
                ax.set_ylim([0, np.maximum(np.nanmax(y1), np.nanmax(y2)) + 1])
                plt.legend([p1, p2], ['Consents', 'WAPs'], loc='upper left')
                ax.set_title(a + ' - ' + u)
                ax.yaxis.set_major_locator(MaxNLocator(integer=True))
                fig.tight_layout()
                fname = os.path.join(fig_dir, 'nr_consents_waps_' + a + '_' + u + '.png')
#                 fname = os.path.join(fig_dir, 'nr_consents_waps_' + a + '_' + u + '_filtered.png')
                plt.savefig(fname, dpi=300.)


def est_equations(etr, activity, use_type):
    '''
    Returns a pandas dataframe with an estimated abstraction ratio.
    
    Input:
        - etr: pandas dataframe with reference evapotranspiration
        - activity: 'Take Surface Water', Divert Surface Water', or 'Take Groundwater'
        - use_type: 'Irrigation' , 'Irrigation Scheme', 'Domestic', 'Hydropower', 'Stockwater', 'Other'
    '''
    
    if activity == 'Take Surface Water':
        if use_type == 'Irrigation':
            y = np.minimum(1, np.maximum(0, 0.008 * np.power(etr, 2.337) + 0.033))
        elif use_type == 'Irrigation Scheme':
            y = np.minimum(1, np.maximum(0, 0.067 * etr - 0.011))            
        elif use_type == 'Stockwater':
            y = np.minimum(1, np.maximum(0, 0.016 * etr - 0.017))
        else:
            y = np.nan * etr
    elif activity == 'Take Groundwater':
        if use_type == 'Irrigation':
            y = np.minimum(1, np.maximum(0, 0.012 * np.power(etr, 2.403)))
        elif use_type == 'Stockwater':
            y = np.minimum(1, np.maximum(0, -0.368 * np.power(etr, -2.164) + 0.886))
        elif use_type == 'Domestic':
            y = np.minimum(1, np.maximum(0, 0.206 * etr - 0.16))
        else:
            y = np.nan * etr
    else:
        if use_type == 'Irrigation':
            y = np.minimum(1, np.maximum(0, 0.005 * np.power(etr, 2.395) + 0.081))
        else:
            y = np.nan * etr
    y.columns = ['estimated abstraction ratio']
    
    #-make sure 0 values for ETr are replaced with missing values    
    y.loc[etr['ETr']==0, 'estimated abstraction ratio'] = np.nan
    return y

def fill_meter_gaps(ETr_csv, GW_SD_locations_csv, sw_locations_csv, divert_locations_csv, WAP_subcatchment_csv, df, threshold):
    '''
    Input:
        - ETr_csv: csv-file with in each column a time-series of reference evapotranspiration for the corresponding sub-catchment
        - GW_SD_locations_csv: csv-file with WAPs that have their SD point in the area of interest
        - sw_locations_csv: csv-file with Surface Water Take WAPs that should be included
        - divert_locations_csv: csv-file with Divert WAPs that should be included
        - WAP_subcatchment_csv: csv-file that contains a column with all the WAPs and a column with the sub-catchment ID the WAP belongs to. This is used to link the reference evapotranspiration to the WAP.
        - df: pandas dataframe resulting from the function 'get_WAP_alloc_metered_ts'
        - threshold: threshold value that is used to determine a realistic abstraction ration; e.g. a threshold of 1.5 means a metered abstraction may never exceed 1.5 * crc_wap_max_vol [m3] 
    
    Returns 4 pandas dataframes:
        - df_estimated:
            - Date
            - wap 
            - crc_wap_max_vol [m3]: maximum consented volume for WAP for that day
            - crc_wap_metered_abstraction [m3]: abstraction according to meter on WAP
            - metered [yes/no]: tells whether there was a metered abstraction for that wap/day (yes=1, no=0)
            - estimated abstraction [m3]: abstraction estimated using one of the equations from 'est_equations'
            - estimated abstraction filled [m3]: where abstraction could not be estimated the field is filled with 'crc_wap_max_vol [m3]'
            - threshold_metered [m3]: metered value is limited to be maximum threshold * crc_wap_max_vol [m3]
            - scen1: 'threshold_metered [m3]' if metered value is available, otherwhise use 'estimated abstraction filled [m3]'
            - scen2: 'crc_wap_metered_abstraction [m3]' if metered value is available, otherwise use 'estimated abstraction filled [m3]'
            - scen3: use 'crc_wap_max_vol [m3]' for all dates
    '''
    
    sdate = pd.to_datetime(df['Date'].values[0])
    edate = pd.to_datetime(df['Date'].values[-1])


    #-create a dataframe for the dates to be filled
    df_temp = pd.DataFrame(columns=['Date'])
    df_temp['Date'] = pd.date_range(sdate, edate, freq='D')
    df1 = pd.merge(df_temp, df, how='left', on='Date')
    df_temp = None; del df_temp
    
    #-Get only the waps that have their SD point in one of the Rakaia SWAZs except the Little Rakaia
    gw_sd_df = pd.read_csv(GW_SD_locations_csv)
    gw_sd_df = pd.unique(gw_sd_df['wap'])
    df_temp1 = df1.loc[(df1['Activity']=='Take Groundwater') & df1['wap'].isin(gw_sd_df)]
    df_temp2 = df1.loc[(df1['Activity']!='Take Groundwater')]
    df1 = pd.concat([df_temp1, df_temp2])
    gw_sd_df = None; df_temp1 = None; df_temp2 = None; del gw_sd_df, df_temp1, df_temp2
    #-Get only the Surface Water Take WAPs that should be included
    sw_df = pd.read_csv(sw_locations_csv)
    sw_df = pd.unique(sw_df['wap'])
    df_temp1 = df1.loc[(df1['Activity']=='Take Surface Water') & df1['wap'].isin(sw_df)]
    df_temp2 = df1.loc[(df1['Activity']!='Take Surface Water')]
    df1 = pd.concat([df_temp1, df_temp2])
    sw_df = None; df_temp1 = None; df_temp2 = None; del sw_df, df_temp1, df_temp2
    #-Get only the Divert WAPs that should be included
    divert_df = pd.read_csv(divert_locations_csv)
    divert_df = pd.unique(divert_df['wap'])
    df_temp1 = df1.loc[(df1['Activity']=='Divert Surface Water') & df1['wap'].isin(divert_df)]
    df_temp2 = df1.loc[(df1['Activity']!='Divert Surface Water')]
    df1 = pd.concat([df_temp1, df_temp2])
    divert_df = None; df_temp1 = None; df_temp2 = None; del divert_df, df_temp1, df_temp2

    df_estimated = df1.groupby(['Date', 'wap', 'Activity', 'use_type']).sum().reset_index()
    df_estimated.drop('ratio', axis=1, inplace=True)
    df_estimated.loc[df_estimated['metered [yes/no]']>1, 'metered [yes/no]'] = 1
    df1 = None; del df1
    
    WAP_subcatchment_df = pd.read_csv(WAP_subcatchment_csv)
    df_estimated = pd.merge(df_estimated, WAP_subcatchment_df, how='left', on='wap')
    df_estimated.loc[pd.isna(df_estimated['Catchment_ID']), 'Catchment_ID'] = 10000000 #-use a big number to make sure it cannot be found in the catchment id list
    df_estimated['Catchment_ID'] = df_estimated['Catchment_ID'].astype(int)
    WAP_subcatchment_df = None; del WAP_subcatchment_df
    df_estimated['ETr'] = np.nan
    df_estimated.set_index('Date', inplace=True)
    
    #-add ETr
    ETr_ts_df = pd.read_csv(ETr_csv, parse_dates = [0], index_col=0, dayfirst=True)
    for i in ETr_ts_df.columns:
        if i != 'avg':
            #-Get the catchment ETr
            t = ETr_ts_df[[i]]
            t.columns = ['ETr']
            df_estimated.loc[df_estimated['Catchment_ID']==int(i),['ETr']] = t
    #-add the average ET for records with missing ET
    t = ETr_ts_df[['avg']]
    t.columns = ['ETr']
    df_estimated.loc[pd.isna(df_estimated['ETr']),['ETr']] = t
    ETr_ts_df = None; del ETr_ts_df
    #-for records where 'metered [yes/no]' == 0, the crc_wap_metered_abstraction [m3] is set to nan
    df_estimated.loc[df_estimated['metered [yes/no]']==0, 'crc_wap_metered_abstraction [m3]'] = np.nan
    
    #-calculate abstraction ratio to estimate demand
    df_estimated['estimated abstraction ratio'] = np.nan
    for a in pd.unique(df_estimated['Activity']):
        for u in pd.unique(df_estimated['use_type']):
            t = df_estimated.loc[(df_estimated['Activity']==a) & (df_estimated['use_type']==u),['ETr']]
            df_estimated.loc[(df_estimated['Activity']==a) & (df_estimated['use_type']==u),['estimated abstraction ratio']] = est_equations(t, a, u)
    df_estimated.drop('Catchment_ID', axis=1, inplace=True)
    df_estimated.reset_index(inplace=True)
    
    #-calculate the estimated abstration by multiplying ratio with max abstraction volume
    df_estimated['estimated abstraction [m3]'] = df_estimated['crc_wap_max_vol [m3]'] * df_estimated['estimated abstraction ratio']
    df_estimated.drop(['estimated abstraction ratio', 'ETr'], axis=1, inplace=True)
   
    #-if no relationship is available, then use monthly average ratio per use_type for the ratio    
    df_estimated['Month'] = df_estimated['Date'].dt.strftime('%m').astype(np.int)
    df_avg_month_ratio = df_estimated[['Month', 'use_type', 'crc_wap_max_vol [m3]', 'crc_wap_metered_abstraction [m3]', 'metered [yes/no]']]
    df_avg_month_ratio['ratio'] = df_avg_month_ratio['crc_wap_metered_abstraction [m3]'] / df_avg_month_ratio['crc_wap_max_vol [m3]']
    df_avg_month_ratio.loc[df_avg_month_ratio['ratio']>threshold,:] = np.nan
    df_avg_month_ratio.loc[df_avg_month_ratio['metered [yes/no]']==0,:] = np.nan
    df_avg_month_ratio.dropna(inplace=True)
    df_avg_month_ratio.drop(['crc_wap_max_vol [m3]', 'crc_wap_metered_abstraction [m3]', 'metered [yes/no]'], axis=1, inplace=True)
    df_avg_month_ratio = df_avg_month_ratio.groupby(['Month', 'use_type']).mean()
    df_avg_month_ratio.rename(columns={'ratio':'month_avg_ratio'}, inplace=True)
    df_avg_month_ratio.reset_index(inplace=True)
    #-merge avg monthly ratios per use_type to dataframe    
    df_estimated = pd.merge(df_estimated, df_avg_month_ratio, how='left', on=['Month', 'use_type'])
 
    #-calculate abstraction for fields for which a relationship was not found, so the avg monthly ratio is used
    df_estimated['estimated abstraction filled [m3]'] = df_estimated.loc[pd.isna(df_estimated['estimated abstraction [m3]']), 'crc_wap_max_vol [m3]'] * df_estimated.loc[pd.isna(df_estimated['estimated abstraction [m3]']), 'month_avg_ratio']
    df_estimated.loc[pd.isna(df_estimated['estimated abstraction filled [m3]']), 'estimated abstraction filled [m3]'] = df_estimated['estimated abstraction [m3]']
    #-if there are still missing values (i.e. no relationship was found and no avg monthly ratio could be calculated), then fill with maximum consented volume
    df_estimated.loc[pd.isna(df_estimated['estimated abstraction filled [m3]']), 'estimated abstraction filled [m3]'] = df_estimated['crc_wap_max_vol [m3]']
    df_estimated.drop(['Month', 'month_avg_ratio'], axis=1, inplace=True)
     
    #-groupby date and wap
    df_estimated = df_estimated.groupby(['Date', 'wap']).sum().reset_index()
    df_estimated.loc[df_estimated['metered [yes/no]']>1, 'metered [yes/no]'] = 1
    df_estimated.loc[df_estimated['metered [yes/no]']==0, 'crc_wap_metered_abstraction [m3]'] = np.nan
    df_estimated.loc[df_estimated['estimated abstraction [m3]']!=df_estimated['estimated abstraction filled [m3]'], 'estimated abstraction [m3]'] = np.nan
         
    #-make a field for 'threshold_metered [m3]': metered value cannot be larger than threshold * 'crc_wap_max_vol [m3]'
    df_estimated['ratio'] = df_estimated['crc_wap_metered_abstraction [m3]'] / df_estimated['crc_wap_max_vol [m3]']
    df_estimated['threshold_metered [m3]'] = np.nan
    df_estimated.loc[df_estimated['ratio']>threshold, 'threshold_metered [m3]'] = df_estimated['crc_wap_max_vol [m3]'] * threshold
    df_estimated.loc[df_estimated['ratio']<=threshold, 'threshold_metered [m3]'] = df_estimated['crc_wap_metered_abstraction [m3]']
    df_estimated.drop('ratio', inplace=True, axis=1)
     
    #-create the 3 scenarios for WAP time-series
    #-if metered, then use the minimum of the metered and threshold volume (filter out ratios>1). If not metered, then use estimated
    df_estimated['scen1'] = df_estimated.loc[df_estimated['metered [yes/no]']==1, 'threshold_metered [m3]']
    df_estimated.loc[df_estimated['metered [yes/no]']!=1, 'scen1'] = df_estimated['estimated abstraction filled [m3]']
    #-if metered, then use the metered volume (extreme high abstraciton ratios may be in here). If not metered, then use estimated
    df_estimated['scen2'] = df_estimated.loc[df_estimated['metered [yes/no]']==1, 'crc_wap_metered_abstraction [m3]']
    df_estimated.loc[df_estimated['metered [yes/no]']!=1, 'scen2'] = df_estimated['estimated abstraction filled [m3]']
    #-use max vol for scenario 3
    df_estimated['scen3'] = df_estimated['crc_wap_max_vol [m3]']
    
    #-Make 3 empty dataframes for the 3 scenarios to fill
    waps = pd.unique(df_estimated['wap'])
    df = pd.DataFrame(columns=waps, index=pd.date_range(sdate, edate, freq='D'))
    df.rename_axis('Date', inplace=True)
    df_scen1 = df.copy()
    df_scen2 = df.copy()
    df_scen3 = df.copy()
    df = None; del df
    #-fill the dataframes with data
    for w in waps:
        print(w)
        sel_df = df_estimated.loc[df_estimated['wap']==w,['Date', 'scen1', 'scen2', 'scen3']]
        sel_df.set_index('Date', inplace=True)
        df_scen1[[w]] = sel_df[['scen1']]
        df_scen2[[w]] = sel_df[['scen2']]
        df_scen3[[w]] = sel_df[['scen3']]
        sel_df = None; del sel_df
    #-missing values that represent dates for which WAP was not active are filled with zeros
    df_scen1.fillna(0, inplace=True); df_scen1.reset_index(inplace=True)
    df_scen2.fillna(0, inplace=True); df_scen2.reset_index(inplace=True)
    df_scen3.fillna(0, inplace=True); df_scen3.reset_index(inplace=True)
      
    #crc_wap_active.reset_index(inplace=True)
    df_estimated['Date'] = df_estimated['Date'].dt.strftime('%d/%m/%Y')
    df_scen1['Date'] = df_scen1['Date'].dt.strftime('%d/%m/%Y')
    df_scen2['Date'] = df_scen2['Date'].dt.strftime('%d/%m/%Y')
    df_scen3['Date'] = df_scen3['Date'].dt.strftime('%d/%m/%Y')
    #crc_wap_active.set_index('Date', inplace=True)
      
    return df_avg_month_ratio, df_estimated, df_scen1, df_scen2, df_scen3
              
####################-SECTION BELOW TO BE FILLED IN BY USER-#######################################

#-csv file with time-series of metered water takes
#wap_ts_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\Rakaia_wapTS_20190221.csv'
#wap_ts_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\Rakaia_wapTS_20190227.csv'
wap_ts_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\Rakaia_wapTS_20190402.csv'
#-csv file with time-series of Irrigation Restriction Flow (IRF)
IRF_ts_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\lowflows\6852602_IRF.csv'
#-csv-file with time-series of modelled ETr
ETr_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\catchment\ETr.csv'
#-csv-file with time-series of modelled ETa
ETa_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\catchment\ETa.csv'
#-csv-file with time-series of modelled ETp
ETp_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\catchment\ETp.csv'
#-csv-file with time-series of precipitation
P_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\catchment\Precipitation.csv'

#-csv file with detailed consent information
#crc_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\Rakaia_crc_full_20190221.csv'
#crc_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\Rakaia_crc_full_20190227.csv'
crc_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\Rakaia_crc_full_20190402_edits.csv'
#-csv-file with WAPs linked to WEAP sub-catchment ID
WAP_subcatchment_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\WAP_subcatchment.csv'


# ###################-DEMAND ESTIMATION-###########################################
# #-Get matrix that serves as a base for the demand estimation analysis (demand_analysis.m)
# df = demand_estimation_matrix(crc_csv, wap_ts_csv, IRF_ts_csv, ETr_csv, ETa_csv, ETp_csv, P_csv, WAP_subcatchment_csv) 
# df.to_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\demand_estimation_matrix_20190221.csv', index=False)
####################-METER COVERAGE ANALYSIS-#######################################
 
#-Threshold for removing outliers in metered volumnes: being larger than the ratio_threshold * maximum allowed volume 
ratio_threshold = 1.5
 
#-get the max. allowed volumes and metererd abstractions
df = get_WAP_alloc_metered_ts(crc_csv, wap_ts_csv)
df.to_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\allo_metered_volumes_20190402_edits.csv', index=False)
df = pd.read_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\allo_metered_volumes_20190402_edits.csv', index_col=0, parse_dates=True, dayfirst=True).reset_index()
 
# #-aggregate the results to annual values and overal average over the simulation period. 
# [agg_year, avg]  = create_allo_metered_summary_tables(df, 2008)
# agg_year.to_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\annual_allo_metered_volumes_20190221.csv', index=False)
# avg.to_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\avg_metered_allo_volumes_20190221.csv', index=False)

# #-Folder where to save figures
# fdir = r'C:\Active\Projects\Rakaia\Figs\demand_analyses'
# #-Plot the consented volumes and the amount covered by meters
# plot_allo_metered(agg_year, fdir)
# #-Plot the number of consents and waps
# plot_crc_wap_numbers(agg_year, fdir)    


#####################-FILL THE GAPS IN THE METERS        
#-csv-file with time-series of modelled ETr (all catchments are included and also an average ETr column of sub-catchments 1, 11, 13, 15, 16, 17)
ETr_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\catchment\ETr_all_subcatchment_IDs.csv'            
GW_SD_locations_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\GW_SD_locations_except_Little_Rakaia_WAPs_20190402_edits.csv'
sw_locations_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\SW_takes_edits_except_Little_Rakaia_WAPs.csv'
divert_locations_csv = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\diverts_edits_WAPs.csv'

[avg_month_ratio, estimated, sc1, sc2, sc3] = fill_meter_gaps(ETr_csv, GW_SD_locations_csv, sw_locations_csv, divert_locations_csv, WAP_subcatchment_csv, df, ratio_threshold)

avg_month_ratio.to_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\avg_month_ratio.csv', index=False)
estimated.to_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\volumes_by_wap.csv', index=False)
sc1.to_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\scenario_1.csv', index=False)
sc2.to_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\scenario_2.csv', index=False)
sc3.to_csv(r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\consents\scenario_3.csv', index=False)


