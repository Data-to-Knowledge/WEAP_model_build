# -*- coding: utf-8 -*-

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='August 2020'
############################################################################################


from pyhydrotel import get_sites_mtypes, get_ts_data
import pandas as pd

server = 'sql2012prod04'
database = 'hydrotel'

#-Fighting Hill site id
id = ['168526']
#-Get mtypes for Fighting Hill
FH_mtypes = get_sites_mtypes(server, database, sites=id)
#-Fighting Hill flow data
FH_flow = get_ts_data(server, database, ['flow'], id, resample_code='T', period=15, from_date='2018-01-01', to_date='2018-10-03')
FH_flow = pd.DataFrame(FH_flow)
FH_flow.reset_index(inplace=True)
#-Fighting Hill flow data without change of rating curve
FH_flow_no_rating_change = get_ts_data(server, database, ['wilcos flow'], id, resample_code='T', period=15, from_date='2018-01-01', to_date='2018-10-03')
FH_flow_no_rating_change = pd.DataFrame(FH_flow_no_rating_change)
FH_flow_no_rating_change.reset_index(inplace=True)

#-Lake Coleridge site id for stored water release and Rakaia Irrigation Restriction Flow
id = ['6852602']
Coleridge_mtypes = get_sites_mtypes(server, database, sites=id)
SW_release = get_ts_data(server, database, ['non-hydro release lc'], id, resample_code='T', period=15, from_date='2018-01-01', to_date='2018-10-03')
SW_release = pd.DataFrame(SW_release)
SW_release.reset_index(inplace=True)

RIRF = get_ts_data(server, database, ['rakaia fh modified'], id, resample_code='T', period=15, from_date='2018-01-01', to_date='2018-10-03')
RIRF = pd.DataFrame(RIRF)
RIRF.reset_index(inplace=True)

new_df = pd.DataFrame()
new_df['Fighting Hill Flow'] = FH_flow['Value']
new_df['Fighting Hill Flow No Rating Curve Change'] = FH_flow['Value']
new_df['Stored Water Release'] = SW_release['Value']
new_df['RIRF'] = RIRF['Value']
new_df.set_index(FH_flow.DateTime, inplace=True)

new_df.to_csv(r'C:\Active\Projects\Rakaia\Data\Lake_Coleridge_Trustpower\IMS\Example_SWR_CSVs\hydrotel_exports.csv')


