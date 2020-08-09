# -*- coding: utf-8 -*-

import win32com.client
import os, sys
import pandas as pd

pd.options.display.max_columns = 100

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='August 2020'
############################################################################################

'''
This script runs independently from WEAP.py, and is created to:
    1 - Creating a list of consents under the branch: Key Assumptions\\Lake Coleridge\\XXX     with XXX the name of the irrigator group in Trustpower's IMS
    2 - Each consent number in this list then contains the sum of the max. WAP rates for that consent at a certain moment in time (depending on active yes/no)
    3 - Sums the sums of 2)
    4 - Creates a Stored Water Release (SWR) branch under: Other Assumptions\\Consents\\crcXXX\\wapXXX for each consent part of the list created under 2)
    5 - Adds the SWR to the restriction daily volume for that crc/wap combination
'''

# Root folder where the data beloning to the different stored water groups is stored
rootDir = r'C:\Active\Projects\Rakaia\MODEL\WEAP\data\coleridge'

# -Folder names of the Stored Water Groups under the root directory
Stored_Water_Groups = ['BCI', 'CPW', 'MCL', 'RRI']
# -Name of WEAP area to use
area = 'TEST'

# -Read irrigator groups in dataframes
print('Reading IMS irrigator group data into dataframes...')
BCI_df = pd.read_csv(os.path.join(rootDir, 'BCI_group_WEAP.csv'))
CPW_df = pd.read_csv(os.path.join(rootDir, 'CPW_group_WEAP.csv'))
MCL_df = pd.read_csv(os.path.join(rootDir, 'MCL_group_WEAP.csv'))
RRI_df = pd.read_csv(os.path.join(rootDir, 'RRIA_group_WEAP.csv'))

print('Loading WEAP area: %s' % area)
WEAP = win32com.client.Dispatch('WEAP.WEAPApplication')
WEAP.ActiveArea = area
WEAP.ActiveScenario = 'Reference'

# #-Below needs to be done for each group (loop)

for s in Stored_Water_Groups:
    print('Processing Key Assumptions for IMS irrigator group %s...' % s)

    br = WEAP.Branch('\\Key Assumptions\\Lake Coleridge\\' + s)
    print('Adding "Consents sum max rates" under branch "\\Key Assumptions\\Lake Coleridge\\%s"...' % s)
    br2 = br.AddChild('Consents sum max rates')

    # -Get the dataframe for the selected group
    df = eval(s + '_df.copy()')
    # -unique consents for that group
    crc_unique = pd.unique(df.crc).tolist()

    # #-Key Assumptions part
    # -loop over the consents and summarize the maximum rates for the group
    for c in crc_unique:
        print('Processing IMS irrigator group %s and consent %s...' % (s, c))
        # -Add consent number under \\Key Assumptions\\Lake Coleridge\\XX-Group-XX\\   with XX-Group-XX one of the four IMS groups
        br3 = br2.AddChild(c)

        # -select only the waps and values belonging to this consent number
        sel_df = df.loc[df.crc == c]
        # -Calculate the sums of the WAP max rates belonging to the consent
        expr_str = ''
        cnt = 0
        for w in sel_df.iterrows():
            cnt += 1
            if cnt == len(sel_df):
                expr_str += WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + w[1]['wap_name'] + '\\Max daily rate').FullName
            else:
                expr_str += WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + w[1]['wap_name'] + '\\Max daily rate').FullName + ' + '
        br3.Variables('Annual Activity Level').Expression = expr_str
        br3.Variables('Annual Activity Level').ScaleUnit = 'm^3'

    # -summary the max rates of the consents under this group
    print('Summarizing the maximum rates of the consents under IMS irrigator group %s...' % s)
    expr_str = ''
    crs = br2.Children
    for i in crs:
        expr_str += i.FullName + ' + '
    expr_str = expr_str.rstrip(' + ')
    br3 = br2.AddChild('Sum max rates')
    br3.Variables('Annual Activity Level').Expression = expr_str
    br3.Variables('Annual Activity Level').ScaleUnit = 'm^3'

    print('Key Assumptions for IMS irrigator group %s finished.' % s)

    # #-Other Assumptions part
    print('Processing Other Assumptions for IMS irrigator group %s...' % s)

    for c in crc_unique:
        # -select only the waps and values belonging to this consent number
        sel_df = df.loc[df.crc == c]
        # -loop over the waps for that consent
        for i in sel_df.iterrows():
            w = i[1]['wap_name']
            print('Calculating SWR for IMS irrigator group %s, consent %s, and %s...' % (s, c, w))
            crc_wap_branch = WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + w)
            # -Add Stored Water Release branch to crc/wap combination
            swr_br = crc_wap_branch.AddChild('Stored Water Release')
            if WEAP.BranchExists(swr_br):
                print('WEAP Branch %s already exists. Process is stopped.' % swr_br.FullName)
                sys.exit()
            expr_str = '(' + WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + w + '\\Max daily rate').FullName + ' / ' + br3.FullName + ') * ' + WEAP.Branch('\\Key Assumptions\\Lake Coleridge\\' + s + '\\Stored Water Release').FullName + ' * 24 * 3600'
            swr_br.Variables('Annual Activity Level').Expression = expr_str
            swr_br.Variables('Annual Activity Level').ScaleUnit = 'm^3'

            print('Update Restriction daily volume for IMS irrigator group %s, consent %s, and %s...' % (s, c, w))
            # -Add the Stored Water Release to the Restriction daily volume branch
            expr_str = WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + w + '\\Restriction daily volume').Variables('Annual Activity Level').Expression
            expr_str += ' + ' + swr_br.FullName
            WEAP.Branch('\\Other Assumptions\\Consents\\' + c + '\\' + w + '\\Restriction daily volume').Variables('Annual Activity Level').Expression = expr_str

    print('Other Assumptions for IMS irrigator group %s finished.' % s)
    print('IMS irrigator group %s was successfully added to the model.\n\n\n' % s)

print('Processing completed successfully.')

WEAP.SaveArea()
