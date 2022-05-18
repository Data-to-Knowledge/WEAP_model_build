# -*- coding: utf-8 -*-

import pandas as pd
import calendar, os, time

import datetime as dt
from groundwater.stream_depletion import *

#-Authorship information-###################################################################
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__date__ ='May 2022'
############################################################################################

pd.options.display.max_columns = 100


def runModel(self):
    self.WEAP.Verbose = 0
    tic = time.clock()
    simDir = self.config.get('RUNNING', 'simDir')
    run_interactive = self.config.getint('RUNNING', 'run_interactive')
    calculate_SD = self.config.getint('RUNNING', 'calculate_SD')
    zero_SD = self.config.getint('RUNNING', 'zero_SD')
    if zero_SD:
        print('Setting zero demand for stream depletion nodes...')
#         self.WEAP.ActiveScenario = 'Reference'
        gw_waps = pd.unique(self.crc_df.loc[self.crc_df.Activity == 'Take Groundwater', 'wap_name']).tolist()
        for wap in gw_waps:
            print('Setting stream depletion for %s_SD to zero...' % wap)
            br = self.WEAP.Branch('\\Demand Sites and Catchments\\' + wap + '_SD')
            br.Variables('Daily Demand').Expression = 0
        self.WEAP.ActiveScenario = 'Reference'
        for wap in gw_waps:
            print('Setting stream depletion for %s_SD to zero...' % wap)
            br = self.WEAP.Branch('\\Demand Sites and Catchments\\' + wap + '_SD')
            br.Variables('Daily Demand').Expression = 0
        print('Running the model...')
        self.WEAP.Calculate()
        self.WEAP.SaveArea()
        print('Model run completed successfully.')
    else:
        if run_interactive and calculate_SD:
            # -Currently, interactive running is only useful for stream depletion calculations. Disadvantage of this method is that it is slow and stream depletion is running behind one time-step.
            # -If all takes are downstream of the lowflow site, then it is better not to run interactive mode, but run the model twice instead.
            # -sdate for current accounts
            sdate = dt.date(self.sdate.year - 1, self.sdate.month, self.sdate.day)
            # -get the groundwater take waps and create dataframe for period of interest and waps
            gw_waps = pd.unique(self.crc_df.loc[self.crc_df.Activity == 'Take Groundwater', 'wap_name']).tolist()
            # gw_waps = ['L36_1687_GW', 'L36_2005_GW', 'L36_1837_GW']  #-just four waps for testing script
            gw_supply_delivered_df = pd.DataFrame(index=pd.date_range(sdate, self.edate, freq='D'), columns=gw_waps)
            gw_supply_delivered_df.fillna(0, inplace=True)
            gw_supply_delivered_df.index.name = 'Date'
            gw_sd_df = gw_supply_delivered_df.copy()
            # -Run WEAP in interactive mode to calculate stream-depletion on the fly
            if not self.WEAP.IsCalculatingInteractively:
                self.WEAP.InitializeInteractiveCalculations()
                oldYear = sdate.year
                t = 0
                curdate = sdate
                for s in self.WEAP.Scenarios:  # -current accounts, and other scenarios (if defined). normally only current accounts and reference scenario
                    s.InitializeInteractiveCalculations()
                    flag = True
                    while flag:
                        flag = s.CalculateNextTimeStep()
                        if curdate.year != oldYear:
                            t = 1
                            oldYear = curdate.year
                        else:
                            t += 1
                        if not calendar.isleap(curdate.year) and t == 60:
                            t += 1
                        for wap in gw_waps:
                            br = self.WEAP.Branch('\\Demand Sites and Catchments\\' + wap)
                            qpump = self.WEAP.ResultValue(br.FullName + ':Supply Delivered[m^3]', self.WEAP.CalcYear, t, s)
                            gw_supply_delivered_df.loc[gw_supply_delivered_df.index == pd.Timestamp(curdate), wap] = qpump
                            # -get the parameters needed for the interactive stream depletion calculation
                            qpump = gw_supply_delivered_df.loc[gw_supply_delivered_df.index <= pd.Timestamp(curdate), wap].to_numpy()
                            timestep = len(qpump)
                            L = self.crc_df.loc[self.crc_df['wap_name'] == wap, 'Distance'].to_numpy()[0]
                            S = self.crc_df.loc[self.crc_df['wap_name'] == wap, 'S'].to_numpy()[0]
                            T = self.crc_df.loc[self.crc_df['wap_name'] == wap, 'T_Estimate'].to_numpy()[0]
                            # -interactive calculation of stream depletion
                            sd = SD_interactive(L, S, T, qpump, timestep)
                            gw_sd_df.loc[gw_sd_df.index == pd.Timestamp(curdate), wap] = sd
                            # -set the stream depletion as a demand to the stream depletion node --> there is a delay of 1 day because this demand (Stream depletion) will then be used in the next time-step
                            self.WEAP.Branch('\\Demand Sites and Catchments\\' + wap + '_SD').Variables('Daily Demand').Expression = sd
                        curdate = curdate + dt.timedelta(days=1)
                    s.FinalizeInteractiveCalculations()
                self.WEAP.FinalizeInteractiveCalculations()
                # -Write pumped volumes and associated stream depletion volumes to csv-files
                gw_supply_delivered_df.to_csv(os.path.join(simDir, self.config.get('RUNNING', 'pump_csv')))
                gw_sd_df.to_csv(os.path.join(simDir, self.config.get('RUNNING', 'sd_csv')))
        else:
            if calculate_SD:  # -if stream depletion calculations are required, the model is run twice.
                # -Run the mode for the first time
                print('First model iteration run...')
                self.WEAP.Calculate()
                self.WEAP.SaveArea()
                print('First model iteration run completed successfully.')
                sdate = dt.date(self.sdate.year - 1, self.sdate.month, self.sdate.day)
                oldYear = sdate.year
                t = 0
                curdate = sdate
                gw_waps = pd.unique(self.crc_df.loc[self.crc_df.Activity == 'Take Groundwater', 'wap_name']).tolist()
                # gw_waps = ['L36_1687_GW', 'L36_2005_GW', 'L36_1837_GW']  #-just four waps for testing script
                gw_supply_delivered_df = pd.DataFrame(index=pd.date_range(sdate, self.edate, freq='D'), columns=gw_waps)
                gw_supply_delivered_df.fillna(0, inplace=True)
                gw_supply_delivered_df.index.name = 'Date'
                # -get the supply delivered for each day and groundwater take wap
                print('Retrieving supplied pumped volume for each groundwater take wap...')
                while curdate <= self.edate:
                    print(curdate)
                    if curdate.year != oldYear:
                        t = 1
                        oldYear = curdate.year
                    else:
                        t += 1
                    if not calendar.isleap(curdate.year) and t == 60:
                        t += 1
                    for wap in gw_waps:
                        br = self.WEAP.Branch('\\Demand Sites and Catchments\\' + wap)
                        qpump = self.WEAP.ResultValue(br.FullName + ':Supply Delivered[m^3]', curdate.year, t)
                        gw_supply_delivered_df.loc[gw_supply_delivered_df.index == pd.Timestamp(curdate), wap] = qpump
                    curdate = curdate + dt.timedelta(days=1)
                gw_supply_delivered_df.index = gw_supply_delivered_df.index.strftime('%d/%m/%Y')
                gw_supply_delivered_df.to_csv(os.path.join(simDir, self.config.get('RUNNING', 'pump_csv')))

                gw_sd_df = gw_supply_delivered_df.copy() * 0.
                # -Calculate the stream depletion using the dataframe of supplied pumping rates
                waps = gw_sd_df.columns.tolist()
                # for wap in gw_supply_delivered_df.columns:
                for wap in waps:
                    qpump = gw_supply_delivered_df[wap].to_numpy()
                    print('Calculating stream depletion rate for %s...' % wap)
                    L = self.crc_df.loc[self.crc_df['wap_name'] == wap, 'Distance'].to_numpy()[0]
                    S = self.crc_df.loc[self.crc_df['wap_name'] == wap, 'S'].to_numpy()[0]
                    T = self.crc_df.loc[self.crc_df['wap_name'] == wap, 'T_Estimate'].to_numpy()[0]
                    sd = SD(L, S, T, qpump)
                    gw_sd_df[wap] = sd
                # -write dataframe with calculated stream depletion to csv file
                gw_sd_df.to_csv(os.path.join(simDir, self.config.get('RUNNING', 'sd_csv')))
                for wap in waps:
                    print('Adding stream depletion as demand to %s_SD...' % wap)
                    br = self.WEAP.Branch('\\Demand Sites and Catchments\\' + wap + '_SD')
                    br.Variables('Daily Demand').Expression = 'ReadFromFile(' + os.path.join(simDir, self.config.get('RUNNING', 'sd_csv')) + ', ' + str(waps.index(wap) + 1) + ', , , , Interpolate)'
                self.WEAP.SaveArea()
                # -Run the model for the second time
                print('Second and final model iteration run...')
                self.WEAP.Calculate()
                self.WEAP.SaveArea()
                print('Second and final model iteration run completed successfully.')
            else:
                print('Running the model...')
                self.WEAP.Calculate()
                self.WEAP.SaveArea()
                print('Model run completed successfully.')

    toc = time.clock()
    deltat = toc - tic
    self.WEAP.Verbose = 1
    print('Simulation took %.0f minute(s).' % (deltat / 60.))
