# -*- coding: utf-8 -*-

import win32com.client
import ConfigParser
import datetime as dt
import os, shutil, sys
import pandas as pd

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='August 2020'
############################################################################################

'''
This is the main script that builds the WEAP model from scratch, and uses the config.cfg for its settings.
'''

class WEAP():
    
    def __init__(self):
        #-read config file with user settings
        self.config = ConfigParser.RawConfigParser()
        self.config.read('config.cfg')
        
        #-read some directories
        self.workDir = self.config.get('DIRS', 'workDir')
        self.tempDir = self.config.get('DIRS', 'tempDir')
        
        #-date settings
        syear = self.config.getint('TIMINGS', 'syear')
        smonth = self.config.getint('TIMINGS', 'smonth')
        sday = self.config.getint('TIMINGS', 'sday')
        self.sdate = dt.date(syear, smonth, sday) #-start date for the reference scenario
        eyear = self.config.getint('TIMINGS', 'eyear')
        emonth = self.config.getint('TIMINGS', 'emonth')
        eday = self.config.getint('TIMINGS', 'eday')
        self.edate = dt.date(eyear, emonth, eday) #-end date for the reference scenario
        
        #-Connect to the WEAP API
        self.WEAP=win32com.client.Dispatch('WEAP.WEAPApplication')
        while not self.WEAP.ProgramStarted:
            pass
            #i=1
        #-Default time-step is daily
        self.deltaT = 'D'
        self.WEAP.AutoCalc = True
        
        #-initialize the model
        self.initArea()
        #-set Key Assumptions Branch as variable
        self.keyAssumpBranch = self.WEAP.Branch('\Key Assumptions')
        #-set Other Assumptions Branch as variable
        self.otherAssumpBranch = self.WEAP.Branch('\Other Assumptions')
        
        #-initialize the catchments
        self.initCatchments()
        #-initialize the groundwater nodes
        self.initGroundwater()
        #-pre-process time-series
        self.preprocessTimeSeries()
        #-add low flow sites and corresponding bands
        self.processLowFlows()
        #-add consents
        self.processConsents()
        
        
        
        #-Run the model
        self.run()
        

    
    def initArea(self):
        '''
        Copy the base area to a new area to work with and make that one the active area
        '''
        #-check if a new model should be built
        self.newModel = self.config.getint('AREA', 'newModel')
        if self.newModel:
            print 'Initializing model...'
            
            self.curAccountDate = dt.date(self.sdate.year-1, self.sdate.month, self.sdate.day) #-start date for the current accounts year
            
            #-Area settings
            self.baseArea = self.config.get('AREA', 'baseArea')
            self.workArea = self.config.get('AREA', 'workArea')
            self.WEAP.ActiveArea = self.baseArea
            print 'Using "%s" as base for new model "%s"' %(self.baseArea, self.workArea)
            if self.WEAP.Areas.Exists(self.workArea):
                print '"%s" already exists and will be overwritten' %(self.workArea)
                shutil.rmtree(os.path.join(self.WEAP.AreasDirectory, self.workArea))
            self.WEAP.SaveAreaAs(self.workArea)
            self.WEAP.ActiveArea = self.workArea
            #-When setting up new area make sure the current accounts is the active scenario
            self.WEAP.ActiveScenario = 'Current Accounts'
            self.WEAP.BaseYear = self.curAccountDate.year
            self.WEAP.EndYear = self.edate.year
            #-set the first time step if day is not the first of january, or month is not january
            if self.deltaT == 'D':
                if (self.curAccountDate >= dt.date(self.curAccountDate.year,3,1)) & self.WEAP.IncludeLeapDays: 
                    firstTimeStep = (self.curAccountDate - dt.date(self.WEAP.BaseYear,1,1)).days + 2
                else:
                    firstTimeStep = (self.curAccountDate - dt.date(self.WEAP.BaseYear,1,1)).days + 1
                print 'Model simulation period is %s through %s with %s as current accounts year' %(self.sdate.strftime('%d-%m-%Y'), self.edate.strftime('%d-%m-%Y'), self.curAccountDate.year)
            else:
                firstTimeStep = self.curAccountDate.month
                print 'Model simulation period is %s through %s with %s as current accounts year' %(self.sdate.strftime('%B %Y'), self.edate.strftime('%B %Y'), self.curAccountDate.year)
            if self.WEAP.WaterYearStart != firstTimeStep:
                self.WEAP.WaterYearStart = firstTimeStep
            print 'Each simulation year has %d time steps' %self.WEAP.NumTimeSteps
            self.WEAP.SaveArea()
        else:
            self.workArea = self.config.get('AREA', 'workArea')
            self.WEAP.ActiveArea = self.workArea
            self.WEAP.ActiveScenario = 'Current Accounts'
        
    def initCatchments(self):
        '''
        Set-up the catchments in WEAP if processCatchments is True. This option is useful if the user
        does not want to add all catchments manually. It also allows for postprocessing SPHY model output into
        a csv-format time-series that can be added to the catchments.
        '''
        
        #-check if catchments should be processed
        self.processCatchments = self.config.getint('CATCHMENTS', 'processCatchments')
        if self.processCatchments:
            from catchment.init_catchments import initCatchmentTable
            self.catchmentDir, self.catch_df = initCatchmentTable(self.config)
            #-check if SPHY model simulations should be postprocessed
            #print self.catch_df
            self.postprocessSPHY = self.config.getint('CATCHMENTS', 'postprocessSPHY')
            if self.postprocessSPHY:
                from catchment.init_catchments import postprocessSPHY
                postprocessSPHY(self.config, self.catchmentDir, self.catch_df, self.sdate, self.edate)
            #-check if riverbed catchments should be added automatically
            self.addRiverbedCatchments = self.config.getint('CATCHMENTS', 'addRiverbedCatchments')
            if self.addRiverbedCatchments:
                from catchment.init_catchments import addRiverbedCatchments
                addRiverbedCatchments(self.WEAP, self.catch_df)
            #-set the riverbed catchment properties; i.e. adding precipitation time-series, etc.
            self.setRiverbedCatchmentProps = self.config.getint('CATCHMENTS', 'setRiverbedCatchmentProps')
            if self.setRiverbedCatchmentProps:
                from catchment.init_catchments import setRiverbedCatchmentProps
                setRiverbedCatchmentProps(self.WEAP, self.catch_df, self.catchmentDir)
                
    def initGroundwater(self):
        '''
        Set-up groundwater nodes in WEAP if processGW is True. This option is useful if the user
        does not want to add all groundwater nodes manually.
        '''
        
        #-check if groundwater nodes should be processed
        self.processGW = self.config.getint('GROUNDWATER', 'processGW')
        if self.processGW:
            from groundwater.init_groundwater import initGWTable
            self.gw_dir, self.gw_df = initGWTable(self.config)
            #-check if groundwater nodes should be added automatically
            self.add_GWnodes = self.config.getint('GROUNDWATER', 'add_GWnodes')
            if self.add_GWnodes:
                from groundwater.init_groundwater import addGWnodes
                addGWnodes(self.WEAP, self.gw_df)
            #-set the groundwater node properties; i.e. storage capacity, initial storage etc.
            self.set_GWprops = self.config.getint('GROUNDWATER', 'set_GWprops')
            if self.set_GWprops:
                from groundwater.init_groundwater import setGWprops
                setGWprops(self.WEAP, self.gw_df, self.config)
                
    def preprocessTimeSeries(self):
        '''
        Pre-process various time-series to be read into WEAP. These are written to csv-files.
        '''
        
        #-check whether time-series should be pre-processed
        self.preprocessTS = self.config.getint('TSPROCESSING', 'preprocessTS')
        if self.preprocessTS:
            from other_functions.get_TS import getTimeSeries
            get_TS = getTimeSeries(self.config)
            #-check if observed streamflow should be processed
            self.processQobs = self.config.getint('TSPROCESSING', 'processQobs')
            if self.processQobs:
                get_TS.writeQobs()
                
                
    def processLowFlows(self):
        '''
        Add low flows bands for each low flow site of interest to the Key Assumptions under the Key "Low Flows"
        '''
        #-check whether low flows should be added to model
        self.processLF = self.config.getint('LOWFLOWS', 'add_LF')
        if self.processLF:
            from lowflows.lowflows import getBandInfo, addIRFsite, addBands
            #-get low flow band information
            self.bands = getBandInfo(self.config)
            #print self.bands
            #-add the Irrigation Restriction Flow (IRF) site(s) and time-series to the model
            addIRFsite(self.WEAP, self.config, self.bands, self.sdate, self.edate)
            #-for each low flow site, add the bands with associated max_trig, min_trig, and Ballocated (=calculated based on max_trig and min_trig and IRF) 
            addBands(self.WEAP, self.config, self.bands)
            
            
    def processConsents(self):
        '''
        Add all the consents that were active during the simulation period. 
        '''
        
        #-Check whether consents section should be processed
        self.processCRC = self.config.getint('CONSENTS', 'process_crc')
        if self.processCRC:
            #-get directory where consent data for model will be stored
            self.crc_dir = self.config.get('CONSENTS', 'crc_dir')
            #-check if consent data should be extracted from database
            self.get_crc_db = self.config.getint('CONSENTS', 'get_crc_db')
            if self.get_crc_db:
                from consents.consents import get_CRC_DB
                self.crc_df, lMessageList = get_CRC_DB(self.config)
            #-otherwise read data from csv
            else:
                from consents.consents import get_CRC_CSV
                #crc_df = get_CRC_CSV(self.config)
                self.crc_df = get_CRC_CSV(self)
            
            #-check if groundwater waps should be filtered
            self.filter_gw_waps = self.config.getint('CONSENTS', 'filter_gw_waps')
            if self.filter_gw_waps:
                from consents.consents import filter_gw_sd_waps
                filter_gw_sd_waps(self)
                
            #-check if surface water take waps should be filtered
            self.filter_sw_waps = self.config.getint('CONSENTS', 'filter_sw_waps')
            if self.filter_sw_waps:
                from consents.consents import filter_sw_waps
                filter_sw_waps(self)
                
            #-check if divert waps should be filtered
            self.filter_divert_waps = self.config.getint('CONSENTS', 'filter_divert_waps')
            if self.filter_divert_waps:
                from consents.consents import filter_divert_waps
                filter_divert_waps(self)
                
            #-check if discharge consents should be filtered
            self.filter_discharge_crc = self.config.getint('CONSENTS', 'filter_discharge_crc')
            if self.filter_discharge_crc:
                from consents.consents import filter_discharge_consents
                filter_discharge_consents(self)
            
            #-add lat lon coordinates to dataframe and a field with wap names that weap can understand
            from consents.consents import add_latlon_coordinates, add_WAPs_as_nodes, removeWAPs
            self.crc_df = add_latlon_coordinates(self.crc_df)
            
            #-clean-up dataframe with waps and consents
            from consents.consents import cleanup_crc_df
            cleanup_crc_df(self)
            
            #-Write time-series with concent being active (yes/no) and another time-series for the consent/wap combination for each day 
            from consents.consents import crc_wap_active_ts
            crc_wap_active_ts(self)
            
            #-if user wants to remove WAPs for certain activity, then do so
            self.remove_SW_WAPs = self.config.getint('CONSENTS', 'remove_SW_WAPs')
            if self.remove_SW_WAPs:
                removeWAPs(self.WEAP, 'Take Surface Water')
            self.remove_GW_WAPs = self.config.getint('CONSENTS', 'remove_GW_WAPs')
            if self.remove_GW_WAPs:
                removeWAPs(self.WEAP, 'Take Groundwater')                
            
            #-check if SW take WAPs should be added as demand node
            self.add_SW_WAPs = self.config.getint('CONSENTS', 'add_SW_WAPs')
            if self.add_SW_WAPs:
                add_WAPs_as_nodes(self.WEAP, self.crc_df, 'Take Surface Water')
            #-check if GW take WAPs should be added as demand node
            self.add_GW_WAPs = self.config.getint('CONSENTS', 'add_GW_WAPs')
            if self.add_GW_WAPs:
                add_WAPs_as_nodes(self.WEAP, self.crc_df, 'Take Groundwater')
                
            #-Set consumption on nodes
            self.add_consumption = self.config.getint('CONSENTS','add_consumption')
            if self.add_consumption:
                from consents.consents import set_consumption
                f = set_consumption(self)
                if f is False:
                    'Error: setting of consumption failed!'
                    sys.exit()
 
            #-Check if blanco WAP Key Assumption framework should be set-up
            self.create_blanco_WAPs_KA = self.config.getint('CONSENTS', 'create_blanco_WAPs_KA')
            if self.create_blanco_WAPs_KA:
                from consents.consents import init_WAP_KeyAssumptions
                init_WAP_KeyAssumptions(self)
            #-Check if Consents Other Assumption framework should be initialized
            self.init_consents_OA = self.config.getint('CONSENTS', 'init_consents_OA')
            if self.init_consents_OA:
                from consents.consents import init_Consents_OtherAssumptions
                init_Consents_OtherAssumptions(self)

        else:
            try:
                print('Trying to reading consents details from csv-file into a dataframe...')
                self.crc_dir = self.config.get('CONSENTS', 'crc_dir')
                csv_file = os.path.join(self.crc_dir, self.config.get('CONSENTS', 'crc_csv_out_final'))
                self.crc_df = pd.read_csv(os.path.join(self.crc_dir, csv_file), parse_dates=[1,2,3,39,40], dayfirst=True)
                print('Consents details successfully read into a pandas dataframe.')
            except:
                print('Consents details could not be read. Check if "crc_csv_out_final" is specified in the config file.')                

        #-Check whether the sections above have been completed by the user before processing the code below
        self.process_crc_part2 = self.config.getint('CONSENTS_PART_2', 'process_crc_part2')
        if self.process_crc_part2:
            #-Add supply delivered expression to key assumptions
            self.add_supply_delivered = self.config.getint('CONSENTS_PART_2', 'add_supply_delivered')
            if self.add_supply_delivered:
                from consents.consents import add_supply_delivered
                add_supply_delivered(self)
            #-Calculate restriction daily volume for each consents/wap combi
            self.calc_restrict_volume = self.config.getint('CONSENTS_PART_2', 'calc_restrict_volume')
            if self.calc_restrict_volume:
                from consents.consents import calc_restrict_daily_vol
                calc_restrict_daily_vol(self)
            #-Add Ballocated from Key Assumptions to the Ballocated under 'Other Assumptions\Consents\crc-xx\wap-yy\Ballocated
            self.add_consent_bands = self.config.getint('CONSENTS_PART_2', 'add_consent_bands')
            if self.add_consent_bands:
                from consents.consents import add_bands_to_crc
                add_bands_to_crc(self)
                
            ######-Now set demand on WAPs-#####################################
            self.add_demand = self.config.getint('CONSENTS_PART_2', 'add_demand')
            if self.add_demand:
                demand = self.config.get('CONSENTS_PART_2', 'demand')
                if demand == 'max_conditions':
                    #-set daily restriction volumes as demand on demand nodes
                    from consents.consents import set_demand_to_restrict_daily_vol
                    print 'Using daily restrictions for demand'
                    set_demand_to_restrict_daily_vol(self)
                elif '.csv' in demand:
                    print 'Using time-series from csv-file for demand'
                    from consents.consents import set_demand_to_csv_ts
                    set_demand_to_csv_ts(self)
                elif demand == '0':
                    print 'Setting demand to zero for all nodes'
                    from consents.consents import set_demand_to_zero
                    set_demand_to_zero(self)
                else:
                    print 'Nothing specified for demand. This means no water is taken from the streams or groundwater nodes!'
            else:
                print 'Demand settings remain unchanged.'
                
            #-Set restrictions on transmission links (to SW take or GW take)
            self.set_restrictions_transmission = self.config.getint('CONSENTS_PART_2', 'set_restrictions_transmission')
            if self.set_restrictions_transmission:
                from consents.consents import set_restriction_on_transmission_links
                print 'Setting daily restriction volumes on transmission links to groundwater and surface water take nodes'
                set_restriction_on_transmission_links(self)
            #-Set restrictions on diverts
            self.set_restrictions_divert = self.config.getint('CONSENTS_PART_2', 'set_restrictions_divert')
            if self.set_restrictions_divert:
                from consents.consents import set_restriction_on_diverts
                print 'Setting daily restriction volumes on diverts'
                set_restriction_on_diverts(self)
            #-Remove restrictions on transmission links (to SW take or GW take)
            self.remove_restrictions_transmission = self.config.getint('CONSENTS_PART_2', 'remove_restrictions_transmission')
            if self.remove_restrictions_transmission:
                from consents.consents import remove_restriction_from_transmission_links
                remove_restriction_from_transmission_links(self)               
            #-Remove restrictions from diverts
            self.remove_restrictions_divert = self.config.getint('CONSENTS_PART_2', 'remove_restrictions_divert')
            if self.remove_restrictions_divert:
                from consents.consents import remove_restriction_from_diverts
                remove_restriction_from_diverts(self)

    def run(self):
        run_model = self.config.getint('RUNNING', 'run_model')
        if run_model:
            from simulate.runModel import runModel
            runModel(self)

    def getBasicInfo(self):
        '''
        Display some information about the models (areas)
        '''
        print 'WEAP working directory:\n\t%s\n' %self.WEAP.WorkingDirectory
        print 'WEAP areas directory:\n\t%s\n' %self.WEAP.AreasDirectory

        print 'WEAP contains the following areas:'
        for i in self.WEAP.Areas:
            print '\t%s' %i.Name
        print '\nActive area:\n\t%s' %self.WEAP.ActiveArea.Name
        print '\nScenarios in active area:'
        for i in self.WEAP.Scenarios:
            print '\t%s' %i.Name
        print '\nActive scenario:\n\t%s' %self.WEAP.ActiveScenario

    def getAreaInfo(self, area=False):
        '''
        Display some information about active area or the area provided as argument
        '''
        #-store the active area and scenario
        activeArea = self.WEAP.ActiveArea
        activeScenario = self.WEAP.ActiveScenario
        #-set another active area if that is provided as argument
        if area:
            self.WEAP.ActiveArea = area
    
        print 'Providing info for area:\n\t%s' %self.WEAP.ActiveArea.Name
        #-Display time settings for the current accounts
        print 'Time settings:'
        nrTimeSteps = self.WEAP.NumTimeSteps
        firstTimeStep = self.WEAP.FirstTimeStep
        lastTimeStep = firstTimeStep + nrTimeSteps - 1
        if nrTimeSteps == 12:
            deltaT = 'monthly'
            sdate = dt.date(self.WEAP.BaseYear, firstTimeStep, 1).strftime('%b %Y')
            if lastTimeStep>12:
                lastTimeStep = lastTimeStep-12
                eyear = self.WEAP.BaseYear + 1
            else:
                eyear = self.WEAP.BaseYear
            edate = dt.date(eyear, lastTimeStep, 1).strftime('%b %Y')
            print '\tCurrent accounts runs from %s through %s' %(sdate, edate)
        else:
            deltaT = 'daily'
            if nrTimeSteps == 366:
                leapDays = True
            else:
                leapDays = False
            print '\tLeap days is %s' %leapDays
            print '\tTime-step is %s' %deltaT
            if lastTimeStep - nrTimeSteps >0:
                lastTimeStep = lastTimeStep - nrTimeSteps
                eyear = self.WEAP.BaseYear + 1
            else:
                eyear = self.WEAP.BaseYear
            print '\tCurrent accounts runs from: %s %s through %s %s' %(self.WEAP.TimeStepName(firstTimeStep), self.WEAP.BaseYear, self.WEAP.TimeStepName(lastTimeStep), eyear)
        #-Display information for each scenario
        print 'Scenarios:'
        for s in self.WEAP.Scenarios:
            if s.Name != 'Current Accounts':
                self.WEAP.ActiveScenario = s
                print '\t%s' %s
                if deltaT == 'daily':
                    print '\t\tRuns from: %s %s through %s %s' %(self.WEAP.TimeStepName(firstTimeStep), s.FirstYear, self.WEAP.TimeStepName(lastTimeStep), s.LastYear)
                else:
                    sdate = dt.date(s.FirstYear, firstTimeStep, 1).strftime('%b %Y')
                    edate = dt.date(s.LastYear, lastTimeStep, 1).strftime('%b %Y')
                    print '\t\tRuns from: %s through %s' %(sdate, edate)
            
        #-reset to original active area and scenario
        self.WEAP.ActiveArea = activeArea
        self.WEAP.ActiveScenario = activeScenario
        
        
####-Main programme execution
w = WEAP()        