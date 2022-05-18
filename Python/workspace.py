import configparser
from ctypes import create_unicode_buffer
from distutils.command.config import config
import os
import datetime as dt
from typing_extensions import Self
import pandas as pd
import numpy as np
import pdsql, os, sys
import csv


os.chdir(r'C:\Users\hamishg.CH\OneDrive - Environment Canterbury\Documents\_Projects\git\WEAP_model_build\Python')

from groundwater.stream_depletion import Theis
from other_functions.reproject import reproject

config = configparser.RawConfigParser()
config.read('config.cfg')


get_CRC_DB(config)  

get_CRC_CSV(config)

add_latlon_coordinates(config)

cleanup_crc_df(config)



# config.crc_dir= r'C:\Users\hamishg.CH\OneDrive - Environment Canterbury\Documents\_Projects\git\WEAP_model_build\Python\config.cfg'



   
    


