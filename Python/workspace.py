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
from numpy import NaN

os.chdir(r'D:\Active\Projects\Rakaia\git\Rakaia_v1\Python')

from groundwater.stream_depletion import Theis
from other_functions.reproject import reproject

config = configparser.RawConfigParser()
config.read('config_Rakaia.cfg')
config.read('config_Rangitata.cfg')

get_CRC_DB(config)  

get_CRC_DB_listinput(config)

get_CRC_CSV(config)

add_latlon_coordinates(config)

cleanup_crc_df(config)

'Connection7day', 'Connection30day', 'Connection150day'


export_dir = r'D:\Active\Projects\Rakaia\model\data\consents'
crc_csv = 'waimak_sd.csv'
file_path = os.path.join(export_dir, crc_csv)
xx.to_csv(file_path, index=False)

# config.crc_dir= r'C:\Users\hamishg.CH\OneDrive - Environment Canterbury\Documents\_Projects\git\WEAP_model_build\Python\config.cfg'
