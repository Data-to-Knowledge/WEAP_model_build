# -*- coding: utf-8 -*-

'''
Repojects a coordinate from one EPSG to another EPSG
'''

from osgeo import ogr
from osgeo import osr

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='August 2020'
############################################################################################



def reproject(src_epsg, dst_epsg, x, y):
    '''
    Reproject coordinates x and y from EPSG defined by src_epsg to EPSG defined by dst_epsg
    Returns the coordinates x and y in the dst_epsg projection
    '''
    source = osr.SpatialReference()
    source.ImportFromEPSG(src_epsg)
    
    target = osr.SpatialReference()
    target.ImportFromEPSG(dst_epsg)
    
    transform = osr.CoordinateTransformation(source, target)
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(x,y)
    point.Transform(transform)
    return point.GetX(), point.GetY()