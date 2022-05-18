# -*- coding: utf-8 -*-

'''
Repojects a coordinate from one EPSG to another EPSG
'''

from osgeo import ogr
from osgeo import osr

#-Authorship information-###################################################################
__copyright__ = 'Environment Canterbury'
__version__ = '1.0'
__date__ ='May 2022'
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
    #point.AddPoint(float(x), float(y))  # check why this has changed, because it used to be the x argument first and then the y argument
    point.AddPoint(float(y), float(x))
    point.Transform(transform)

    return point.GetY(), point.GetX()

