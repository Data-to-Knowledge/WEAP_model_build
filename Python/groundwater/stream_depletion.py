# -*- coding: utf-8 -*-

from scipy.special import erfc
from math import sqrt
import numpy as np

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ ='August 2020'
############################################################################################


def sdf(L,S,T):
    '''
    Calculates Stream Depletion Factor given:
        L - Shortest distance from well to stream (m)
        S - Storage coefficient (-)
        T - Transmissivity (m2/d)
    Returns:
        y - stream depletion factor
    '''
    y = (L**2)*S/T
    return y


def Theis(T, S, L, q, d):
    '''
    Calculates the stream depletion rate in l/s given a constant pumping rate during x days. Single values as input.
    Returns:
        sdf         - Stream Depletion Factor
        connection  - connection: %
        SD          - Stream Depletion Rate (L3/T) - Unit of SD depends on input unit of average pumping rate; e.g. input l/s = output in l/s, input in m3/d = output in m3/d 
    Input:
        T - Transmissivity (m2/d)
        S - Storage coefficient (-)
        L - Separation distance (m)
        q - Average pumping rate (L3/T)
        d - Days of pumping with the average rate
    '''

    SDF = sdf(L,S,T)
    connection = erfc(sqrt(SDF/(4*d)))
    SD = connection * q 
    return SDF, connection, SD

def SD_interactive(L, S, T, qpump, t):
    '''
    Calculates the stream depletion for time-step t, given an array with time-series of pumping up to time-step t. This function was specifically
    developed to be used in WEAP interactive simulation mode.
    Input:
        T - Transmissivity (m2/d)
        S - Storage coefficient (-)
        L - Separation distance (m)
        qpump - NumPy 1D array of pumping rates with the array length being equal to the requested time-step t
    Returns:
        sd - stream depletion for time-step t in same unit as qpump
    
    '''
    #qpump = np.nan_to_num(qpump)
    qpump[np.isnan(qpump)] = 0. 
    SDF = sdf(L,S,T)
    dQ = np.zeros(t)
    for i in range(0, t):
        if i == 0:
            dQ[i] = qpump[i]
        else:
            dQ[i] = qpump[i] - qpump[i-1]
    ix = 0
    sd = 0
    for j in range(1, t+1):
        y = erfc(sqrt(SDF/(4*(t-j+1))))
        sd+= y * dQ[ix]
        ix+=1
    return sd

#-SD for on/off pumping at various rates
def SD(L, S, T, Qpump):
    '''
    Calculates stream depletion effect given:
        L - Shortest distance from well to stream (m)
        S - Storage coefficient (-)
        T - Transmissivity (m2/d)
        Qpump - NumPy 1D array with dynamic pumping rates (L/d)
    Returns:
        sd_matrix - NumPy 1D array with stream depletion rate (L/d) based on dynamic pumping rate Qpump - unit is same as provided in the pumping rate: e.g. if pumping rate is in liters per second, then so is the stream depletion rate
    '''
    #Qpump = np.nan_to_num(Qpump)  #-make sure NaNs are replaced by zeros
    Qpump[np.isnan(Qpump)] = 0.
    #-Calculate Stream depletion factor
    SDF = sdf(L,S,T)
    t = np.arange(1,len(Qpump)+1,1)
    ###-Calculate SD pumping going on and off and variable pumping rates
    dQ = np.zeros(len(t))
    sd_matrix = np.zeros([len(t), len(t)])
    for i in t:
        ix = np.argwhere(t==i)[0][0]
        if ix==0:
            dQ[ix] = Qpump[ix]
        else:
            dQ[ix] = Qpump[ix] - Qpump[ix-1]
        for j in t:
            if j>=i:
                jx = np.argwhere(t==j)[0][0]
                y = erfc(sqrt(SDF/(4*(j-i+1))))
                y = y * dQ[ix]
                sd_matrix[ix,jx] = y
    #-super position the individual curves
    sd_matrix = np.sum(sd_matrix, axis=0)
    
    return sd_matrix

