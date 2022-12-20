# coding=utf-8


import os, sys
import rasterio.plot

import datetime
import zipfile
import xarray as xr
import pandas as pd
import rioxarray
import numpy as np
##
from pathlib import Path
from glob import glob


"""
def paths_to_datetimeindex(paths):
    string_slice=(45,-5) #string_slice=(45,60)
    date_strings = [os.path.basename(i)[slice(*string_slice)]
                    for i in paths]
    return pd.to_datetime(date_strings)
"""
def paths_to_datetimeindex2(paths):
    string_slice=(7,-12) #string_slice=(45,60) #pendent cambiar
    date_strings = [os.path.basename(i)[slice(*string_slice)]
                    for i in paths]
    return pd.to_datetime(date_strings)


def selection_sort(x):
    for i in range(len(x)):
        swap = i + np.argmin(x[i:])
        (x[i], x[swap]) = (x[swap], x[i])
    return x

def unzip(zipped_filename):
    with zipfile.ZipFile(zipped_filename, 'r') as zip_ref:
        if not os.path.exists('unzipped'):
            os.makedirs('unzipped')
        zip_ref.extractall('./unzipped')

if not os.path.exists('unzipped'):
    os.makedirs('unzipped')


def queryS2(file):
    """Read an input product list returning the full-path product list suitable for reading datasets.

    Parameters:
        file (str): full-path of the input file listing target products

    Return: S5P L2 full-path to files (list)
    """
    with open(file,"r") as f:
        data = f.readlines()
        list = [d.split("\n")[0] for d in data]
    products = []
    for item in list:
        if item.endswith('.zip') or item.endswith('.SAFE') :
            products.append(item)
        else:
            for file in Path(item).rglob('*.zip') or Path(item).rglob('*.SAFE') :
                products.append(str(file))
    return products


def product_level(item):
    """Check for S2 product type. This information will change the relative path to images.

    Parameters:
        item (str): full path to S2 products location

    Return: exit status (bool)
    Raise ValueError for Unrecognized product types
    """
    if "MSIL2A" in item:
        return True
    elif "MSIL1C" in item:
        return False
    else:
        raise ValueError("%s: Unrecognized S2 product type"%item)



def bands(item,res='10m'):
    """Search for target MSIL2A bands given an input resolution. This is useful for index computations.

    Parameters:
        item (str): full path to S2 products location
        res (str): resolution of S2 images; default set to `10m`; allowed options: `10m`,`20m`,`60m`

    Return: bands sorted by increasing wavelength (list)
    """
    msi = product_level(item)
    products = []
    string = '*_'+str(res)+'.jp2'
    if msi: # L2A
        for path in Path(item).rglob(string):
            products.append(str(path))
    else: # L1C
        for path in Path(item).rglob('*.jp2'):
            products.append(str(path))
    return sorted(products) # ordered bands


def sclbands(item):
    """Search for target MSIL2A bands given an input resolution. This is useful for index computations.

    Parameters:
        item (str): full path to S2 products location
        res (str): resolution of S2 images; default set to `10m`; allowed options: `10m`,`20m`,`60m`

    Return: bands sorted by increasing wavelength (list)
    """
    msi = product_level(item)
    products = []
    #string = '*_'+str(res)+'.jp2'
    if msi: # L2A
        for path in Path(item).rglob('*_SCL_*'):
            products.append(str(path))
    #else: # L1C
        #for path in Path(item).rglob('*.jp2'):
            #products.append(str(path))
    return sorted(products) # ordered bands
