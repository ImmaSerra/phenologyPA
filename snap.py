# coding=utf-8

import os,sys
#import urllib
#from urllib import request

##
from pathlib import Path
from glob import glob

##
#import urllib.request
# connect to the API
import datetime
import zipfile
import xarray as xr
import pandas as pd
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date

import rioxarray
import json

#fileBbox = bboxgeo   #geojosn file closed linestring
#bboxgeo = 'bboxgeo.json'
#bboxgeo = sys.argv[1]

#fileTemporal = sys.argv[2]   #txt file


def main():

    ARG=json.load(open("vlabparams.json","r"))
    print('arg',str(ARG)) #print example :arg {'data1': '2019-01-01T00:00:00.000Z', 'data2': '2019-01-05T23:59:59.999Z', 'bbox': 'false'}

    list = []
    for item in os.listdir(os.getcwd()):
        #print(item)
        list.append(item)
    #print('list1',list[1])   #print name of file: bboxgeo.json

    fileBbox2 = read_geojson('bboxgeo.json')
    print('namefile', fileBbox2)  #namefile {"features": [{"geometry": {"coordinates": [[[2, 42], [2.15, 42], [2.15, 42.15], [2, 42.15], [2, 42]]], "type": "Polygon"}, "properties": {}, "type": "Feature"}], "type": "FeatureCollection"}
    #print(list.files(full.names=TRUE, recursive=TRUE))#
    #cat("\n\n## ------------------------------------------------------------------ ##\n\n")

    # Get command line values
    #args <- commandArgs(trailingOnly=TRUE)

    #https://www.tutorialspoint.com/python/python_command_line_arguments.htm

    """
    bboxgeo
    {'data1': '20210101', 'data2': '20211231', 'bbox': 'false'}
    -- data1 20210101 -- data2 20211231
    """

    print('arg',str(ARG)) #f
    arg=""
    for k,v in ARG.items():
        if (v is False)|(v in ["False","false","F"]):
            continue
        else:
            if (v is True)|(v=="true"):
                #v=""
                v="true"
            arg+=" -- "+" ".join([k,str(v)])
    #print('arg2',arg)  #print dates

    user=ARG['user']
    passw=ARG['passw']
    print(user)
    print(passw)
    dates =[ARG['data1'],ARG['data2']]
    print(dates[0])
    print(dates[1])
    print(ARG['bbox'])

    coord = ARG['bbox'].split(',')
    print(coord[0])

    env_coord = coord
    """
    def bbox():
        coord = ARG['bbox'].split(',')
        return coord

    env_coord = bbox()
    """

    #print(ARG['bbox'][0]) #Error 4 primer digit
    #print(ARG['bbox'][1]) #Error 3 segon digit

    #api = SentinelAPI('iserra', 'Creaf-21', 'https://scihub.copernicus.eu/dhus')
    api = SentinelAPI(user, passw, 'https://scihub.copernicus.eu/dhus')
    """
    with open(fileTemporal) as f:
    #with open('input/dates.txt') as f:
        contents = f.read()
        dates = contents.split(",")
        print(dates)
    """

    footprint = geojson_to_wkt(read_geojson('bboxgeo.json'))
    products = api.query(footprint,
                         date = (dates[0],dates[1]),
                         platformname = 'Sentinel-2',
                         processinglevel = 'Level-2A',
                         cloudcoverpercentage = (0, 80))  #80%

    print('files:',len(products))

    if not os.path.exists('output'):
        os.makedirs('output')

    output = "output.txt"
    outputfile = os.path.join('output', output)

    #with open(output, "w") as outputfile:
    f = open(outputfile, "w")
    for i in products:
        #print (i,api.get_product_odata(i)['title'],api.get_product_odata(i)['url'])
        #print (api.get_product_odata(i)['url'])
        print (api.get_product_odata(i)['title'])  #print filename
        #print (api.get_product_odata(i)['url'])
        #outputfile.write(str(api.get_product_odata(i)['title'])+'\n')
        f.write(str(api.get_product_odata(i)['title'])+'\n')
    f.close()

    # convert to Pandas DataFrame
    products_df = api.to_dataframe(products)
    #print(products_df)

    # sort and limit to first 1 sorted products
    #products_df_sorted = products_df.sort_values(['cloudcoverpercentage', 'ingestiondate'], ascending=[True, True])
    products_df_sorted = products_df.sort_values(['ingestiondate'], ascending=[True])
    #products_df_sorted = products_df_sorted.head(2)


    if not os.path.exists('temp'):
        os.makedirs('temp')

    # download sorted and reduced products
    #api.download_all(products_df_sorted.index)  #donwload products

    download_path = './temp'
    # download sorted and reduced products in a specific folder
    api.download_all(products_df_sorted.index,directory_path=download_path)

    #ds = products_df.to_xarray()  #es un xarrayDataset
    ds = products_df['title'].to_xarray()  #es un xarray.DataArray


    if not os.path.exists('unzipped'):
        os.makedirs('unzipped')


    download_unzipped_path = os.path.join(os.getcwd(), 'unzipped')
    #print(download_unzipped_path)

    extension = ".zip"
    for item in os.listdir(download_path): #canviar download_unzipped_path
        print(item)
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.join(download_path, item) # get full path of files
            zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            zip_ref.extractall(download_unzipped_path) # extract file to dir
            zip_ref.close() # close file

    files_unzip = os.listdir(download_unzipped_path)
    print(files_unzip)  #filename

    return


if __name__ == "__main__":
    main()
