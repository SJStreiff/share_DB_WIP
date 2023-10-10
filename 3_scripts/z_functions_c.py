#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for namechecking the Harvard University Herbarium collectors database

2023-01-10 sjs

CHANGELOG:
    2023-01-10: created
    2023-01-12: it works. BUT it doesn't give what i want. I do not get access to the full name of the collector or ID I need to access more deta
    2023-01-19: HUH still nothing. But crossfill working

CONTAINS:
    harvard_reference():
      queries the HUH database of collectors for the correct name format.
    country_crossfill():
      crossfils between the two country identifier columns (i.e. country_id (ISO 2 letter abbreviation), and country (full name))
'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import requests
import logging
import swifter
import country_converter as coco
import reverse_geocoder as rg

cc = coco.CountryConverter()


#custom dependencies
import z_dependencies # can be replaced at some point, but later...



def country_crossfill(occs, verbose=True):
    """
    Take records and crossfill the country_id and country name columns
    """
    occs.reset_index(drop=True)
    #logging.info(f'Let\'s see if this works {occs.country_id}')
    try:
        occs.country = occs.country.replace('0', pd.NA)
    except:
        a=1
    try:
        occs.country_id = occs.country_id.replace('0', pd.NA)
    except:
        a=1
    try:
        occs['country_id'] = occs.country_id.fillna(cc.pandas_convert(series = occs.country, to='ISO2'))
    except:
        occs['country_id'] = cc.pandas_convert(series=occs.country, to='ISO2')
    occs['country'] = occs.country.fillna(cc.pandas_convert(series = occs.country_id, to='name_short'))
    occs['country_iso3'] = cc.pandas_convert(series = occs.country, to='ISO3') # needed for later in coordinate cleaner
    logging.info('Countries all filled: {occs.country}')

    return occs
###---- country-crossfill



def get_cc(ddlat, ddlong):
    """ 
    do the actual extraction of the country from coords
    """
    # make coordinate query
    coords = (ddlat, ddlong)
    #mode=2 only works for multiple queries together. not suitable for the structure here
    try:
        res = pd.DataFrame(rg.search(coords, mode=1)) 
        # print(type(res))
        # print(res.cc)
        country = res['cc']
    except:
        country = pd.NA


    return country
 ###--- get-cc(iso2 country code)   

def cc_missing(occs, verbose=True):
    """
    Big problem is data with no useable country information. needs solving
    use reverse-geocoder to fill in these cases
    """
    
    occs = occs.reset_index(drop=True)
    occs_nf = occs[occs['country_iso3'] == 'not found']
    if len(occs_nf) > 0:
        # if all countries good we need not bother...
        occs_good = occs[occs['country_iso3'] != 'not found']
        occs_nf['country_id'] = occs_nf.swifter.apply(lambda row: get_cc(row['ddlat'], row['ddlong']), axis = 1, result_type = 'expand')
        occs_nf['country'] = cc.pandas_convert(series = occs_nf.country_id, to='name_short')
        occs_nf['country_iso3'] = cc.pandas_convert(series = occs_nf.country, to='ISO3')
    


        occs_out = pd.concat([occs_nf, occs_good])

        return occs_out
    else:
        # return unmodified data
        return occs
###--- countries missing




# debugging and updating an olde version of db

# cc_debug = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/20230824_mdb_for_upload.csv', sep=';')

# # #cc_debug = cc_debug.head(n=1)
# # print(cc_debug)

# newres = cc_missing(cc_debug)
# print(newres.country_iso3)

# newres.to_csv('/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/C_20230824_mdb_for_upload.csv', sep=';', index=False)




##########
# DEPRECATED: SEE HUH_query.py
# def get_HUH_names(recordedBy, verbose=True):
#     """ Query the HUH database on botanists/collectors names.
#     In: Collector name (in clean format?)
#     Out: I don't know yet
#     """
#     logging.info('HUH name checker \n DEBUGGING!! \n .........................\n')
#     logging.info(f'Checking the botanist {recordedBy}')


#     # split recorded by into Firstnames and Surnames
#     lastname, firstnames = recordedBy.split(',')
#     # key_mid = r'([A-Z]*)', ''
#     mid_insert = re.sub(r'([A-Z])', '', firstnames).strip()
#     # key_first = {: r''}
#     firstnames = re.sub(r'([a-z]{0,3})', '', firstnames)
#     logging.info(f'{firstnames} {lastname} MID= {mid_insert}')


#     # add points and plus into firstnames string
#     if len(firstnames) > 0:
#         s = firstnames[0]
#         firstnames=firstnames.strip()
#         for i in range(4):
#             try:
#                 s = s + firstnames[i] + '.' + '+'
#                 s.replace('.+.', '.')
#             except:
#                 f='Idontknow'


#         Firstname_query = s.strip()
#     logging.info(f'{Firstname_query}')

#     # create name=<string> for insertion into url for query.
#     lastname=lastname.strip()
#     if mid_insert == '':
#         name_string = Firstname_query+lastname
#     else:
#         name_string = Firstname_query+mid_insert+'+'+lastname
#     logging.info(f'{name_string}')
#     name_string=name_string.strip() # just to make sure no leadin/trailing whitespace
#     # do query

#     url = "https://kiki.huh.harvard.edu/databases/botanist_search.php?name="+name_string+"&individual=on&json=y"
#     logging.info('The URL is: {url}')
#     response = requests.get(url)
#     logging.info(f"The response is: {response.text}")




#     return 'Working now.'
# #
# test = get_HUH_names(recordedBy)
# print(test)
#    except:
