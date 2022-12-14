#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for nomenclature checking etc.

2022-12-14 sjs

CHANGELOG:
    2022-12-14: created


CONTAINS:
    kew_query():
        queries IPNI for the ipni number, and POWO for status of a name, including potential synonymy and updated nomenclature.
        potentially it can query a distribution, if this is wished.

'''

import pykew.powo as powo
import pykew.ipni as ipni
from pykew.powo_terms import Name, Filters
import pandas as pd


def powo_query(gen, sp, distribution=False):
    ''' This function takes genus species and crosschecks it to POWO. If the name is
    accepted, it is copied into the output, if it is a synonym, the accepted name is
    copied into the output. In the end the accepted names are returned.

    INPUT: 'genus'{string}, 'specificEpithet'{string} and 'distribution'{bool}
    OUTPUT: 'accepted_species', string of Genus species.
            'status' the status of the inputted name
            'ipni_no' the IPNI number assigned to the input name
         if distribution=True:
             'native_to' POWO range information
    '''
    #print('Checking uptodate-ness of nomenclature in your dataset...')
    query = {Name.genus: gen, Name.species: sp}
    res = powo.search(query , filters = [Filters.species])
    print('Checking the taxon', gen, sp)
    #print(res.size()) # for debugging
    try:
        for r in res:
            if 'name' in r:
                r['name']

        print('Input taxon accepted:', r['accepted'])

        if r['accepted'] == False:
            status = 'SYNONYM'
            acc_taxon = r['synonymOf']
            qID = acc_taxon['fqId']
            ipni_no = r['url'].split(':', )[-1]
            print('Accepted taxon name:', acc_taxon['name']) #TODO: add taxon author
            scientificName = acc_taxon['name']
            if distribution:
                res2 = powo.lookup(qID, include=['distribution'])
                try:
                    native_to = [d['name'] for d in res2['distribution']['natives']]
                except:
                    status = 'EXTINCT'
                    native_to = [d['name'] for d in res2['distribution']['extinct']]

        else:
            status = 'ACCEPTED'
            qID = r['fqId']
            ipni_no = r['url'].split(':', )[-1]
            scientificName = gen + ' ' + sp
            if distribution:
                res2 = powo.lookup(qID, include=['distribution'])
                #print(res2)
                try:
                    native_to = [d['name'] for d in res2['distribution']['natives']]
                except:
                    status = 'EXTINCT'
                    native_to = [d['name'] for d in res2['distribution']['extinct']]
        ipni_no = 'https://ipni.org/n/' + ipni_no

    except:
        # there are issues when the function is presented the string 'sp.' or 'indet.' etc
        print('The species', gen, sp, 'is not registered in POWO...\n',
              ' I don\'t know what to do with this now, so I will put the status on NA and the accepted species as NA.')
        status = pd.NA
        scientificName = pd.NA
        native_to = pd.NA
        ipni_no = pd.NA

    print(status)
    print(scientificName)

    res = ipni.search(query)  # , filters = [Filters.accepted])
    try:
        for r in res:
            if 'name' in r:
                r['name']
        ipni_pubYr = r['publicationYear']
    except:
        ipni_pubYr = pd.NA
        print('IPNI publication year NOT found.')



    return status, scientificName, ipni_no, ipni_pubYr, native_to


def kew_query(occs, working_directory):
    


#
