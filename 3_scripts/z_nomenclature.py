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


def powo_query(gen, sp, distribution=False, verbose=True):
    ''' This function takes genus species and crosschecks it to POWO. If the name is
    accepted, it is copied into the output, if it is a synonym, the accepted name is
    copied into the output. In the end the accepted names are returned.

    INPUT: 'genus'{string}, 'specific_epithet'{string} and 'distribution'{bool}
    OUTPUT: 'accepted_species', string of Genus species.
            'status' the status of the inputted name
            'ipni_no' the IPNI number assigned to the input name
         # if distribution=True: this has been disabled here.....
             'native_to' POWO range information
    '''
    #print('Checking uptodate-ness of nomenclature in your dataset...')
    query = {Name.genus: gen, Name.species: sp}
    res = powo.search(query, filters=Filters.species)  # , filters = [Filters.accepted])
    if verbose:
        print('Checking the taxon', gen, sp)
        # print('checking distribution', distribution)
    #print(res.size()) # for debugging
    try:
        for r in res:
            if 'name' in r:
                r['name']

        if verbose:
            print('Input taxon accepted:', r['accepted'])

        if r['accepted'] == False:
            status = 'SYNONYM'
            acc_taxon = r['synonymOf']
            qID = acc_taxon['fqId']
            ipni_no = r['url'].split(':', )[-1]
            if verbose:
                print('Accepted taxon name:', acc_taxon['name']) #TODO: add taxon author
            scientificname = acc_taxon['name']
            species_author = acc_taxon['author']
            # if distribution:
            #     res2 = powo.lookup(qID, include=['distribution'])
            #     try:
            #         native_to = [d['name'] for d in res2['distribution']['natives']]
            #     except:
            #         status = 'EXTINCT'
            #         native_to = [d['name'] for d in res2['distribution']['extinct']]
            # else:
            #     native_to = pd.NA

        else:
            status = 'ACCEPTED'
            qID = r['fqId']
            ipni_no = r['url'].split(':', )[-1]
            scientificname = gen + ' ' + sp
            species_author = r['author']
            # if distribution:
            #     res2 = powo.lookup(qID, include=['distribution'])
            #     #print(res2)
            #     try:
            #         native_to = [d['name'] for d in res2['distribution']['natives']]
            #     except:
            #         status = 'EXTINCT'
            #         native_to = [d['name'] for d in res2['distribution']['extinct']]
        ipni_no = 'https://ipni.org/n/' + ipni_no

    except:
        # there are issues when the function is presented the string 'sp.' or 'indet.' etc
        if verbose:
            print('The species', gen, sp, 'is not registered in POWO...\n',
              ' I don\'t know what to do with this now, so I will put the status on NA and the accepted species as NA.')
        status = pd.NA
        scientificname = pd.NA
        species_author = pd.NA
        # native_to = pd.NA
        ipni_no = pd.NA

    if verbose:
        print(status)
        print(scientificname, species_author)
        #print(native_to)

    res = ipni.search(query)  # , filters = [Filters.accepted])
    try:
        for r in res:
            if 'name' in r:
                r['name']
        ipni_pubYr = r['publicationYear']
        if verbose:
            print('IPNI publication year found.')
    except:
        ipni_pubYr = pd.NA


    return status, scientificname, species_author, ipni_no, ipni_pubYr#, native_to



def kew_query(occs, working_directory, verbose=True):
    ''' This function wraps the function above to query all the interesting stuff from Kew.
    Note I have verbose=False here, as this function does a load of output, which is not strictly necessary.
    '''
    
    occs[['genus', 'specific_epithet']] = occs[['genus', 'specific_epithet']].astype(str)
    occs[['genus', 'specific_epithet']] = occs[['genus', 'specific_epithet']].replace('nan', pd.NA)
    occs = occs.dropna(how='all', subset=['genus', 'specific_epithet']) # these are really bad for the query ;-)
    print(occs[['genus', 'specific_epithet']])
    occs[['status','accepted_name', 'species_author', 'ipni_no', 'ipni_pub']] = occs.apply(lambda row: powo_query(row['genus'], row['specific_epithet'], distribution=False, verbose=True), axis = 1, result_type='expand')
    # now drop some of the columns we really do not need here...
    print(occs)
    occs = occs.drop(['ipni_pub'], axis=1)


    if verbose:
        print('I started with', len(occs), 'records. \n')

    #occs.to_csv(out_dir + 'no_subset.csv', index = False, sep=';')

    issue_occs = occs[occs['status'].isna()]
    occs = occs[occs['status'].notna()]

    # some stats
    print(len(occs), 'records had an ACCEPTED name in the end. \n')
    print(len(issue_occs), 'records had an ISSUE in their name and could not be assigned any name name. \n',
    'These are saved to a separate output, please check these, and either rerun them or look for duplicates with a determination.')
    #occs.to_csv(out_dir + 'taxonomy_checked.csv', index = False, sep=';')
    issue_occs.to_csv(working_directory + 'TO_CHECK_unresolved_taxonomy.csv', index = False, sep = ';')


    return occs


#
