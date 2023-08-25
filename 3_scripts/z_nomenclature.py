#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for nomenclature checking etc.

2022-12-14 sjs

CHANGELOG:
    2022-12-14: created


CONTAINS:
    powo_query():
        despite the misleading function name, this function queries both POWO 

    kew_query():
        queries IPNI for the ipni number, and POWO for status of a name, including potential synonymy and updated nomenclature.
        potentially it can query a distribution, if this is wished.

'''

import pykew.powo as powo
import pykew.ipni as ipni
from pykew.powo_terms import Filters as powo_filter
from pykew.ipni_terms import Filters as ipni_filter
from pykew.powo_terms import Name as powo_name
from pykew.ipni_terms import Name as ipni_name

import pandas as pd
import logging
import swifter


def powo_query(gen, sp, distribution=False, verbose=True, debugging=False):
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


    if pd.notna(gen) and pd.notna(sp):
        # annoying error when no det associated with a record
        query = {ipni_name.genus: gen, ipni_name.species: sp}
        res = powo.search(query, filters=powo_filter.species) #, filters=Filters.specific)  # , filters = [Filters.accepted])
        logging.info(f'Checking the taxon {gen} {sp}')
            # print('checking distribution', distribution)
        #print(res.size()) # for debugging
        try:
            for r in res:
                if 'name' in r:
                    r['name']
                #logging.info(f'Input taxon accepted: {r.accepted}')

            if r['accepted'] == False:
                status = 'SYNONYM'
                acc_taxon = r['synonymOf']
                qID = acc_taxon['fqId']
                ipni_no = r['url'].split(':', )[-1]
                logging.debug(f'Accepted taxon name: {acc_taxon.name}') 
                scientificname = acc_taxon['name']
                species_author = acc_taxon['author']
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
                scientificname = gen + ' ' + sp
                species_author = r['author']
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
            logging.debug(f'The species {gen} {sp} is not registered in POWO...\n I don\'t know what to do with this now, so I will put the status on NA and the accepted species as NA.')
            status = 'not found in POWO'
            scientificname = pd.NA
            species_author = pd.NA
            native_to = pd.NA
            ipni_no = pd.NA

        logging.info(f'STATUS: {status}')
        logging.debug(f'{scientificname} {species_author}')
            #print(native_to)
        #query = {ipni_name.genus: gen, ipni_name.species: sp}
        query = gen + ' ' + sp
        #print(query)
        res = ipni.search(query, filters = ipni_filter.specific) # so we don't get a mess with infraspecific names
        #res = ipni.search(query, filters=Filters.species)  # , filters = [Filters.accepted])
        try:
            for r in res:
             
                if 'name' in r:
                    r['name']
            ipni_pubYr = r['publicationYear']
            #print(ipni_pubYr)
            #logging.debug('IPNI publication year found.')
        except:
            ipni_pubYr = pd.NA
    else:
        status = 'not det'
        scientificname = pd.NA
        species_author = pd.NA
        ipni_no = pd.NA
        ipni_pubYr = pd.NA
        native_to = pd.NA
        

    if distribution:
        print(native_to)
        return status, scientificname, species_author, ipni_no, ipni_pubYr, native_to
    else:
        return status, scientificname, species_author, ipni_no, ipni_pubYr 



def kew_query(occs, working_directory, verbose=True, debugging=False):
    ''' This function wraps the function above to query all the interesting stuff from Kew.
    '''

    #occs_na
    occs.specific_epithet = occs.specific_epithet.replace('nan', pd.NA) 
    occs['sp_idx'] = occs['genus']+ ' ' + occs['specific_epithet']
    occs.set_index(occs.sp_idx, inplace = True)
    occs_toquery = occs[['genus', 'specific_epithet']].astype(str).copy()
    occs_toquery[['genus', 'specific_epithet']] = occs_toquery[['genus', 'specific_epithet']].replace('nan', pd.NA)
    #occs_toquery[['genus', 'specific_epithet']] = occs_toquery[['genus', 'specific_epithet']].replace('None', pd.NA)
    occs_toquery['sp_idx'] = occs_toquery['genus']+ ' ' + occs_toquery['specific_epithet']
    logging.debug('The is the index and length of taxa column (contains duplicated taxon names; should be same length as input dataframe)')
    logging.debug(f'{occs_toquery.sp_idx}')
    logging.debug(f'{len(occs_toquery.sp_idx)}')
    occs_toquery.set_index(occs_toquery.sp_idx, inplace = True)

    occs_toquery = occs_toquery.dropna(how='all', subset=['genus', 'specific_epithet']) # these are really bad for the query ;-)
    # drop duplicated genus-species combinations (callable by index in final dataframe)
    occs_toquery = occs_toquery.drop_duplicates(subset = 'sp_idx', keep = 'last')
    logging.info(f'Number of unique taxa to check: {len(occs_toquery.sp_idx)}')
    # SWIFTER VERSION
    occs_toquery[['status','accepted_name', 'ipni_species_author', 'ipni_no', 'ipni_pub']] = occs_toquery.swifter.apply(lambda row: powo_query(row['genus'], 
                                                                                                                            row['specific_epithet'],
                                                                                                                         distribution=False, verbose=True),
                                                                                                                              axis = 1, result_type='expand')

    # # NON-SWIFTER VERSION
    # occs_toquery[['status','accepted_name', 'ipni_species_author', 'ipni_no', 'ipni_pub']] = occs_toquery.apply(lambda row: powo_query(row['genus'], 
    #                                                                                                                         row['specific_epithet'],
    #                                                                                                                      distribution=False, verbose=True),
    #                                                                                                                           axis = 1, result_type='expand')
   


    occs_toquery = occs_toquery.drop(['ipni_pub'], axis=1)
    occs_toquery = occs_toquery.set_index('sp_idx')
    occs_toquery = occs_toquery.drop(['genus', 'specific_epithet'], axis = 1)
    

    occs_out = occs.join(occs_toquery)
  ###----> finish implementing this. broken now! 
  # ask just unique species and then reinsert based on index... WHICH NEEDS TO ALSO GO INTO ORIGINAL OCCS!!!

    logging.debug(f'{occs_out.genus}{occs_out.specific_epithet}{occs_out.accepted_name}')
    # occs[['status','accepted_name', 'species_author', 'ipni_no', 'ipni_pub']] = occs.apply(lambda row: powo_query(row['genus'], row['specific_epithet'], distribution=False, verbose=True), axis = 1, result_type='expand')
    # # now drop some of the columns we really do not need here...
    # print(occs)
    # occs = occs.drop(['ipni_pub'], axis=1)


    # Keep all records together here. We filter for this later....
    
    #indet_occs = occs_out[occs_out['status'].isna()]
    #occs_out = occs_out[occs_out['status'].notna()]

    # some stats
    logging.info(f'{len(occs_out[occs_out.status.notna()])} records had an ACCEPTED name in the end.')
    logging.info(f'{len(occs_out[occs_out.status.isna()])} records had an ISSUE in their name and could not be assigned any name name.')
    logging.info('These are saved to a separate output, please check these, and either rerun them or look for duplicates with a determination.')
    

    return occs_out #, indet_occs



# for debugging:
# debug_occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/2_data_out/G_Phil_cleaned.csv', sep=';')
# print(debug_occs.columns)
# debug_occs = debug_occs.drop(['status', 'accepted_name', 'species_author', 'ipni_no'], axis = 1)
# tmp1 = kew_query(debug_occs, 'Users/serafin/Sync/1_Annonaceae/share_DB_WIP/', verbose = True)
# print('TMP!:', tmp1)


#
