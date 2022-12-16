#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for duplicate detection and tratment

2022-12-13 sjs

CHANGELOG:
    2022-12-13: created


CONTAINS:
    duplicate_stats():
        does some stats on duplicates found with different combinations
    duplicate_cleaner():
        actually goes in and removes, merges and cleans duplicates
'''


import pandas as pd
import numpy as np
import codecs
import os
import regex as re

import z_dependencies







def duplicate_stats(occs, verbose=True, debugging=False):
    '''
    Function that prints a load of stats about potential duplicates
    '''
    #-------------------------------------------------------------------------------
    # these columns are used to identify duplicates (i.e. if a value of both these column is shared
    # for a row, then we flag the records as duplicates)
    dup_cols = ['coll_surname', 'colNum', 'colYear'] # the columns by which duplicates are identified
    #-------------------------------------------------------------------------------


    # data types can be annoying
    occs = occs.astype(z_dependencies.final_col_type) # double checking
    print(occs.dtypes)
    occs.replace('nan', pd.NA, inplace=True)


    #-------------------------------------------------------------------------------
    # MISSING collector information and number
    # remove empty collector and coll num
    occs1 = occs.dropna(how='all', subset = ['recordedBy', 'colNum_full'])
    print('Deleted', len(occs.index) - len(occs1.index),
     'rows with no collector and no number; \n',
     len(occs1.index), 'records left')
    print(occs1)

    #-------------------------------------------------------------------------------
    # MISSING col num
    # these are removed, and added later after processing(?)
    subset_col = ['colNum_full']

    occs_colNum = occs1.dropna(how='all', subset=subset_col)
    occs_nocolNum = occs1[occs1['colNum_full'].isna()]

    occs_colNum.ddlong.astype(float)
    occs_nocolNum['ddlong'].astype(float)

    occs_colNum['coll_surname'] = occs_colNum['recordedBy'].str.split(',', expand=True)[0]
    print(occs_colNum['coll_surname'])

    #-------------------------------------------------------------------------------
    # Perform some nice stats, just to get an idea what we have

    occs_colNum.colNum_full.astype(str)
    print('Total records:', len(occs),';\n Records with colNum_full:', len(occs_colNum),';\n Records with no colNum_full:', len(occs_nocolNum))

    print('\n \n Some stats about potential duplicates: \n .................................................\n')
    print('\n By Collector-name and FULL collector number', occs_colNum[occs_colNum.duplicated(subset=['recordedBy', 'colNum_full'], keep=False)].shape)
    print('\n By NON-STANDARD Collector-name and FULL collector number', occs_colNum[occs_colNum.duplicated(subset=['ORIG_recBy', 'colNum_full'], keep=False)].shape)
    print('\n By Collector-name and FULL collector number, and coordinates', occs_colNum.duplicated(['recordedBy', 'colNum_full', 'ddlat', 'ddlong'], keep=False).sum())
    print('\n By Collector-name and FULL collector number, genus and specific epithet', occs_colNum.duplicated(['recordedBy', 'colNum_full', 'genus' , 'specificEpithet'], keep=False).sum())
    print('\n By FULL collector number, genus and specific epithet', occs_colNum.duplicated([ 'colNum_full', 'genus' , 'specificEpithet'], keep=False).sum())
    print('\n By FULL collector number and genus', occs_colNum.duplicated([ 'colNum_full', 'genus' ], keep=False).sum())
    print('\n By FULL collector number, collection Year and country', occs_colNum.duplicated([ 'colNum_full', 'colYear' , 'country'], keep=False).sum())
    print('\n By collection Year and FULL collection number (checking for directionality)', occs_colNum.duplicated([ 'colYear' , 'colNum_full'], keep=False).sum())
    print('\n By REDUCED collection number and collection Yeear', occs_colNum.duplicated([ 'colNum', 'colYear' ], keep=False).sum())
    print('\n By locality, REDUCED collection number and collection Year', occs_colNum.duplicated([ 'locality', 'colNum', 'colYear' ], keep=False).sum())
    print('\n By SURNAME and COLLECTION NUMBER', occs_colNum.duplicated([ 'coll_surname', 'colNum' ], keep=False).sum())
    print('\n By SURNAME and FULL COLLECTION NUMBER', occs_colNum.duplicated([ 'coll_surname', 'colNum_full' ], keep=False).sum())
    print('\n By SURNAME and COLLECTION NUMBER and YEAR', occs_colNum.duplicated([ 'coll_surname', 'colNum', 'colYear' ], keep=False).sum())
    print('\n ................................................. \n ')



    # this function only does stats, no return call...




def duplicate_cleaner(occs, working_directory, prefix, verbose=True, debugging=False):
    '''
    This one actually goes and cleans/merges duplicates.
    '''

    occs = occs.astype(z_dependencies.final_col_type) # double checking
    print(occs.dtypes)
    occs.replace('nan', pd.NA, inplace=True)


    dup_cols = ['colNum_full', 'colYear'] # the columns by which duplicates are identified

    #-------------------------------------------------------------------------------
    # MISSING collector information and number
    # remove empty collector and coll num
    occs1 = occs.dropna(how='all', subset = ['recordedBy', 'colNum_full'])


    subset_col = ['colNum_full']

    occs_colNum = occs1.dropna(how='all', subset=subset_col)
    occs_nocolNum = occs1[occs1['colNum_full'].isna()]

    occs_colNum.ddlong.astype(float)
    occs_nocolNum['ddlong'].astype(float)

    occs_colNum['coll_surname'] = occs_colNum['recordedBy'].str.split(',', expand=True)[0]
    #print(occs_colNum['coll_surname'])

    #-------------------------------------------------------------------------------
    occs_dup_col =  occs_colNum.loc[occs_colNum.duplicated(subset=dup_cols, keep=False)]
    #print(occs_dup_col)
    # get the NON-duplicated records
    occs_unique = occs_colNum.drop_duplicates(subset=dup_cols, keep=False)

    if verbose:
        print('\n First filtering. \n Total records: ',
        len(occs_colNum), ';\n records with no duplicates (occs_unique): ',
        len(occs_unique), ';\n records with duplicates (occs_dup_col): ',
        len(occs_dup_col))

    #-------------------------------------------------------------------------------
    # Duplicates part 1:
    #---------------------------------
    ## A  DIFFERENT COORDINATES
    # Coordinates not identical:
        # small difference --> MEAN
        # big difference --> exctract and either discard or check manually

    # double-check type of coordinate columns
    convert_dict = {'ddlat': float,
                    'ddlong': float}
    occs_dup_col = occs_dup_col.astype(convert_dict)

    if verbose:
        print('\n The duplicates subset, before cleaning dups has the shape: ', occs_dup_col.shape)
    # in this aggregation step we calculate the variance between the duplicates.
    test = occs_dup_col.groupby(dup_cols, as_index = False).agg(
        ddlong = pd.NamedAgg(column = 'ddlong', aggfunc='var'),
        ddlat = pd.NamedAgg(column = 'ddlat', aggfunc='var'),
                              )

    # if this variance is above 0.1 degrees
    test.loc[test['ddlong'] >= 0.1, 'long bigger than 0.1'] = 'True'
    test.loc[test['ddlong'] < 0.1, 'long bigger than 0.1'] = 'False'

    test.loc[test['ddlat'] >= 0.1, 'lat bigger than 0.1'] = 'True'
    test.loc[test['ddlat'] < 0.1, 'lat bigger than 0.1'] = 'False'

    # filter by large variance.
    true = test[(test["long bigger than 0.1"] == 'True') | (test["lat bigger than 0.1"] == 'True')]

    # write these records to csv for correcting or discarding.
    true.to_csv(working_directory + 'TO_CHECK_'+prefix+'_coordinates_to_combine.csv', index = False, sep = ';')

    # remove the offending rows and spit them out for manual checking or just discarding
    coord_to_check = occs_dup_col[occs_dup_col.index.isin(true.index)]
    occs_dup_col = occs_dup_col[~ occs_dup_col.index.isin(true.index)]
    coord_to_check.to_csv(working_directory + 'TO_CHECK_'+prefix+'coordinate_issues.csv', index = False, sep = ';')
    #print(occs_dup_col.shape)

    # print summary
    if verbose:
        print('\n Input records: ', len(occs_colNum),
        ';\n records with no duplicates (occs_unique): ', len(occs_unique),
        ';\n duplicate records with very disparate coordinates removed:', len(coord_to_check),
        '\n If you want to check them, I saved them to', working_directory + 'TO_CHECK_'+prefix+ 'coordinate_issues.csv')

    # for smaller differences, take the mean value...
    occs_dup_col['ddlat'] = occs_dup_col.groupby(['colYear', 'colNum_full'])['ddlat'].transform('mean')
    occs_dup_col['ddlong'] = occs_dup_col.groupby(['colYear', 'colNum_full'])['ddlong'].transform('mean')
    #print(occs_dup_col.shape)


    #---------------------------------
    ## B DIFFERENT IDENTIFICATIONS
    # Different identification between duplicates
    # update by newest det, or remove spp./indet or string 'Annonaceae'

    no_go = 'Annonaceae'

    # expert_list ??? it might be a nice idea to gather a list of somewhat recent identifiers
    # to  use them to force through dets? Does that make sense? We do update the
    # taxonomy at a later date, so dets to earlier concepts might not be a massive issue?

    dups_diff_species = occs_dup_col[occs_dup_col.duplicated(['colYear','colNum_full', 'country'],keep=False)&~occs_dup_col.duplicated(['recordedBy','colNum_full','specificEpithet','genus'],keep=False)]
    dups_diff_species = dups_diff_species.sort_values(['colYear','colNum_full'], ascending = (True, True))

    if verbose:
        print('\n We have', len(dups_diff_species),
        'duplicate specimens with diverging identification.')

    #-------------------------------------------------------------------------------
    # i think this just is nice for visual confirmation we didn't reverse an identification
    # by replacing it with 'sp.'
    # check which speciimens have no specificEpithet, or 'sp./indet.'
    # # occs_dup_col['sE-sp'] = occs_dup_col['specificEpithet']
    # occs_dup_col['sE-sp'] = occs_dup_col['sE-sp'].str.replace(r'^(.(?<!sp\.))*?$', '')
    # #[dups_diff_species['specificEpithet'] == 'sp.']
    # print('HA')
    # occs_dup_col['sE-indet'] = occs_dup_col['specificEpithet']
    # #.str.replace(r"^(.(?<!Grass))*?$", "Turf")
    # occs_dup_col['sE-indet'] = occs_dup_col['sE-indet'].str.replace(r'^(.(?<!indet.))*?$', '')
    # # thinking about it, no sense in seperating out empty values, as these will not merge back ...
    # print(occs_dup_col.columns)
    #
    # # combine the sp and indet cols
    #
    # occs_dup_col = occs_dup_col.assign(indets=occs_dup_col[['sE-indet', 'sE-sp']].sum(1)).drop(['sE-indet', 'sE-sp'], 1)
    #-------------------------------------------------------------------------------

    # backup the old dets
    occs_dup_col['genus_old'] = occs_dup_col['genus']
    occs_dup_col['specificEpithet_old'] = occs_dup_col['specificEpithet']

    # cols we to change
    cols = ['genus','specificEpithet','detBy', 'detYear']

    # https://stackoverflow.com/questions/59697994/what-does-transformfirst-do
    #groupby col and num, and sort more recent det
    occs_dup_col = occs_dup_col.groupby(dup_cols, group_keys=False, sort=True).apply(lambda x: x.sort_values('detYear', ascending=False))

    #groupby col and num, and transform the rest of the columns
    #we shall create a new column just to keep a trace

    occs_dup_col['genus'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['genus'].transform('first')
    occs_dup_col['specificEpithet'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['specificEpithet'].transform('first')

    #save a csv with all duplicates beside each other but otherwise cleaned, allegedly.
    occs_dup_col.to_csv(working_directory + 'TO_CHECK_' + prefix + 'dupli_dets_cln.csv', index = False, sep = ';')
    print('\n I have saved a checkpoint file of all cleaned and processed duplicates, nicely beside each other, to:',
    working_directory + 'TO_CHECK_' + prefix + 'dupli_dets_cln.csv')

    #-------------------------------------------------------------------------------
    # not sure how to integrate revisions just yet... this might be something
    # for when integrating new data into the established database, when we already
    # have revision datasets present and we can compare these...
    #-------------------------------------------------------------------------------

    #-------------------------------------------------------------------------------
    # DE-DUPLICATE AND THEN MERGE

    # check type (again)
    occs_dup_col = occs_dup_col.astype(z_dependencies.final_col_type)
    #print(occs_dup_col.dtypes)

    occs_merged = occs_dup_col.groupby(dup_cols, as_index = False).agg(
        scientificName = pd.NamedAgg(column = 'scientificName', aggfunc = 'first'),
    	genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
    	specificEpithet = pd.NamedAgg(column = 'specificEpithet', aggfunc = 'first' ),
    	speciesAuthor = pd.NamedAgg(column = 'speciesAuthor', aggfunc = 'first' ),
    	collectorID = pd.NamedAgg(column = 'collectorID', aggfunc = 'first' ),
        recordedBy = pd.NamedAgg(column = 'recordedBy', aggfunc = 'first' ),
    	prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
    	colNum = pd.NamedAgg(column = 'colNum', aggfunc = 'first' ),
    	sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
        colDate = pd.NamedAgg(column = 'colDate', aggfunc = 'first' ),
        colDay = pd.NamedAgg(column = 'colDay', aggfunc = 'first' ),
        colMonth = pd.NamedAgg(column = 'colMonth', aggfunc = 'first' ),
        colYear = pd.NamedAgg(column = 'colYear', aggfunc = 'first' ),
    	detBy = pd.NamedAgg(column = 'detBy', aggfunc = lambda x: ', '.join(x) ),
    	detByDate = pd.NamedAgg(column = 'detByDate', aggfunc = 'first' ),
        detDay = pd.NamedAgg(column = 'detDay', aggfunc = 'first' ),
        detMonth = pd.NamedAgg(column = 'detMonth', aggfunc = 'first' ),
        detYear = pd.NamedAgg(column = 'detYear', aggfunc = 'first' ),
    	countryID = pd.NamedAgg(column = 'countryID', aggfunc = 'first' ),
    	country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
    	continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
    	locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
    	coordinateID = pd.NamedAgg(column = 'coordinateID', aggfunc = 'first' ),
    	ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
        ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
    	institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
        herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
        barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)))
    # here quite some data might get lost, so we need to check where we want to just join first,
    # and where we add all values, and then decide on the columns we really want in the final
    # database!!!

    # de-duplicated duplicates sorting to get them ready for merging
    occs_merged = occs_merged.sort_values(dup_cols, ascending = (True, True))

    print('\n There were', len(occs_dup_col), 'duplicated specimens')
    print('\n There are', len(occs_merged), 'unique records after merging.')

    print('\n \n FINAL DUPLICATE STATS:')
    print('-------------------------------------------------------------------------')
    print('\n Input data:', len(dat), '; \n De-duplicated duplicates:', len(occs_merged),
    '; \n Non-duplicate data:', len(occs_unique),
     '; \n No collection number. (This is not included in output for now!) :', len(occs_nocolNum),
     ';\n total data written:', len(occs_merged) + len(occs_unique) ,
     '; \n Datapoints removed: ',
    len(dat) - (len(occs_nocolNum) + len(occs_merged) + len(occs_unique)))
    print('-------------------------------------------------------------------------')


    occs_cleaned = pd.merge(pd.merge(occs_merged,occs_unique,how='outer'),occs_nocolNum ,how='outer')
    occs_cleaned = occs_cleaned.sort_values(dup_cols, ascending = (True, True))
    #print(occs_cleaned.head(50))

    occs_cleaned.to_csv(working_directory+prefix+'deduplicated.csv', index = False, sep = ';', )
    print('\n The output was saved to', working_directory+prefix+'deduplicated.csv', '\n')

    return occs_cleaned








#
