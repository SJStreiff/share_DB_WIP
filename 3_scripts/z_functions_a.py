#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script
'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re

#custom dependencies
import z_dependencies as x_1_cols # can be replaced at some point, but later...






def col_standardiser(importfile, data_source_type, verbose=True):
    ''' reads a file, checks the columns and subsets and adds columns where
    necessary to be workable later down the line.'''


    imp = codecs.open(importfile,'r','utf-8') #open for reading with "universal" type set

    #-------------------------------------------------------------------------------
    # different sources have different columns, here we treat the most common source types,
    # rename the odd columns, and subset the wished ones
    # (dictionary and list of wanted columns in auxilary variable files, can be easily added and modified)

    if(data_source_type == 'P'):
        # for data in the format of herbonautes from P
        if verbose:
            print('\n','data type P')
        occs = pd.read_csv(imp, sep = ';',  dtype = str) # read the data
        occs = occs.rename(columns = x_1_cols.herbo_key) # rename columns
        occs = occs[x_1_cols.herbo_subset_cols] # subset just required columns
        if verbose:
 	          print('Just taking the Philippines for now!')
        occs = occs[occs['country'] == 'Philippines']

    elif(data_source_type == 'GBIF'):
        # for all data in the darwin core format!!
        if verbose:
 		         print('\n','data type GBIF')
        occs = pd.read_csv(imp, sep = '\t',  dtype = str) # read data
        occs = occs[occs['basisOfRecord'] == "PRESERVED_SPECIMEN"] # remove potential iNaturalist data....
        occs = occs[occs['occurrenceStatus'] == 'PRESENT'] # loose absence data from surveys
        # here we a column species-tobesplit, as there is no single species columns with only epithet
        occs = occs.rename(columns = x_1_cols.gbif_key) # rename
        occs = occs[x_1_cols.gbif_subset_cols] # and subset

    else:
        if verbose:
 		         print('\n','datatype not found')
        # maybe think if we want to somehow merge and conserve the plant description
        # for future interest (as in just one column 'plantdesc'???)
    if verbose:
 	      print('\n', 'The columns now are:', occs.columns)

    #-------------------------------------------------------------------------------
    # add all final columns as empty columns
    # check for missing columns, and then add these, as well as some specific trimming
    # splitting etc...
    miss_col = [i for i in x_1_cols.final_cols if i not in occs.columns]


    if verbose:
 	      print('\n','These columns are missing in the data from source: ', miss_col,
          '\n Empty columns will be added and can later be modified.')

    if verbose:
 	      print('These columns are missing in the data being handled: ', miss_col)
    occs[miss_col] = '0'
    occs = occs.astype(dtype = x_1_cols.final_col_type)
    #print(occs.dtypes)

    return occs


def column_cleaning(occs, data_source_type, out_dir, verbose=True):
    '''
    Cleaning up the columns to all be the same...
    '''
    print('Standardising column names.')
    if(data_source_type == 'P'):
        """things that need to be done in the herbonautes data:
            - coordinate split (is ['lat, long'])
            - date split coll
            - date split det
            - split ColNum into pre-, main and sufix!
        """
        # COORDINATES
        occs[['ddlat', 'ddlong']] = occs.coordinates.str.split(",", expand = True)
        occs['ddlat'] = occs['ddlat'].str.strip() # trim any trailing spaces
        occs['ddlong'] = occs['ddlong'].str.strip()
        occs.drop(['coordinates'], axis = 'columns', inplace=True)

        # DATES

        # herbonautes data has 2 dates, start and end. We have decided to just take the first.
        # sometimes these are identical, but sometimes these are ranges.




        occs[['colDate_1', 'colDate_2']] = occs.colDate.str.split("-", expand=True,)
        occs[['detDate_1', 'detDate_2']] = occs.detDate.str.split("-", expand=True,)
        #delete the coldate_2 colomn, doesn't have info we need
        occs.drop(['colDate_2'], axis='columns', inplace=True)
        occs.drop(['detDate_2'], axis='columns', inplace=True)

        #split colDate_1 on '/' into three new fields (dd/mm/yyyy). Let's just hope no american ever turns up in that data
        occs[['colDay', 'colMonth', 'colYear']] = occs.colDate_1.str.split("/", expand=True,)
        occs[['detDay', 'detMonth', 'detYear']] = occs.detDate_1.str.split("/", expand=True,)
        # cahnge datatype to pd Int64 (integer that can handle NAs)
        occs[['colDay', 'colMonth', 'colYear', 'detDay', 'detMonth', 'detYear']] = occs[['colDay', 'colMonth', 'colYear', 'detDay', 'detMonth', 'detYear']].astype(float).astype(pd.Int64Dtype())

        #split colDate_1 on '/' into three new fields
        occs[['colDay', 'colMonth', 'colYear']] = occs.colDate_1.str.split("/", expand=True,)
        occs[['detDay', 'detMonth', 'detYear']] = occs.detDate_1.str.split("/", expand=True,)
        occs[['colDay', 'colMonth', 'colYear', 'detDay', 'detMonth', 'detYear']].astype(float)
        #occs[['colDay', 'colMonth', 'colYear', 'detDay', 'detMonth', 'detYear']].astype(pd.Int64Dtype())



        # not sure which is best, but if T, then need to rename cols for standardisation
        # T: drop original col and det date comumns, but keep colDate_1 and DetDAte_1
        # S: keep the very original col and det date cols, remove the colDate_1...

        # for the time being I'm just doing it my way...
        occs.drop(['colDate_1'], axis='columns', inplace=True)
        occs.drop(['detDate_1'], axis='columns', inplace=True)

        if verbose:
            print(occs.colDate)
        if verbose:
            print('\n','The collection date has now been split into separate (int) columns for day, month and year')



        #occs.drop(['colDate_1'], axis='columns', inplace=True)
        #occs.drop(['detDate_1'], axis='columns', inplace=True)

        if verbose:
 		         print(occs.colDate)


        # COLLECTION NUMBERS

        # keep the original colNum column
        occs.rename(columns={'colNum': 'colNum_full'}, inplace=True)
        #create prefix, extract text before the number
        occs['prefix'] = occs['colNum_full'].astype(str).str.extract('^([a-zA-Z]*)')
        ##this code deletes spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()
        #print(occs.dtypes)

        #create sufix , extract text or pattern after the number
        #you can create different regex expressions here. The point of this code is to have the extract into a single and same colum
        #otherwise it creates different coloumns for each pattern.
        # see here: https://toltman.medium.com/matching-multiple-regex-patterns-in-pandas-121d6127dd47

        # going from most specific to most general regex, this list takes all together in the end
        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colNum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only digits without associated stuff, but including some characters (colNam)
        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\s\d+\s\d+)', # 00 00 00 or so
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colNum']  = occs['colNum_full'].astype(str).str.extract('(' + '|'.join(regex_list_digits) + ')')

        occs['colNum'] = occs['colNum'].str.strip()
        occs = occs.replace('nan', pd.NA) # remove NAs that aren't proper

        #print(occs.head(5))

    #-------------------------------------------------------------------------------
    if(data_source_type == 'GBIF'):
        """things that need to be done in the herbonautes data:
            - BARCODE (just number or Herbcode + number)
            THIS STILL NEEDS SOME FINE TUNING, FOR NOW IGNORING THIS ISSUE
            - specific epithet!
            - record number (includes collectors regularly...)
        """
    #==
    # NOT WORKING YET
            # check for different barcode formats:
            # either just numeric : herb missing
            #     --> merge herbarium and code
        if verbose:
            print('debugging barcode issues \n')
        if verbose:
            print(occs.barcode)

        #occs[['prel_herbCode', 'prel_BC']] = occs['barcode'].str.split('.', expand = True)
        # if the barcode is only digits, copy it into prel_code

    #TODO!!!!!!


        # if there is a nondigit, take it away from the digits, and modify it to contain only letters
        #occs['prel_BC'] = occs['barcode'].str.extract('(' + '|'.join([r'(\d+\/\d+)',r'(\d+)']) + ')')
        occs['prel_BC'] = occs['barcode'].str.extract(r'(\d+\/\d+)')
        # TODO find HERBXX-XXX-XXX-etc.
        occs['prel_BC2'] = occs['barcode'].str.extract(r'(\d+)')
        occs['prel_BC'] = occs['prel_BC'].fillna(occs['prel_BC2'])

        if verbose:
            print(occs.prel_BC)

        occs['prel_herbCode'] = occs['barcode'].str.extract(r'([A-Z]+)' or r'(\[A-Z]+\-)') # gets most issues...
        occs['prel_code'] = occs['barcode'].astype(str).str.extract(r'(\D+)')
        occs['prel_code_X'] = occs['barcode'].astype(str).str.extract(r'(\d+\.\d)') # this is just one entry and really f@#$R%g annoying

        if verbose:
            print(type(occs.prel_code))
        if verbose:
            print(occs.prel_herbCode)
        #occs['Institute'] = occs['Institute'].astype(float)
        #occs['Institute'] = occs['Institute'].replace('nan', 'NaN')
        #occs['prel_herbCode'].fillna(occs['institute'], inplace=True)
        occs['tmp_hc'] = occs['institute'].str.extract(r'(^[A-Z]+\Z)')
        occs['tmp_hc'] = occs['herbarium_code'].str.extract(r'(^[A-Z]+\Z)')
        occs['tmp_hc'] = occs['tmp_hc'].replace('PLANT', 'TAIF')
        #occs['tmp_hc'] = occs['tmp_hc'].str.replace('PLANT', '')
        occs.prel_herbCode.fillna(occs['tmp_hc'], inplace = True)
        #occs[occs['herbarium_code'] == r'([A-Z]+)', 'tmp_test'] = 'True'
        if verbose:
            print(occs.tmp_hc)
        # this now works, but,
            # we have an issue with very few institutions messing up the order in which stuff is supposed to be...
            # (TAIF)

        #occs.institute.fillna(occs.prel_herbCode, inplace=True)

        if verbose:
            print(occs.prel_herbCode)
        if verbose:
            print(occs.prel_BC)
        occs['st_barcode'] = occs['prel_herbCode'] + occs['prel_BC']
        if verbose:
            print(occs.st_barcode)
    #    prel_herbCode trumps all others,
    #        then comes herbarium_code, IF (!) it isn't a word (caps and non-caps) and if it is not (!!) 'PLANT'
    #           then comes institute


        if verbose:
            print(occs.columns)


        if occs.st_barcode.isna().sum() > 0:
            if verbose:
                print('I couldn\'t standardise the barcodes of some records.')
            na_bc = occs[occs['st_barcode'].isna()]
            na_bc.to_csv(out_dir + 'NA_barcodes.csv', index = False, sep = ';', mode = 'x')
            if verbose:
                print('I have saved', len(na_bc), 'occurences to the file', out_dir+'NA_barcodes.csv for corrections')
            if verbose:
                print('I am continuing without these.')
            occs = occs[occs['st_barcode'].notna()]
            if verbose:
                print(occs)


        occs = occs.drop(['prel_BC', 'prel_BC2', 'prel_herbCode', 'prel_code', 'prel_code_X', 'tmp_hc'], axis = 1)
        occs = occs.rename(columns = {'barcode': 'orig_BC'})
        occs = occs.rename(columns = {'st_barcode': 'barcode'})
        if verbose:
            print(occs.columns)
        # # now i have pure number barcodes: my herbarium code is either in 'Herbarium_Code'
        # or in 'prel_herbCode'

        # if there is no entry in the prel_* column, then combine institute and code column

    #==


        # COLLECTION NUMBERS
        # keep the original colNum column
        occs.rename(columns={'colNum': 'colNum_full'}, inplace=True)
        occs.colNum_full.replace('s.n.', pd.NA, inplace=True)
        #occs.colNum_full.replace("s. n.", pd.NA, inplace=True)

        #create prefix, extract text before the number
        occs['prefix'] = occs['colNum_full'].astype(str).str.extract('^([a-zA-Z]*)')
        ##this code deletes spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()

        #create sufix , extract text or pattern after the number
        #you can create different regex expressions here. The point of this code is to have the extract into a single and same colum
        #otherwise it creates different coloumns for each pattern.
        # see here: https://toltman.medium.com/matching-multiple-regex-patterns-in-pandas-121d6127dd47

        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colNum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only number (colNam)

        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colNum']  = occs['colNum_full'].astype(str).str.extract('(' + '|'.join(regex_list_digits) + ')')
        occs['colNum'] = occs['colNum'].str.strip()

        #print(occs)
        # ok this looks good for the moment.
        """ Still open here:
        remove collector name in prefix (partial match should take care of that)
        """

        # SPECIFIC EPITHET

        # darwin core specific...

        occs[['tmp', 'specificEpithet']] = occs['species-tobesplit'].str.split(' ', expand = True)
        #print(occs.tmp)


        occs[['tmp', 'specificEpithet']] = occs['species-tobesplit'].str.split(' ', expand = True)
        if verbose:
            print(occs.tmp)

        occs.drop(['tmp'], axis='columns', inplace=True)
        occs.drop(['species-tobesplit'], axis='columns', inplace=True)

        #occs['barcode'] = occs['prel_herbCode'].fillna('') + occs['prel_BC']

        #
        # else:
        #     print('nay')
        # \w.\d+ then remove punctuation and fill in herbarium code
            # split barcode into herbarium and barcodes




    #for name in occs.recordedBy:
    """ We could consider modifying the collector names to all be standard at least
     if initials are there and pre-names (de, van, ...) to have these all the same. Then
     we might be able to have prettier names. It also might facilitate merging collector names
     and later on searching for collectorID (ORCID or other IDs??)
    """

    occs = occs.astype(dtype = x_1_cols.final_col_type) # check data type
    #print(occs.dtypes)
    if verbose:
        print('\n','Data has been standardised to conform to the columns we need later on.') #\n It has been saved to: ', out_file)
    if verbose:
        print('\n','Shape of the final file is: ', occs.shape )

    if verbose:
        print(occs.columns)
    #occs = occs.replace('nan', '')
    #occs = occs.astype(dtype = x_1_cols.final_col_type)
    if verbose:
        print('Shape of the final file is: ', occs.shape )

    #occs.to_csv(out_file, sep = ';')

    print('\n','STEP 1 completed successfully.')

    return occs

def collector_names(occs, out_dir, verbose=True, debugging=False):
    pd.options.mode.chained_assignment = None  # default='warn'
    # this removes this warning. I am aware that we are overwriting stuff with this function.
    # the column in question is backed up





    #print(occs.dtypes) # if you want to double check types again
    occs.replace('nan', pd.NA, inplace=True)
    #print(occs.head)
    # -------------------------------------------------------------------------------
    #print('MINUS FIRST TRY \n', occs.info())

    occs['ORIG_recBy'] = occs['recordedBy'] # keep original column...
    # remove the introductory string before double point :
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.replace('Collector(s):', '', regex=False)
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.replace('&', ';', regex=False)
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.replace(' y ', ';', regex=False)
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.replace(' and ', ';', regex=False)
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.replace('Jr.', 'JUNIOR', regex=False)
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.replace('et al.', '', regex=False)
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.replace('et al', '', regex=False)
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.replace('etal', '', regex=False)
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.replace('Philippine Plant Inventory (PPI)', 'Philippines, Philippines Plant Inventory', regex=False)
    ##we will need to find a way of taking out all the recordedBy with (Dr) Someone's Collector

    ##isolate just the first collector (before a semicolon)
    occs['recordedBy'] = occs['recordedBy'].astype(str).str.split(';').str[0]

    occs['recordedBy'] = occs['recordedBy'].str.strip()

    if verbose:
        print('---------------- \n The changing of the weird exceptions still not completely done',
        '\n -------------------')

    # sep collectors by ';', multiple collectors separated like this
    # 'Collector(s):' is also something that pops up in GBIF....

    # now we need to account for many different options
    # 1 Meade, C.     --> ^[A-Z][a-z]*\s\[A-Z]   ^([A-Z][a-z]*)\,
    # 2 Meade, Chris  -->
    # 3 Meade C       -->
    # 4 C. Meade      -->
    # 5 Chris Meade   -->


    extr_list = {
        r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([A-Z])[a-z]+.*' : r'\1, \2 \3 \4',  # all full full names with sep = ' '
        r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+': r'\1, \2 \3',  # all full names: 2 given names
        r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]{2,20}': r'\1, \2',  # just SURNAME, Firstname
        r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W*\Z': r'\1, \2 \3 \4',  # Surname, F(.) M(.) M(.)
        r'(^[A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W\,.+': r'\1, \2 \3 \4',  # Surname, F(.) M(.) M(.), other collectors
        r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+\Z': r'\1, \2 \3',  # Surname, F(.) M(.)
        r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])': r'\1, \2 \3 \4',  # Surname FMM
        r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])': r'\1, \2 \3 \4',  # Surname FMM
        r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])\Z': r'\1, \2 \3',  # Surname FM
        r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])\Z': r'\1, \2 \3',  # Surname FM
        r'^([A-Z][a-z]+)\W+([A-Z])\Z': r'\1, \2',  # Surname F
        r'^([A-Z][a-z]+)\W*\Z': r'\1',  # Surname without anything
        r'^([A-Z][a-z]+)\,\W+([A-Z])\W+': r'\1, \2',  # Surname, F(.)
        r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\4, \1 \2 \3', # Firstname Mid Nid Surname ...
        r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\3, \1 \2', # Firstname Mid Surname ...
        r'^([A-Z])[a-z]+\s([A-Z])\W+([A-Z][a-z]+)': r'\3, \1 \2', # Firstname M. Surname ...
        r'^([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\2, \1', # Firstname Surname
        r'^([A-Z])\W+([A-Z][a-z]+)': r'\2, \1', # F. Surname ...
        r'^([A-Z])\W+([A-Z])\W+([A-Z][a-z]+)': r'\3, \1 \2', #F. M. Surname
        r'^([A-Z])\W+([A-Z][a-z]+)': r'\2, \1', #F Surname
        r'^([A-Z])([A-Z])\W+([A-Z][a-z]+)': r'\3, \1 \2', #FM Surname
        r'^([A-Z])([A-Z])([A-Z])\W+([A-Z][a-z]+)': r'\4, \1 \2 \3', #FMM Surname
    }
    # The row within the extr_list corresponds to the column in the debugging dataframe printed below


    names_WIP = occs[['recordedBy']] #.astype(str)

    #print(names_WIP)
    i = 0
    for key, value in extr_list.items():
        i = i+1
        # make a new panda series with the regex matches/replacements
        X1 = names_WIP.recordedBy.str.replace(key, value, regex = True)
        # replace any fields that matched with empty string, so following regex cannot match.
        names_WIP.loc[:,'recordedBy'] = names_WIP.loc[:,'recordedBy'].str.replace(key, '', regex = True)
        # make new columns for every iteration
        names_WIP.loc[:,i] = X1 #.copy()

    # debugging dataframe: every column corresponds to a regex query
    if debugging:
        names_WIP.to_csv('1a_n_regex_debug.csv', index = False, sep =';', mode = 'x')
        print('debugging dataframe printed to ./1a_n_regex_debug.csv!')

    #####
    # Now i need to just merge the columns from the right and bobs-your-uncle we have beautiful collector names...
    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')

    #-------------------------------------------------------------------------------
    # For all names that didn't match anything:
    # extract and then check manually
    TC_occs = occs.copy()
    TC_occs['to_check'] = names_WIP['recordedBy']
    #print(TC_occs.to_check)
    TC_occs.dropna(subset= ['to_check'], inplace = True)
    #print(TC_occs.to_check)

    # output so I can go through and check manually
    if len(TC_occs)!= 0:
        TC_occs.to_csv(out_dir + 'to_check_names.csv', index = False, sep = ';', mode = 'x')
        print(len(TC_occs), ' records couldn\'t be matched to the known formats.',
        '\n Please double check these in the separate file saved at: \n', out_dir+'to_check_names.csv')
    #-------------------------------------------------------------------------------


    # mask all values that didn't match for whatever reason in the dataframe (results in NaN)
    names_WIP = names_WIP.mask(names_WIP.recordedBy.notna())

    # now merge all columns into one
    while(len(names_WIP.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
        i = i+1
        names_WIP.iloc[:,-1] = names_WIP.iloc[:,-1].fillna(names_WIP.iloc[:,-2])
        names_WIP = names_WIP.drop(names_WIP.columns[-2], axis = 1)
        #print(names_WIP) # for debugging, makes a lot of output
        #print('So many columns:', len(names_WIP.columns), '\n')
    #print(type(names_WIP))


    #print('----------------------\n', names_WIP, '----------------------\n')
    # just to be sure to know where it didn't match
    names_WIP.columns = ['corrnames']
    names_WIP = names_WIP.astype(str)

    # now merge these cleaned names into the output dataframe
    occs_newnames = occs.assign(recordedBy = names_WIP['corrnames'])

    occs_newnames['recordedBy'] = occs_newnames['recordedBy'].replace('nan', 'ZZZ_THIS_NAME_FAILED')
    #print(occs_newnames.recordedBy)
    # remove records I cannot work with...
    occs_newnames = occs_newnames[occs_newnames['recordedBy'] != 'ZZZ_THIS_NAME_FAILED']
    if verbose:
        print('It used to look like this:\n', occs.recordedBy)
        print('\n---------------------------------------------------\n')
        print(" Now it looks like this:\n", occs_newnames.recordedBy)

    # summary output
    print('I removed', len(occs) - len(occs_newnames), 'records because I could not handle the name.')
    print('I have saved', len(occs_newnames), ' to the specified file (originally I read', len(occs), 'points)' )
    print('\n Don\'t worry about the warnings above, it works as it should and i don\'t understand python enough to make them go away')

    occs_newnames = occs_newnames.astype(x_1_cols.final_col_type)
    # save to file
    occs_newnames.to_csv(out_dir+'names_standardised.csv', index = False, sep = ';', mode = 'x')

    if debugging:
        unique_names = occs_newnames['recordedBy'].unique()
        unique_names = pd.DataFrame(unique_names)
        unique_names.to_csv(out_dir + 'collector_un_tmp.csv', index = False, sep =';', mode = 'x')
        print('I have saved', len(unique_names), 'Collector names to the file \"collector_un_tmp.csv\".')

    # done
    return occs_newnames
