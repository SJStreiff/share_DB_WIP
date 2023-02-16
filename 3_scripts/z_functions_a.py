#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for basic format changes, subsetting, ...

2022-12-13 sjs

CHANGELOG:
    2022-12-13: created


CONTAINS:
    column_standardiser():
        reads dataframe and subsets columns, adds empty columns for standardised format
    column_cleaning():
        takes columns and puts the content into standard/exchangeable format
    collector_names():
        takes collector names column ("recorded_by") and rearranges it into the format
        Surname, F (firstname just as initials)
'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re

#custom dependencies
import z_dependencies # can be replaced at some point, but later...






def column_standardiser(importfile, data_source_type, verbose=True):
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
        occs = occs.rename(columns = z_dependencies.herbo_key) # rename columns
        occs = occs[z_dependencies.herbo_subset_cols] # subset just required columns
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
        occs = occs.rename(columns = z_dependencies.gbif_key) # rename
        occs = occs[z_dependencies.gbif_subset_cols] # and subset

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
    miss_col = [i for i in z_dependencies.final_cols if i not in occs.columns]


    if verbose:
 	      print('\n','These columns are missing in the data from source: ', miss_col,
          '\n Empty columns will be added and can later be modified.')

    if verbose:
 	      print('These columns are missing in the data being handled: ', miss_col)
    occs[miss_col] = '0'
    occs = occs.astype(dtype = z_dependencies.final_col_type)
    #print(occs.dtypes)
    #_bc
    return occs


def column_cleaning(occs, data_source_type, working_directory, prefix, verbose=True):
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




        occs[['col_date_1', 'col_date_2']] = occs.col_date.str.split("-", expand=True,)
        occs[['det_date_1', 'det_date_2']] = occs.det_date.str.split("-", expand=True,)
        #delete the coldate_2 colomn, doesn't have info we need
        occs.drop(['col_date_2'], axis='columns', inplace=True)
        occs.drop(['det_date_2'], axis='columns', inplace=True)

        #split colDate_1 on '/' into three new fields (dd/mm/yyyy). Let's just hope no american ever turns up in that data
        occs[['col_day', 'col_month', 'col_year']] = occs.col_date_1.str.split("/", expand=True,)
        occs[['det_day', 'det_month', 'det_year']] = occs.det_date_1.str.split("/", expand=True,)
        # cahnge datatype to pd Int64 (integer that can handle NAs)
        occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']] = occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']].astype(float).astype(pd.Int64Dtype())

        #split colDate_1 on '/' into three new fields
        occs[['col_day', 'col_month', 'col_year']] = occs.col_date_1.str.split("/", expand=True,)
        occs[['det_day', 'det_month', 'det_year']] = occs.det_date_1.str.split("/", expand=True,)
        occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']].astype(float)
        #occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']].astype(pd.Int64Dtype())



        # not sure which is best, but if T, then need to rename cols for standardisation
        # T: drop original col and det date comumns, but keep colDate_1 and DetDAte_1
        # S: keep the very original col and det date cols, remove the colDate_1...

        # for the time being I'm just doing it my way...
        occs.drop(['col_date_1'], axis='columns', inplace=True)
        occs.drop(['det_date_1'], axis='columns', inplace=True)

        if verbose:
            print(occs.col_date)
        if verbose:
            print('\n','The collection date has now been split into separate (int) columns for day, month and year')



        #occs.drop(['col_date_1'], axis='columns', inplace=True)
        #occs.drop(['det_date_1'], axis='columns', inplace=True)

        if verbose:
 		         print(occs.col_date)


        # COLLECTION NUMBERS

        # keep the original colnum column
        occs.rename(columns={'colnum': 'colnum_full'}, inplace=True)
        #create prefix, extract text before the number
        occs['prefix'] = occs['colnum_full'].astype(str).str.extract('^([a-zA-Z]*)')
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

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only digits without associated stuff, but including some characters (colNam)
        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\s\d+\s\d+)', # 00 00 00 or so
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_digits) + ')')

        occs['colnum'] = occs['colnum'].str.strip()


        # for completeness we need this
        occs['orig_bc'] = occs['barcode']

        occs = occs.replace('nan', pd.NA) # remove NAs that aren't proper

        #print(occs.head(5))

    #-------------------------------------------------------------------------------
    if(data_source_type == 'GBIF'):
        """things that need to be done in the GBIF data:
            - BARCODE (just number or Herbcode + number)
            - specific epithet!
            - record number (includes collectors regularly...)
        """
    # -----------------------------------------------------------------------
    # split dates into format we can work with

        # format det date into separate int values yead-month-day
        occs['tmp_det_date'] = occs['det_date'].str.split('T', expand=True)[0]
        try:
            occs[['det_year', 'det_month', 'det_day']] = occs['tmp_det_date'].str.split("-", expand=True,)
            occs.drop(['tmp_det_date'], axis='columns', inplace=True)
        except:
            if verbose:
                print('no det dates available...')
        occs = occs.replace('nan', pd.NA)

        if verbose:
            print(occs.det_year)


    # -----------------------------------------------------------------------
    # Barcode issues:
    # sonetimes the herbarium is in the column institute, sometimes herbarium_code, sometimes before the actual barcode number...
            # check for different barcode formats:
            # either just numeric : herb missing
            #     --> merge herbarium and code
        if verbose:
            print('debugging barcode issues \n')
        if verbose:
            print(occs.barcode)


        # if there is a nondigit, take it away from the digits, and modify it to contain only letters
        barcode_regex = [
            r'(\d+\-\d+\/\d+)$',
            r'(\d+\-\d+)$',
            r'(\d+\/\d+)$',
            r'(\d+)$',
        ]
        # extract the numeric part of the barcode
        bc_extract = occs['barcode'].astype(str).str.extract('(' + '|'.join(barcode_regex) + ')')
        #occs['prel_bc'] = occs['prel_bc'].str.strip()
        if verbose:
            print('Numeric part of barcodes extracted', bc_extract)

        i=0
        while(len(bc_extract.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
            i = i+1
            bc_extract.iloc[:,-1] = bc_extract.iloc[:,-1].fillna(bc_extract.iloc[:,-2])
            bc_extract = bc_extract.drop(bc_extract.columns[-2], axis = 1)
            #print(names_WIP) # for debugging, makes a lot of output
            #print('So many columns:', len(names_WIP.columns), '\n')
        print(bc_extract)
        # reassign into dataframe
        occs['prel_bc'] = bc_extract

        if verbose:
            print(occs.prel_bc)
        # now get the herbarium code. First if it was correct to start with, extract from barcode.
        bc = pd.Series(occs['barcode'])
        prel_herbCode = occs['barcode'].str.extract(r'(^[A-Z]+\-[A-Z]+\-)') # gets most issues...
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'([A-Z]+\-)'))
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'([A-Z]+)'))
        occs = occs.assign(prel_herbCode = prel_herbCode)
        occs['prel_code'] = occs['barcode'].astype(str).str.extract(r'(\D+)')
        occs['prel_code_X'] = occs['barcode'].astype(str).str.extract(r'(\d+\.\d)') # this is just one entry and really f@#$R%g annoying
        #occs.to_csv('debug.csv', sep = ';')
        if verbose:
            print(type(occs.prel_code))
        if verbose:
            print(occs.prel_herbCode)

        # if the barcode column was purely numericm integrate
        occs['tmp_hc'] = occs['institute'].str.extract(r'(^[A-Z]+\Z)')
        #occs['tmp_hc'] = occs['barcode'].str.extract(r'(^[A-Z]+\-[A-Z]+\-)')
        occs['tmp_hc'] = occs['herbarium_code'].str.extract(r'(^[A-Z]+\Z)')
        occs['tmp_hc'] = occs['tmp_hc'].replace({'PLANT': 'TAIF'})
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
            print(occs.prel_bc)
        occs['st_barcode'] = occs['prel_herbCode'] + occs['prel_bc']
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
            na_bc.to_csv(working_directory + prefix + 'NA_barcodes.csv', index = False, sep = ';', )
            if verbose:
                print('I have saved', len(na_bc), 'occurences to the file', working_directory+'NA_barcodes.csv for corrections')
            if verbose:
                print('I am continuing without these.')
            occs = occs[occs['st_barcode'].notna()]
            if verbose:
                print(occs)

        # and clean up now
        occs = occs.drop(['prel_bc', 'prel_herbCode', 'prel_code', 'prel_code_X', 'tmp_hc'], axis = 1)
        occs = occs.rename(columns = {'barcode': 'orig_bc'})
        occs = occs.rename(columns = {'st_barcode': 'barcode'})
        if verbose:
            print(occs.columns)

        # -----------------------------------------------------------------------
        # COLLECTION NUMBERS
        # keep the original colnum column
        # split to get a purely numeric colnum, plus a prefix for any preceding characters,
        # and sufix for trailing characters

        occs.rename(columns={'colnum': 'colnum_full'}, inplace=True)
        try:
            occs = occs.colnum_full.replace('s.n.', np.nan, inplace=True) # keep s.n. for later?
        except:
            if verbose:
                print('No plain s.n. values found in the full collection number fields.')
        #occs.colnum_full.replace("s. n.", pd.NA, inplace=True)
        print(occs.colnum_full)
        #create prefix, extract text before the number
        occs['prefix'] = occs['colnum_full'].astype(str).str.extract('^([a-zA-Z]*)')
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

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only number (colNam)

        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_digits) + ')')
        occs['colnum'] = occs['colnum'].str.strip()
        occs['colnum'].replace('nan', pd.NA, inplace=True)

        #print(occs)
        # ok this looks good for the moment.
        """ Still open here: TODO
        remove collector name in prefix (partial match could take care of that)
        """

        # SPECIFIC EPITHET

        # darwin core specific...

        occs[['tmp', 'specific_epithet']] = occs['species-tobesplit'].str.split(' ', expand = True)
        #print(occs.tmp)


        occs[['tmp', 'specific_epithet']] = occs['species-tobesplit'].str.split(' ', expand = True)
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



    occs = occs.astype(dtype = z_dependencies.final_col_type) # check data type
    #print(occs.dtypes)
    if verbose:
        print('\n','Data has been standardised to conform to the columns we need later on.') #\n It has been saved to: ', out_file)
    if verbose:
        print('\n','Shape of the final file is: ', occs.shape )

    if verbose:
        print(occs.columns)
    #occs = occs.replace('nan', '')
    #
    occs = occs.astype(dtype = z_dependencies.final_col_type)
    if verbose:
        print('Shape of the final file is: ', occs.shape )

    #occs.to_csv(out_file, sep = ';')

    print('\n','Column standardisation completed successfully.')

    return occs

def collector_names(occs, working_directory, prefix, verbose=True, debugging=False):
    """
    With an elaborate regex query, match all name formats I have been able to come up with.
    This is applied to both recorded_by and det_by columns. All those records that failed are
    written into a debugging file, to be manually cleaned.
    """

    pd.options.mode.chained_assignment = None  # default='warn'
    # this removes this warning. I am aware that we are overwriting stuff with this function.
    # the column in question is backed up


    #print(occs.dtypes) # if you want to double check types again
    occs = occs.astype(dtype = z_dependencies.final_col_type)
    occs['recorded_by'] = occs['recorded_by'].replace('nan', pd.NA)
    occs['det_by'] = occs['det_by'].replace('nan', pd.NA)
    #print(occs.head)
    # -------------------------------------------------------------------------------
    # Clean some obvious error sources, back up original column
    occs['orig_recby'] = occs['recorded_by'] # keep original column...
    occs['orig_detby'] = occs['det_by']
    # remove the introductory string before double point :
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Collector(s):', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('&', ';', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace(' y ', ';', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace(' and ', ';', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Jr.', 'JUNIOR', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('et al.', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('et al', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('etal', '', regex=False)
    #occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Philippine Plant Inventory (PPI)', 'Philippines, Philippines Plant Inventory', regex=False)
    #we will need to find a way of taking out all the recorded_by with (Dr) Someone's Collector

    #isolate just the first collector (before a semicolon)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.split(';').str[0]

    occs['recorded_by'] = occs['recorded_by'].str.strip()

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


    #-------------------------------------------------------------------------------
    # regex queries: going from most specific to least specific.

   
    extr_list = {
            #r'^([A-Z][a-z]\-[A-Z][a-z]\W+[A-Z][a-z])' : r'\1', # a name with Name-Name Name
            #r'^([A-Z][a-z]+)\W+([A-Z]{2,5})' : r'\1, \2', #Surname FMN
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([a-z]{0,3})' : r'\1, \2\3\4\5 \6',  # all full full names with sep = ' ' plus Surname F van
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([A-Z])[a-z]+.*' : r'\1, \2\3\4\5',  # all full full names with sep = ' '

            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([a-z]{0,3})' : r'\1, \2\3\4 \5',  # all full full names with sep = ' ' plus Surname F van
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([A-Z])[a-z]+.*' : r'\1, \2\3\4',  # all full full names with sep = ' '

            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([a-z]{0,3})': r'\1, \2\3 \4',  # all full names: 2 given names # + VAN
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+': r'\1, \2\3',  # all full names: 2 given names

            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]{2,20}\s+([a-z]{0,3})': r'\1, \2 \3',  # just SURNAME, Firstname  # + VAN
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]{2,20}': r'\1, \2',  # just SURNAME, Firstname

            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W*\s+([a-z]{0,3})\Z': r'\1, \2\3\4 \5',  # Surname, F(.) M(.) M(.) # VAN
            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W*': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.)

            r'(^[A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W\s+([a-z]{0,3})\,.+': r'\1, \2\3\4 \5',  # Surname, F(.) M(.) M(.), other collectors
            r'(^[A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W\,.+': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.), other collectors

            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+\s+([a-z]{0,3})\Z': r'\1, \2\3 \4',  # Surname, F(.) M(.)
            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+\Z': r'\1, \2\3',  # Surname, F(.) M(.)

            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])([A-Z])\s+([a-z]{0,3})': r'\1, \2\3\4\5 \6',  # Surname FMMM
            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])([A-Z])': r'\1, \2\3\4\5',  # Surname FMMM

            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])\s+([a-z]{0,3})': r'\1, \2\3\4 \5',  # Surname FMM
            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])': r'\1, \2\3\4',  # Surname FMM

            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])\s+([a-z]{0,3})\Z': r'\1, \2\3 \4',  # Surname FM
            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])\Z': r'\1, \2\3',  # Surname FM

            r'^([A-Z][a-z]+)\W+([A-Z])\s+([a-z]{0,3})\Z': r'\1, \2 \3',  # Surname F
            r'^([A-Z][a-z]+)\W+([A-Z])\Z': r'\1, \2',  # Surname F

            r'^([A-Z][a-z]+)\W*\Z': r'\1',  # Surname without anything
            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+\s+([a-z]{0,3})': r'\1, \2 \3',  # Surname, F(.)
            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+': r'\1, \2',  # Surname, F(.)

            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([a-z]{0,3})\s([A-Z][a-z]+)': r'\6, \1\2\3\4 \5', # Firstname Mid Nid Surname ...
            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\5, \1\2\3\4', # Firstname Mid Nid Surname ...

            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([a-z]{0,3})\s([A-Z][a-z]+)': r'\5, \1\2\3 \4', # Firstname Mid Nid Surname ...
            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\4, \1\2\3', # Firstname Mid Nid Surname ...

            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([a-z]{0,3})\s([A-Z][a-z]+)': r'\4, \1\2 \3', # Firstname Mid Surname ...
            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\3, \1\2', # Firstname Mid Surname ...

            r'^([A-Z])[a-z]+\s([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\4, \1\2 \3', # Firstname M. Surname ...
            r'^([A-Z])[a-z]+\s([A-Z])\W+([A-Z][a-z]+)': r'\3, \1\2', # Firstname M. Surname ...

            r'^([A-Z])[a-z]+\s([a-z]{0,3})\s([A-Z][a-z]+)': r'\3, \1 \2', # Firstname Surname
            r'^([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\2, \1', # Firstname Surname

            r'^([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\2, \1', # F. Surname ...
            r'^([A-Z])\W+([A-Z][a-z]+)': r'\2, \1', # F. Surname ...

            r'^([A-Z])\W+([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\4, \1\2 \3', #F. M. van  Surname
            r'^([A-Z])\W+([A-Z])\W+([A-Z][a-z]+)': r'\3, \1\2', #F. M. Surname
            
            r'^([A-Z])\W+([A-Z])\W+([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\5, \1\2\3 \4', #F. M. M. van Surname
            r'^([A-Z])\W+([A-Z])\W+([A-Z])\W+([A-Z][a-z]+)': r'\4, \1\2\3', #F. M. M. van Surname


            r'^([A-Z])([A-Z])([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\5, \1\2\3 \4', #FMM Surname
            r'^([A-Z])([A-Z])([A-Z])\W+([A-Z][a-z]+)': r'\4, \1\2\3', #FMM Surname

            r'^([A-Z])([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\4, \1\2 \3', #FM Surname
            r'^([A-Z])([A-Z])\W+([A-Z][a-z]+)': r'\3, \1\2', #FM Surname

            r'^([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\3, \1 \2', #F Surname
            r'^([A-Z])\W+([A-Z][a-z]+)': r'\2, \1', #F Surname
            #r'^([A-Z][a-z]+)\W+([A-Z]{2,5})' : r'\1, \2', #Surname FMN

        }


    # The row within the extr_list corresponds to the column in the debugging dataframe printed below


    names_WIP = occs[['recorded_by']] #.astype(str)

    #print(names_WIP)
    i = 0
    for key, value in extr_list.items():
        i = i+1
        # make a new panda series with the regex matches/replacements
        X1 = names_WIP.recorded_by.str.replace(key, value, regex = True)
        # replace any fields that matched with empty string, so following regex cannot match.
        names_WIP.loc[:,'recorded_by'] = names_WIP.loc[:,'recorded_by'].str.replace(key, '', regex = True)
        # make new columns for every iteration
        names_WIP.loc[:,i] = X1 #.copy()

    # debugging dataframe: every column corresponds to a regex query
    if debugging:
        names_WIP.to_csv(working_directory + prefix + 'DEBUG_regex.csv', index = False, sep =';', )
        print('debugging dataframe printed to', working_directory + prefix + 'DEBUG_regex.csv')

    #####
    # Now i need to just merge the columns from the right and bobs-your-uncle we have beautiful collector names...
    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')

    #-------------------------------------------------------------------------------
    # For all names that didn't match anything:
    # extract and then check manually
    TC_occs = occs.copy()
    TC_occs['to_check'] = names_WIP['recorded_by']
    #print(TC_occs.to_check)

    # #-------------------------------------------------------------------------------


    # mask all values that didn't match for whatever reason in the dataframe (results in NaN)
    names_WIP = names_WIP.mask(names_WIP.recorded_by.notna())

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
    occs_newnames = occs.assign(recorded_by = names_WIP['corrnames'])



    occs_newnames['recorded_by'] = occs_newnames['recorded_by'].replace('nan', 'ZZZ_THIS_NAME_FAILED')
    occs_newnames['recorded_by'] = occs_newnames['recorded_by'].replace('<NA>', 'ZZZ_THIS_NAME_FAILED')

    #print(occs_newnames.recorded_by)
    # remove records I cannot work with...
    occs_newnames = occs_newnames[occs_newnames['recorded_by'] != 'ZZZ_THIS_NAME_FAILED']


    # ------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------
    # repeat the story with the det names
    print(occs_newnames.recorded_by)
    names_WIP = occs_newnames[['det_by']] #.astype(str)

    #print(names_WIP)
    i = 0
    for key, value in extr_list.items():
        i = i+1
        # make a new panda series with the regex matches/replacements
        X1 = names_WIP.det_by.str.replace(key, value, regex = True)
        # replace any fields that matched with empty string, so following regex cannot match.
        names_WIP.loc[:,'det_by'] = names_WIP.loc[:,'det_by'].str.replace(key, '', regex = True)
        # make new columns for every iteration
        names_WIP.loc[:,i] = X1 #.copy()

    # debugging dataframe: every column corresponds to a regex query
    if debugging:
        names_WIP.to_csv(working_directory + prefix + 'DEBUG_detby_regex.csv', index = False, sep =';', )
        print('debugging dataframe for det bys printed to', working_directory + prefix + 'DEBUG_detby_regex.csv')

    #####
    # Now i need to just merge the columns from the right and bobs-your-uncle we have beautiful collector names...
    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')

    #-------------------------------------------------------------------------------
    # For all names that didn't match anything:
    # extract and then check manually

    #print(TC_occs.to_check)

    TC_occs1 = occs_newnames.copy()
    TC_occs1['to_check_det'] = names_WIP['det_by']
    #print(TC_occs.to_check)
    TC_occs1['to_check_det'] = TC_occs1['to_check_det'].replace('<NA>', pd.NA)

    ###




    #-------------------------------------------------------------------------------


    # mask all values that didn't match for whatever reason in the dataframe (results in NaN)
    names_WIP = names_WIP.mask(names_WIP.det_by.notna())

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
    occs_newnames = occs_newnames.assign(det_by = names_WIP['corrnames'])




    TC_occs.dropna(subset= ['to_check'], inplace = True)
    TC_occs.det_by = occs_newnames.det_by

    TC_occs1.dropna(subset= ['to_check_det'], inplace = True)
    #print(TC_occs.to_check)
    TC_occs['to_check_det'] = 'recby_problem'
    TC_occs1['to_check'] = 'detby_problem'

    TC_occs = pd.concat([TC_occs, TC_occs1])#, on = 'orig_recby', how='inner')
    print('YAY1',TC_occs.shape, 'COLUMNS', TC_occs.columns)
    TC_occs = TC_occs.drop_duplicates(subset = ['barcode'], keep = 'first')
    print('YAY2',TC_occs.shape)

    TC_occs_write = TC_occs[['recorded_by', 'orig_recby', 'colnum_full', 'det_by', 'orig_detby', 'to_check', 'to_check_det']]

    # output so I can go through and check manually
    if len(TC_occs)!= 0:
        TC_occs_write.to_csv(working_directory +'TO_CHECK_' + prefix + 'probl_names.csv', index = True, sep = ';', )
        print(len(TC_occs), ' records couldn\'t be matched to the known formats.',
        '\n Please double check these in the separate file saved at: \n', working_directory+'TO_CHECK_'+prefix+'probl_names.csv')



    # ------------------------------------------------------------------------------#
    # ------------------------------------------------------------------------------

    if verbose:
        print('It used to look like this:\n', occs.recorded_by)
        print('\n---------------------------------------------------\n')
        print(" Now it looks like this:\n", occs_newnames.recorded_by)

    if verbose:
        print('\n AND \n It used to look like this:\n', occs.det_by)
        print('\n---------------------------------------------------\n')
        print(" Now it looks like this:\n", occs_newnames.det_by)

    # summary output
    print('I removed', len(occs) - len(occs_newnames), 'records because I could not handle the name.')
    print('I have saved', len(occs_newnames), ' to the specified file (originally I read', len(occs), 'points)' )
    print('\n Don\'t worry about the warnings above, it works as it should and i don\'t understand python enough to make them go away')

    occs_newnames = occs_newnames.astype(z_dependencies.final_col_type)
    # save to file
    occs_newnames.to_csv(working_directory+prefix+'names_standardised.csv', index = False, sep = ';', )
    occs_newnames['coll_surname'] = occs_newnames['recorded_by'].str.split(',', expand=True)[0]

    if debugging:
        unique_names = occs_newnames['recorded_by'].unique()
        unique_names = pd.DataFrame(unique_names)
        unique_names.to_csv(working_directory + prefix+'collectors_unique.csv', index = False, sep =';', )
        print('I have saved', len(unique_names), 'Collector names to the file', working_directory + prefix+'collectors_unique.csv.')

    # done
    return occs_newnames, TC_occs



def reinsertion(occs_already_in_program, frame_to_be_better, names_to_reinsert, verbose=True):
    '''
    Quickly read in data for reinsertion, test that nothing went too wrong, and append to the data already in the system.
    '''
    if verbose:
        print('REINSERTING...')
        print(names_to_reinsert)
    imp = codecs.open(names_to_reinsert,'r','utf-8') #open for reading with "universal" type set
    print('problems?')
    re_occs = pd.read_csv(imp, sep = ';',  dtype = str) # read the data
    print('yes')
    print(re_occs)
    print(re_occs.columns)
    #try:
    re_occs = re_occs.drop(['to_check', 'to_check_det'], axis = 1)
    re_occs.sort_index(inplace=True)
    re_occs = re_occs.replace('NaN', pd.NA)
    print(re_occs.recorded_by)
    frame_to_be_better.sort_index(inplace=True)
    print(frame_to_be_better.orig_recby)
    frame_to_be_better['recorded_by'] = re_occs['recorded_by']
    frame_to_be_better['det_by'] = re_occs['det_by']
    
    print(frame_to_be_better.recorded_by) #[['recordedBy', 'orig_recby']])
    print(frame_to_be_better.orig_recby)
    #except:
    #    print('Special columns are already removed.')
    print('here?')
    frame_to_be_better = frame_to_be_better.astype(z_dependencies.final_col_type)

    #if verbose:
    print('Reinsertion data read successfully')

    occs_merged = pd.concat([occs_already_in_program, frame_to_be_better])
    print('TOTAL', len(occs_merged), 'in Prog', len(occs_already_in_program), 'reintegrated', len(re_occs))
    if len(occs_merged) == len(occs_already_in_program) + len(re_occs):
        if verbose:
            print('Data reinserted successfully.')
    else:
        raise Exception("Something weird happened, please check input and code.")

    return occs_merged







#
