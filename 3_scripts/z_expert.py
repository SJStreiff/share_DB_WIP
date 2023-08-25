#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Functions for expert det

2023-08-23 sjrs


- format input data

- check for missing data etc.

- integrate with master/find duplicated BC
"""

import codecs




def read_expert(importfile, verbose=True):
    """
    read file, check columns
    """
    print('EXPERT file integration. \n',
          'Please assure that your columns are the following:',
          'ddlat, ddlong, locality, country or ISO2, recorded_by, colnum_full, det_by, det_date, barcode')
    imp = codecs.open(importfile,'r','utf-8')
    exp_dat = pd.read_csv(imp, sep = ';',  dtype = str)
    exp_dat['source_id'] = 'specialist'

    # make prefix from colnum
    exp_dat['prefix'] = exp_dat.colnum_full.str.extract('^([a-zA-Z]*)')
    exp_dat['prefix'] = exp_dat['prefix'].str.strip()

    # make sufix from colnum
    # going from most specific to most general regex, this list takes all together in the end
    regex_list_sufix = [
        r'(?:[a-zA-Z ]*)$', ## any charcter at the end
        r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
        r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
    ]
    exp_dat['sufix'] = exp_dat['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
    exp_dat['sufix'] = exp_dat['sufix'].str.strip()

    # extract only digits without associated stuff, but including some characters (colNam)
    regex_list_digits = [
        r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
        r'(?:\d+\s\d+\s\d+)', # 00 00 00 or so
        r'(?:\d+\.\d+)', # 00.00
        r'(?:\d+)', # 00000
    ]
    exp_dat['colnum']  = exp_dat.colnum_full.str.extract('(' + '|'.join(regex_list_digits) + ')')
    exp_dat['colnum'] = exp_dat['colnum'].str.strip()




