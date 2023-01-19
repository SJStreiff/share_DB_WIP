#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Connect to GLOBAL
2023-01-12 sjs

CHANGELOG:
    2023-01-12: created
CONTAINS:
    db_connector():
      script which connects to the postgres DB


'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re

# sqlalchemy lets us talk to the database
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import MetaData, Table, Column, Integer, String

# password prompt
from getpass import getpass




def fetch_master_db(database, host, tablename, schema):  #subset????
    """ PLEASE PROTECT YOUR SENSITIVE INFORMATION: passwords, ports etc.
    Connects to the master database (GLOBAL) and downloads the database subset
    for comparing the new data later on
    INPUTS:
     inputs with * are prompted when running the script, protecting somewhat passwords etc.
        master_db_name: the name of the database
        *username: the username to be used
        *pw: password of the user
        host: the host to access
        *port: port to access:
        tablename: name of the table that we want
        subset: which part of the dataset do you want (e.g. country)
    OUTPUT:
        master_db: pandas dataframe of the subset requested

    """
    #-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-
    #
    #  MAY HAVE SENSITIVE INFORMATION. DO NOT UPLOAD TO GITHUB!!!
    #
    #-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-

    print('\n ................................\n',
    'NOTE that for the GLOBAL database you must be connected to the VPN...\n'
    'Please type the USERNAME used to connect to the database:')
    username=input() #'n' # make back to input()
    print('\n ................................\n',
    'Please type the PASSWORD used to connect to the database for user', username)
    password=getpass() #'n' # make back to input()
    print('\n ................................\n',
    'Please type the PORT required to connect to the database:')
    port=input() #'n' # make back to input()

    url_obj = URL.create(
        'postgresql',
        username,
        password,
        host,
        #-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!
        port,
        database,
        )

    engine = create_engine(url_obj)

    print('Connection successful, the following tables are found on the standard schema:', engine.table_names())

    metadata_obj = MetaData()


    master_db_test = pd.read_sql_table('philippines_test', engine)
    print(master_db_test.head())

    return master_db_test


def send_to_sql(data, database, host, tablename, schema):
    """ send a dataframe to the specified sql databases
    """

    print('\n ................................\n',
    'NOTE that for the GLOBAL database you must be connected to the VPN...\n'
    'Please type the USERNAME used to connect to the database:')
    username=input() #'n' # make back to input()
    print('\n ................................\n',
    'Please type the PASSWORD used to connect to the database for user', username)
    password=getpass() #'n' # make back to input()
    print('\n ................................\n',
    'Please type the PORT required to connect to the database:')
    port=input() #'n' # make back to input()

    url_obj = URL.create(
        'postgresql',
        username,
        password,
        host,
        #-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!
        port,
        database,
        )

    engine = create_engine(url_obj)

    data.to_csv('loading_table', engine, schema)

    print('Maybe this worked')

    #
# print('Hostname?')
# hostname=input()
# test = fetch_master_db('GLOBAL', hostname, 'phil_test_221209', 'serafin_test')


#
