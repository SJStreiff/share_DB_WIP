#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for namechecking the Harvard University Herbarium collectors database

2023-01-10 sjs

CHANGELOG:
    2023-01-10: created
    2023-01-12: it works. BUT it doesn't give what i want. I do not get access to the full name of the collector or ID I need to access more deta


CONTAINS:
    harvard_reference():
      queries the HUH database of collectors for the correct name format.

'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import requests

#custom dependencies
import z_dependencies # can be replaced at some point, but later...


recordedBy = "Wilde, WJ de"

def get_secret_message():
    url = os.environ["SECRET_URL"]
    response = requests.get(url)
    print(f"The secret message is: {response.text}")



def get_HUH_names(recordedBy, verbose=True):
    """ Query the HUH database on botanists/collectors names.
    In: Collector name (in clean format?)
    Out: I don't know yet
    """
    print('HUH name checker \n DEBUGGING!! \n .........................\n')
    if verbose:
        print("Checking the botanist", recordedBy)


    # split recorded by into Firstnames and Surnames
    lastname, firstnames = recordedBy.split(',')
    # key_mid = r'([A-Z]*)', ''
    mid_insert = re.sub(r'([A-Z])', '', firstnames).strip()
    # key_first = {: r''}
    firstnames = re.sub(r'([a-z]{0,3})', '', firstnames)

    print(firstnames, lastname, 'MID=', mid_insert)


    # add points and plus into firstnames string
    if len(firstnames) > 0:
        s = firstnames[0]
        firstnames=firstnames.strip()
        for i in range(4):
            try:
                s = s + firstnames[i] + '.' + '+'
                s.replace('.+.', '.')
            except:
                f='Idontknow'


        Firstname_query = s.strip()
    print(Firstname_query)

    # create name=<string> for insertion into url for query.
    lastname=lastname.strip()
    if mid_insert == '':
        name_string = Firstname_query+lastname
    else:
        name_string = Firstname_query+mid_insert+'+'+lastname
    print(name_string)
    name_string=name_string.strip() # just to make sure no leadin/trailing whitespace
    # do query

    url = "https://kiki.huh.harvard.edu/databases/botanist_search.php?name="+name_string+"&individual=on&json=y"
    print('The URL is:', url)
    response = requests.get(url)
    print(f"The response is: {response.text}")




    return 'Yay maybe'

test = get_HUH_names(recordedBy)
print(test)
