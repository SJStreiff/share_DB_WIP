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

from bs4 import BeautifulSoup
# get the 
#beautiful soup?
# scrapy




# for debugging. Full name is "de Wilde, Willem Jan Jacobus Oswald", and W. J. de Wilde links to this in HUH in theory
recordedBy = "Wilde, WJ de"










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
    # if mid_insert == '':
    #     name_string = Firstname_query+lastname
    # else:
    #     name_string = Firstname_query+mid_insert+'+'+lastname
    name_string = lastname
    print(name_string)
    name_string=name_string.strip() # just to make sure no leadin/trailing whitespace
    # do query

    url = "https://kiki.huh.harvard.edu/databases/botanist_search.php?name="+name_string+"&individual=on"
    print('The URL is:', url)
    response = requests.get(url)
    # print(f"The response is: {response.text}")


    # now feed the html document into beautiful soup?
    soup = BeautifulSoup(response.text, "html.parser")


    print(soup.findAll(href = re.compile("botanist_search.php")))

    # we now get all possibilities found on the webpage
    pot_names = soup.findAll(href = re.compile("botanist_search.php"))
    """ structure is:
     <a href="botanist_search.php?mode=details&amp;id=28447">P. A. W. J. de Wilde</a>
     SEP = ','
     
    """
    # now format it in a way we can work with it
    print(pot_names)
    #pot_names = pd.DataFrame(pot_names)
    print(type(pot_names))
    print(len(pot_names))
    
    for i in pot_names:
        print('I:', i) # just to check the whole thing...
        print(type(i))
        i1 = i.attrs # this gets us the href which has the botanistsearch id...
        i1 = pd.Series(str(i1))

        i2 = pd.DataFrame(i).astype(str)
        print(i1[0], '111111')
        print(i2[0], '222222')

        i1 = i1.str.split(':', expand=True)[1]
        i1 = i1.str.strip('}')
        i1 = i1.str.replace('\'','')
        i1 = i1.str.replace(' ','')

        I_out = (i1[0], i2)
        print(I_out)

# I think the best idea is to search by surname only, and then filter by firstname initials. 
# Then i can match regex patterns when I have cleaned the dataframe....

        #new_row =  pd.DataFrame(i.str.split('\"', extend = True))
        #pot_df = pd.concat(pot_df, new_row)

    #pot_names = pd.DataFrame(pot_names.str.split(',', extend = True))
    #print(pot_names)



#
test = get_HUH_names(recordedBy)
print(test)








#
