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
    # important: re-encode into utf-8 to have all non-english characters and accents work
    response.encoding = 'UTF-8'
    # print('ENCODING', response.apparent_encoding)
    # print(f"The response is: {response.text}")

    # now feed the html document into beautiful soup
    soup = BeautifulSoup(response.text, "html.parser")

    # print(soup)
    print(soup.findAll(href = re.compile("botanist_search.php")))

    # we now get all possibilities found on the webpage
    pot_names = soup.findAll(href = re.compile("botanist_search.php"))
    """ structure is:
     <a href="botanist_search.php?mode=details&amp;id=28447">P. A. W. J. de Wilde</a>     
    """

    # now format it in a way we can work with it
    # print(pot_names)
    #pot_names = pd.DataFrame(pot_names)
    # print(type(pot_names))
    # print(len(pot_names))
    
    # to integrate into a dataframe afterwards

    tmp = ('link/id', 'name')
    pot_df = pd.DataFrame(tmp).transpose()
    print(pot_df)

    for i in pot_names:
        #print('I:', i) # just to check the whole thing...
        #print(type(i))
        i1 = i.attrs # this gets us the href which has the botanistsearch id...
        i1 = pd.Series(str(i1))

        i2 = pd.DataFrame(i).astype(str)
        i2 = i2.astype(str).replace('<strong>','')
        i2 = i2.replace('</strong>','')
        #print('DIM!', len(i2))
        #print(i2)       
        i3 = ''
        for j in range(0,len(i2)):
            j_tmp = i2.iloc[j]
            # print(i2.iloc[j])

            j_tmp = j_tmp.astype(str).str.replace('</strong>','')           
            j_tmp = j_tmp.astype(str).str.replace('<strong>','') 
            i3 = i3+j_tmp
            # print(j)
            # print(i3)

        i3 = i3.str.replace('.', '. ', regex=False)

        i1 = i1.str.split(':', expand=True)[1]
        i1 = i1.str.strip('}')
        i1 = i1.str.replace('\'','')
        i1 = i1.str.replace(' ','')

        I_out = (i1[0], i3[0])
       # print(I_out)

        # I_out is now a vector with one element of links to detail page, and other element the full name as offered as choice at HUH
        # 
         

# I think the best idea is to search by surname only, and then filter by firstname initials. 
# Then i can match regex patterns when I have cleaned the dataframe....

        new_row =  pd.DataFrame(I_out).transpose()
        # print(new_row)
        pot_df = pd.concat([pot_df, new_row], axis=0)

    #pot_names = pd.DataFrame(pot_names.str.split(',', extend = True))
    pot_df.reset_index(inplace=True)

    pot_df.columns = pot_df.iloc[0]
    pot_df.drop(index = 0, axis = 0, inplace=True)
    print('Here the pot_df after cleaning up:\n',pot_df)

    pot_df.name = pot_df.name.str.replace('De ', 'de ')
    pot_df.name = pot_df.name.str.replace('.', '')
    # TODO: remove any fields with &, indicating teams instead of individuals...
    




    ###---------
    # Now with the power of regex, format resulting names to the same standard as used in previous steps.
    # Then check for similarity 




    extr_list = {
            #r'^([A-Z][a-z]\-[A-Z][a-z]\W+[A-Z][a-z])' : r'\1', # a name with Name-Name Name
            #r'^([A-Z][a-z]+)\W+([A-Z]{2,5})' : r'\1, \2', #Surname FMN

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


    names_WIP = pd.DataFrame(pot_df.name) #.astype(str)
    print(names_WIP)
    i = 0
    for key, value in extr_list.items():
        i = i+1
        # make a new panda series with the regex matches/replacements
        X1 = names_WIP.name.str.replace(key, value, regex = True)
        # replace any fields that matched with empty string, so following regex cannot match.
        names_WIP.loc[:,'name'] = names_WIP.loc[:,'name'].str.replace(key, '', regex = True)
        # make new columns for every iteration
        names_WIP.loc[:,i] = X1 #.copy()

    # debugging dataframe: every column corresponds to a regex query
        #if debugging:
    names_WIP.to_csv('DEBUG_regex.csv', index = False, sep =';', )
    print('debugging dataframe printed to','DEBUG_regex.csv')

    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')
    names_WIP = names_WIP.mask(names_WIP.name.notna())

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
    pot_df_ext = pot_df.assign(regexed_nm = names_WIP['corrnames'])
    print(pot_df_ext)


#
test = get_HUH_names(recordedBy)
print(test)








#
