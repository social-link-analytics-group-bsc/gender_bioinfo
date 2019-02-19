#!/usr/bin/env python
# coding: utf-8

import csv
import sys
import MySQLdb
import re
import unidecode
import string
import pickle
import json, requests, pprint
import os
import pandas as pd
from selenium import webdriver
import time
import requests
import bs4
import numpy as np
from hammock import Hammock as GendreAPI
from collections import Counter
gendre = GendreAPI("http://api.namsor.com/onomastics/api/json/gendre")

cwd = os.getcwd()

foundationsData = pd.read_csv(cwd + '/biolitmap_data.csv', sep='\t')
foundationsData['source'] = foundationsData['source'].str.lower()


# In[2]:


def get_links_trackable(l, journal):
    
    links = []
    
    if journal == 'plos computational biology':
        for link in l:
            if link is not np.nan:
                links.append('https://journals.plos.org/ploscompbiol/article?id=' + str(link))
            else:
                links.append(np.nan)

    if journal == 'bmc bioinformatics' or journal == 'bmc genomics':
        for link in l:
            if link is not np.nan:
                links.append('https://bmcbioinformatics.biomedcentral.com/articles/' + str(link))
            else:
                links.append(np.nan)
            
    return links

def get_links_untrackable(l):
    
    driver = webdriver.Chrome()
    start = time.time()
    
    links = []
    
    for count, DOI_link in enumerate(l):
        if DOI_link is not np.nan:

            driver.get("https://dx.doi.org/")
            element = driver.find_element_by_xpath("//input[@name='hdl'][@type='text']")
            element.send_keys(str(DOI_link))
            element.submit() 

            if len(driver.current_url) > 70:
                
                # page not found...
                if 'unavailable' in driver.current_url:
                    
                    driver.close()
                    links.append(np.nan)
                    now = time.time()
                    print(count, now-start)
                    time.sleep(2)
                    
                # if we are detected as robots...    
                else:

                    driver.close()
                    time.sleep(500)
                    driver = webdriver.Chrome()
                    driver.get("https://dx.doi.org/")
                    element = driver.find_element_by_xpath("//input[@name='hdl'][@type='text']")
                    element.send_keys(str(DOI_link))
                    element.submit()
                    links.append(driver.current_url)
                    now = time.time()
                    print(count, now-start)
                    time.sleep(2)
                    
            else:
                
                links.append(driver.current_url)
                now = time.time()
                print(count, now-start)
                time.sleep(2)
        
        else:
            links.append(np.nan)
        
    driver.close()
    uniqueLinksSet = set(links)
    print("Number of links:")
    print(len(uniqueLinksSet))
        
    return links


def get_authors(l):
    
    authors=[]
    
    print('Collecting authors...')
    
    for link in l:
        if link is not np.nan:
            page = requests.get(link).text
            soup = bs4.BeautifulSoup(page, 'lxml')
            list_authors = [item.attrs['content'] for item in soup('meta') if item.has_attr('name') and item.attrs['name'].lower()=='citation_author']
            authors.append(list_authors)
        else:
            authors.append(np.nan)
            
    print('Authors collected!')
        
    return authors

def gender_id(l):
    
    genders = []
    length = len(l)
    i=0
    
    for article in l:
        genders_article = []
        if article is not np.nan:
            for person in article:
                first_name = person.split()[0]
                last_name = person.split()[-1]
                resp = gendre(first_name.encode('utf-8'), last_name.encode('utf-8')).GET()
                genders_article.append(resp.json().get('gender'))
            genders.append(genders_article)
        else:
            genders.append([np.nan])
        
        i+=1
        
    return genders


# # Collect links and authors from trackable journals.

# list_DOI_plos_computational_biology = list(foundationsData[foundationsData['source'] == 'plos computational biology']['DOI'])
# list_DOI_bmc_bioinformatics = list(foundationsData[foundationsData['source'] == 'bmc bioinformatics']['DOI'])
# list_DOI_bmc_genomics = list(foundationsData[foundationsData['source'] == 'bmc genomics']['DOI'])
# 
# links_plos_computational_biology = get_links_trackable(list_DOI_plos_computational_biology, 'plos computational biology')
# links_bmc_bioinformatics = get_links_trackable(list_DOI_bmc_bioinformatics, 'bmc bioinformatics')
# links_bmc_genomics = get_links_trackable(list_DOI_bmc_genomics, 'bmc genomics')

# authors_plos_computational_biology = get_authors(links_plos_computational_biology)
# authors_bmc_bioinformatics = get_authors(links_bmc_bioinformatics)
# authors_bmc_genomics = get_authors(links_bmc_genomics)
# 
# authors_journals = authors_plos_computational_biology + authors_bmc_bioinformatics + authors_bmc_genomics

# # Find genders !

# genders_plos_computational_biology = gender_id(authors_plos_computational_biology)
# genders_bmc_bioinformatics = gender_id(authors_bmc_bioinformatics)
# genders_bmc_genomics = gender_id(authors_bmc_genomics)
# 
# genders_journals = genders_plos_computational_biology + genders_bmc_bioinformatics + genders_bmc_genomics

# db_plos_computational_biology = foundationsData[foundationsData['source'] == 'plos computational biology']
# db_DOI_bmc_bioinformatics = foundationsData[foundationsData['source'] == 'bmc bioinformatics']
# db_DOI_bmc_genomics = foundationsData[foundationsData['source'] == 'bmc genomics']
# 
# db_journals = pd.concat([db_plos_computational_biology, db_DOI_bmc_bioinformatics, db_DOI_bmc_genomics])
# 
# db_journals['authors_fullname'] = authors_journals
# db_journals['genders'] = genders_journals
# 
# db_journals.to_csv('/home/bsclife018/Desktop/BSC/bibmap-master/data/db_biolitmap.csv', index = False)

# # Collect links and authors from untrackable journals.

# In[ ]:

#list_DOI_oxford_bioinformatics = list(foundationsData[foundationsData['source'] == 'oxford bioinformatics']['DOI'])
#links_oxford_bioinformatics= get_links_untrackable(list_DOI_oxford_bioinformatics)

#with open('/home/bsclife018/Desktop/BSC/links_oxford_bioinformatics.txt', 'w') as file_handler:
#    for item in links_oxford_bioinformatics:
#        file_handler.write("{}\n".format(item))
        
list_DOI_nucleic_acids_research = list(foundationsData[foundationsData['source'] == 'nucleic acids research']['DOI'])
links_nucleic_acids_research = get_links_untrackable(list_DOI_nucleic_acids_research)

with open('/home/bsclife018/Desktop/BSC/links_nucleic_acids_research.txt', 'w') as file_handler:
    for item in links_nucleic_acids_research:
        file_handler.write("{}\n".format(item))
        
authors_oxford_bioinformatics = get_authors(links_oxford_bioinformatics)

with open('/home/bsclife018/Desktop/BSC/authors_oxford_bioinformatics.txt', 'w') as file_handler:
    for item in authors_oxford_bioinformatics:
        file_handler.write("{}\n".format(item))

authors_nucleic_acids_research = get_authors(links_nucleic_acids_research)

with open('/home/bsclife018/Desktop/BSC/authors_nucleic_acids_research.txt', 'w') as file_handler:
    for item in authors_nucleic_acids_research:
        file_handler.write("{}\n".format(item))
        
genders_oxford_bioinformatics = gender_id(authors_oxford_bioinformatics)

with open('/home/bsclife018/Desktop/BSC/genders_oxford_bioinformatics.txt', 'w') as file_handler:
    for item in genders_oxford_bioinformatics:
        file_handler.write("{}\n".format(item))
        
genders_nucleic_acids_research = gender_id(authors_nucleic_acids_research)

with open('/home/bsclife018/Desktop/BSC/genders_nucleic_acids_research.txt', 'w') as file_handler:
    for item in genders_nucleic_acids_research:
        file_handler.write("{}\n".format(item))

db_oxford_bioinformatics = foundationsData[foundationsData['source'] == 'oxford bioinformatics']
db_oxford_bioinformatics['authors_fullname'] = authors_oxford_bioinformatics
db_oxford_bioinformatics['genders'] = genders_oxford_bioinformatics
db_oxford_bioinformatics.to_csv('/home/bsclife018/Desktop/BSC/bibmap-master/data/db_oxford.csv', index = False)

db_nucleic_acids_research = foundationsData[foundationsData['source'] == 'nucleic acids research']
db_nucleic_acids_research['authors_fullname'] = authors_nucleic_acids_research
db_nucleic_acids_research['genders'] = genders_nucleic_acids_research
db_nucleic_acids_research.to_csv('/home/bsclife018/Desktop/BSC/bibmap-master/data/db_nucleic.csv', index = False)
