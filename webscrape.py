#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 14:26:30 2020

@author: Jonathan Seward

Program: This is a scraper to pull misdemeanor data from Travis County Clerk's 
    office website, and shows two methods of pulling the data from the websites.
    It pulls three types of web pages: 
        1) The summary of defendants by date, 
        2) the defendant's details, and 
        3) the defendant's disposition details. 
    Finally, the program outputs the three datasets to csv files.
"""


import requests
import lxml.html as lh
import time
from bs4 import BeautifulSoup
#import numpy as np

import pandas as pd

from datetime import timedelta, date

# Lets us loop through dates
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

###############################################################################
#  Input filepaths
###############################################################################
main_file = 'misdemeanors_main.csv'
detail_file = 'misdemeanors_detail.csv'
disp_file = 'misdemeanors_disp.csv'


###############################################################################
#  Gets the defendants' summaries
###############################################################################
start_date = date(2016, 1, 1)
end_date = date(2019, 1, 1)

main_df = pd.DataFrame()

for single_date in daterange(start_date, end_date):
    date_str = single_date.strftime("%Y-%m-%d")

    print(date_str)

    URL_sum = 'https://countyclerk.traviscountytx.gov/component/chronoconnectivity6/?cont=manager&conn=misdemeanor-data&calendar_misdemeanor_start='+ date_str+'&calendar_misdemeanor_end='+date_str+'&event=index'
    
    #Create a handle, page, to handle the contents of the website
    page = requests.get(URL_sum)
    #Store the contents of the website under doc
    doc = lh.fromstring(page.content)
    #Parse data that are stored between <tr>..</tr> of HTML
    tr_elements = doc.xpath('//tr')
    print([len(T) for T in tr_elements[:12]])
    
    #Create empty list
    col=[]
    i=0
    #For each row, store each first element (header) and an empty list
    for t in tr_elements[2]:
        i+=1
        header=t.text_content()
        print(header)
        col.append((header,[]))
    
    # Since first 2 rows are not table and third row is the header, 
    #   data is stored on the second row onwards
    for j in range(3,len(tr_elements)):
        #T is our j'th row
        T=tr_elements[j]
        
        #If row is not of size 8, the //tr data is not from our table 
        if len(T)!=8:
            break
        
        #i is the index of our column
        i=0
        
        #Iterate through each element of the row
        for t in T.iterchildren():
            data=t.text_content() 
            #Check if row is empty
            if i>0:
            #Convert any numerical value to integers
                try:
                    data=int(data)
                except:
                    pass
            #Append the data to the empty list of the i'th column
            col[i][1].append(data)
            #Increment i for the next column
            i+=1
    
    [len(C) for (title,C) in col]
    
    Dict={title:column for (title,column) in col}
    df=pd.DataFrame(Dict)
    
    # append to main dataframe
    main_df = main_df.append(df)
    
    # hopefully prevents us from looking like spam
    time.sleep(1)

main_df.rename(columns=lambda x: x.strip(), inplace = True)
main_df.rename(columns={ main_df.columns[0]: "Cause No" }, inplace = True)

for column in main_df:
    print(column)
    main_df[column] = main_df[column].astype(str).map(lambda x: x.strip('\n'))
    main_df[column] = main_df[column].astype(str).map(lambda x: x.strip('\t'))
    
# save to csv
main_df.to_csv(main_file, index=False)

###############################################################################
#  Gets defendant details
#   The idea is to loop through the Cause Nos pulled from the scrape of the
#   defendants' summaries by date.
###############################################################################
main_df = pd.read_csv(main_file)
# initialize blank lists to add data to data frame later
cause_list = []
id_list = []
name_list = []
race_list = []
gender_list = []
ethnicity_list = []
attorney_list = []
court_list = []

for index, row in main_df.iterrows():
    cause = row['Cause No']

    URL_details = 'https://countyclerk.traviscountytx.gov/component/chronoconnectivity6/?cont=manager&conn=misdemeanor-data&event=view&cause_number='+cause
    response_details = requests.get(URL_details)
    
    soup_details = BeautifulSoup(response_details.text, "html.parser")
    def_details = soup_details.findAll('td')
    
    # This can pull the headers for the data
    header = soup_details.findAll(class_ = 'ui header')
    
    # if def_details block is the correct def_detailsblock, then proceed
    if(str(def_details[1]) == str('<td class="collapsing right aligned"><h4 class="ui header">Cause No</h4></td>')):
        cause_list.append(def_details[2])
    else:
        cause_list.append('')
        
    if(str(def_details[3]) == str('<td class="collapsing right aligned"><h4 class="ui header">Participant ID</h4></td>')):
        id_list.append(def_details[4])
    else:
        id_list.append('')
        
    if(str(def_details[5]) == str('<td class="collapsing right aligned"><h4 class="ui header">Full Name</h4></td>')):
        name_list.append(def_details[6])
    else:
        name_list.append('')
        
    if(str(def_details[7]) == str('<td class="collapsing right aligned"><h4 class="ui header">Race</h4></td>')):
        race_list.append(def_details[8])
    else:
        race_list.append('')
        
    if(str(def_details[9]) == str('<td class="collapsing right aligned"><h4 class="ui header">Gender</h4></td>')):
        gender_list.append(def_details[10])
    else:
        gender_list.append('')
        
    if(str(def_details[11]) == str('<td class="collapsing right aligned"><h4 class="ui header">Ethnicity</h4></td>')):
        ethnicity_list.append(def_details[12])
    else:
        ethnicity_list.append('')
        
    if(str(def_details[13]) == str('<td class="collapsing right aligned"><h4 class="ui header">Attorney Name</h4></td>')):
        attorney_list.append(def_details[14])
    else:
        attorney_list.append('')
        
    if(str(def_details[15]) == str('<td class="collapsing right aligned"><h4 class="ui header">Court Assignment</h4></td>')):
        court_list.append(def_details[16])
    else:
        court_list.append('')
    print(cause)

df_detail = pd.DataFrame(columns = ['Cause No', 'Participant ID', 'Full Name', 'Race', 'Gender', 'Ethnicity', 'Attorney Name', 'Court Assignment'])  
    
df_detail['Cause No'] = cause_list
df_detail['Participant ID'] = id_list
df_detail['Full Name'] = name_list
df_detail['Race'] = race_list
df_detail['Gender'] = gender_list
df_detail['Ethnicity'] = ethnicity_list
df_detail['Attorney Name'] = attorney_list
df_detail['Court Assignment'] = court_list

for column in df_detail:
    df_detail[column] = df_detail[column].astype(str).map(lambda x: x.strip('\n'))
    df_detail[column] = df_detail[column].astype(str).map(lambda x: x.strip('\t'))

# save to csv
df_detail.to_csv(detail_file, index=False)


###############################################################################
#  Gets the defendants' disposition details
###############################################################################
df_disp = pd.DataFrame()

for index, row in main_df.iterrows():
    cause = row['Cause No']
    print(cause)
    URL_disp = 'https://countyclerk.traviscountytx.gov/component/chronoconnectivity6/?cont=manager&conn=misdemeanor-data&event=disp&cause_number='+cause
    
    #Create a handle, page, to handle the contents of the website
    page = requests.get(URL_disp)
    #Store the contents of the website under doc
    doc = lh.fromstring(page.content)
    #Parse data that are stored between <tr>..</tr> of HTML
    tr_elements = doc.xpath('//tr')
    # Keeps only the disposition data
    tr_elements = [T for T in tr_elements if len(T) == 3]
    [len(T) for T in tr_elements[:12]]
    #Create empty list
    col=[]
    i=0
    #For each row, store each first element (header) and an empty list
    for t in tr_elements[0]:
        i+=1
        header=t.text_content()
        #print(header)
        col.append((header,[]))
    
    # Since first row is the header, 
    #   data is stored on the second row onwards
    for j in range(1,len(tr_elements)):
        #T is our j'th row
        T=tr_elements[j]
        
        #If row is not of size 3, the //tr data is not from our table 
        if len(T)!=3:
            break
        
        #i is the index of our column
        i=0
        
        #Iterate through each element of the row
        for t in T.iterchildren():
            data=t.text_content() 
            #Check if row is empty
            if i>0:
            #Convert any numerical value to integers
                try:
                    data=int(data)
                except:
                    pass
            #Append the data to the empty list of the i'th column
            col[i][1].append(data)
            #Increment i for the next column
            i+=1
    
    [len(C) for (title,C) in col]
    
    Dict={title:column for (title,column) in col}
    df=pd.DataFrame(Dict)
    df['Cause No'] = cause
    df_disp = df_disp.append(df)
    # hopefully prevents us from looking like spam
    #time.sleep(1)

for column in df_disp:
    df_disp[column] = df_disp[column].astype(str).map(lambda x: x.strip('\n'))
    df_disp[column] = df_disp[column].astype(str).map(lambda x: x.strip('\t'))
    
# save to csv
df_disp.to_csv(disp_file, index=False)

