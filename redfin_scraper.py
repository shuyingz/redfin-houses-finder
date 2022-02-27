#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
import urllib
from datetime import datetime
import requests
import time
import wget


# In[3]:


# params
max_price = 900000
date_time = datetime.now().strftime("%m%d%Y")
download_folder = "/Users/szhang/Downloads/"

# base url
base_url = 'https://www.redfin.com/stingray/api/gis-csv?al=1&isRentals=false&market=dc&num_homes=350&ord=redfin-recommended-asc&page_number=1&region_id={region_id}&region_type=7&sf=1,2,3,5,6,7&status=9&uipt=1,2,3,4,5,6,7,8&v=8'

# fake headers
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


# In[3]:


elementary_schools = {
    "Mosby Woods Elementary": 55716,
    "Navy Elementary School": 54146,
    "Poplar Tree Elementary": 53030,
    "Colvin Run Elementary": 51493,
    "Spring Hill Elementary": 104262,
    "Oakton Elementary": 98453,
    "Mill Run Elementary": 123504,
    "Madison's Trust Elementary": 231881,
    "Westbriar Elementary": 56040,
    "Wolftrap Elementary": 129050
}

middle_schools = {
    "Cooper Middle School": 142292, # 好
#     "Frost Middle School": 122278, # 好
    "Longfellow Middle School": 55897,
    "Carson Middle School": 90165,
    "Eagle Ridge Middle School": 116345,
    "Rocky Run Middle": 159468,
    "Franklin Middle School": 53305
}

high_schools = {
    "McLean High School": 116658,
    "Langley High School": 141517,
    "Oakton High School": 121474,
    "Briar Woods High School": 122224,
    "Chantilly High School": 139177
}


# In[ ]:





# In[4]:


def _create_folder(folder: str, file_name: str):
    path = os.path.join(folder, file_name)
    if not os.path.exists(path):
        os.mkdir(path)
    print(path)
    
# create today's folder
print("creating date folder..")
_create_folder(download_folder, date_time)
today_path = download_folder + date_time


# create elementary_school's folder
print("creating elementary_school folder..")
elementary_school = "elementary_schools"
_create_folder(today_path, elementary_school)
e_school_path = today_path + "/" + elementary_school

# create middle school's folder
print("creating middle_school folder..")
middle_school = "middle_schools"
_create_folder(today_path, middle_school)
m_school_path = today_path + "/" + middle_school


# TODO: create high school's folder
print("creating high_school folder..")
high_school = "high_schools"
_create_folder(today_path, high_school)
h_school_path = today_path + "/" + high_school


# In[5]:


def _generate_url(base_url: str, region_id: str):
    updated_url = base_url.format(region_id=region_id)
    updated_url += "&max_price=" + str(max_price)
    return updated_url

def _download_csv(url: str, path: str, school_name: str):
    file_name = school + '.csv'
    if not os.path.exists(path + "/" + school + '.csv'):
        response = requests.get(url, headers=headers)
        with open(os.path.join(path, school + '.csv'), 'wb') as f:
            f.write(response.content)
    else:
        print("File of school zone: " + school + "is exist.")
        
# download elementaryschools houses
for school in elementary_schools:
    time.sleep(2)
    url = _generate_url(base_url, elementary_schools[school])
    print(url)
    _download_csv(url, e_school_path, school)

    
# download middle schools houses
for school in middle_schools:
    time.sleep(2)
    url = _generate_url(base_url, middle_schools[school])
    print(url)
    _download_csv(url, m_school_path, school)
    
# TODO: download high schools houses
for school in high_schools:
    time.sleep(2)
    url = _generate_url(base_url, high_schools[school])
    print(url)
    _download_csv(url, h_school_path, school)


# In[6]:


import pandas as pd
import glob


# In[7]:


# elementary_schools DF
e_joined_files = os.path.join(e_school_path, "*.csv")
e_joined_list = glob.glob(e_joined_files)
# print(e_joined_list)

e_dataframes = []

for file in e_joined_list:
    df = pd.read_csv(file)
    schools_info = file.split("/")
    df['Elementary School'] = file.split("/")[-1].replace(".csv", "")
    e_dataframes.append(df)
    
e_df = pd.concat(e_dataframes)
e_df['MLS Number'] = e_df['MLS#']
e_df = e_df.set_index('MLS#')
# e_df


# In[8]:


# middle schools DF
m_joined_files = os.path.join(m_school_path, "*.csv")
m_joined_list = glob.glob(m_joined_files)
# print(m_joined_list)

m_dataframes = []

for file in m_joined_list:
    df = pd.read_csv(file)
    schools_info = file.split("/")
    df['Middle School'] = file.split("/")[-1].replace(".csv", "")
    m_dataframes.append(df)
    
m_df = pd.concat(m_dataframes)
m_df['MLS Number'] = m_df['MLS#']
m_df = m_df.set_index('MLS#')
# m_df


# In[9]:


# high schools DF
h_joined_files = os.path.join(h_school_path, "*.csv")
h_joined_list = glob.glob(h_joined_files)
# print(h_joined_list)

h_dataframes = []

for file in h_joined_list:
    df = pd.read_csv(file)
    schools_info = file.split("/")
    df['High School'] = file.split("/")[-1].replace(".csv", "")
    h_dataframes.append(df)
    
h_df = pd.concat(h_dataframes)
h_df['MLS Number'] = h_df['MLS#']
h_df = h_df.set_index('MLS#')
# h_df


# In[ ]:





# In[10]:


# merge dataframes
merged_df = e_df.merge(m_df, how='outer').merge(h_df, how='outer')

# clean up dataframes
result = merged_df.drop(columns=['SOLD DATE', 'STATE OR PROVINCE', 'LOCATION', 'NEXT OPEN HOUSE START TIME', 'NEXT OPEN HOUSE END TIME', 'SOURCE', 'FAVORITE', 'INTERESTED', 'LATITUDE', 'LONGITUDE'])
result


# In[ ]:





# In[11]:


# pre-setting:

# NaN in HOA & days on market to -1
result['HOA/MONTH'] = result['HOA/MONTH'].fillna(-1)
result['DAYS ON MARKET'] = result['DAYS ON MARKET'].fillna(-1)
# year build to 9999
result['YEAR BUILT'] = result['YEAR BUILT'].fillna(9999)

#### DO FILTERING
# 1. filter TH or SFH
# 2. filter on market > 60 days
# 3. HOA < 250

# 1. Vacant Land
# 2. Single Family Residential
# 3. Condo/Co-op
# 4. Townhouse
house_type_to_keep = ["Single Family Residential", "Townhouse"]

rslt_df = result.loc[result['PROPERTY TYPE'].isin(house_type_to_keep) & 
                    (result['HOA/MONTH'] < 200) & # HOA < 200
                    (result['DAYS ON MARKET'] < 40) & # days on market < 40
                    (result['YEAR BUILT'] > 1980)] # year build > 2000


rslt_df


# In[12]:


# sort result by year and price ASC
final = rslt_df.sort_values(by=['PRICE','YEAR BUILT'], ascending=True)
# write into result csv
final.to_csv(today_path + "/home_suggestion.csv", index=False)


# In[ ]:





# In[ ]:




