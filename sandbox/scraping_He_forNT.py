import time, os
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from parse_nt_basis import parse_file
import xml.etree.ElementTree as ET
import re


decision_dict = {'generell-subvention': 'full', 'begransad-subvention': 'limited', 'avslag-och-uteslutningar': 'rejected'}
decision_name_dict = {'generell-subvention': 'generell', 'begransad-subvention': 'begransad', 'avslag-och-uteslutningar': 'avslag-och-uteslutningar'}

proxies = {
    'http': 'http://gmjorrwi:qgsmml2xrmyn@185.199.229.156:7492'
}

BASE_URL = "https://www.tlv.se"
generell_URL = "https://www.tlv.se/beslut/beslut-lakemedel/generell-subvention.html?start=20230101-00000000-AM&end=20231231-235959999-PM"

headers={
    'User-Agent':
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'
}

def extract_urls_from_file_with_namespace(file_path):
    # Load and parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    # Define the namespace to find the tags correctly
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
    # List to hold URLs
    urls = []
    file_names = []
    # Find all <loc> tags within <url> tags, considering the namespace
    for url in root.findall('ns:url/ns:loc', namespace):
        patterns = r'/bes[_]?\d{6}_under.*\.pdf|/bed.*'
        if bool(re.search(patterns, url.text, re.I)):
            urls.append(url.text)
            file_names.append(url.text.split('/')[-1])
    return urls, file_names

def get_html(url): 
    response = requests.get(url, headers=headers, proxies=proxies) 
    response.encoding = 'utf-8'
    return BeautifulSoup(response.text, 'html.parser')

def get_file(link, file_name):
    response = requests.get(link, headers=headers, proxies=proxies)   
    # Write content in pdf file
    pdf = open(file_name, 'wb')
    pdf.write(response.content)
    pdf.close()
    print("File downloaded: " + file_name)

if __name__ == '__main__':
    df_info_tot = pd.DataFrame(columns={'drug_name':str,'ATC':str,'company':str,'indication':str,
                                       'decision_date':str,'comparators':str,'application_type':str,
                                       'experts':str, 'diarie_nr':str})
    df_work_tot = pd.DataFrame()
    df_info_tot = pd.DataFrame()

    file_path = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/sitemap1.xml'  # Replace with your actual file path

    # Extract URLs from the XML file considering the namespace
    urls, file_names = extract_urls_from_file_with_namespace(file_path)
    could_not_read = []
    for url, file_name in zip(urls,file_names):
        file_exists = os.path.exists('./' + file_name)
                    
        if file_exists:
            print('Found file: ' + file_name)
        else:
            time.sleep(1 + random.random())
            get_file(url, file_name)
            
        df_information, df_work = parse_file(file_name)
        if df_information.empty:
            could_not_read.append(file_name)
        else:
            df_information['decision'] = 'No decision'
            df_information['document_type'] = 'basis for NT'
            df_info_tot = pd.concat([df_info_tot, df_information], ignore_index=True)
            df_work_tot = pd.concat([df_work_tot, df_work], ignore_index=True)
    
    df_not_read = pd.DataFrame(data={"files": could_not_read})
    df_not_read.to_csv("./not_read_for_nt.csv", sep=';', index=False)  

    df_info_tot.to_csv('./files_for_nt.csv', sep=';', encoding='utf-8-sig')
    df_work_tot.to_csv('./work_tot_for_nt.csv', sep=';', encoding='utf-8-sig')
