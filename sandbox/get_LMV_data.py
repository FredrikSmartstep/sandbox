import requests
from bs4 import BeautifulSoup
import csv
import string
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy
from selenium.webdriver.common.proxy import ProxyType
import time

import pandas as pd
import re
SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'
options = webdriver.ChromeOptions()
options.proxy = Proxy({ 'proxyType': ProxyType.MANUAL, 'httpProxy' : 'http://gmjorrwi:qgsmml2xrmyn@185.199.229.156:7492'})
options.browser_version = 'stable'
options.page_load_strategy = 'normal'
options.timeouts = { 'script': 5000 }
options.timeouts = { 'pageLoad': 5000 }
options.timeouts = { 'implicit': 5000 }
options.platform_name = 'any'
options.accept_insecure_certs = True
service = webdriver.ChromeService(executable_path=SAVE_PATH+
                                  '/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=service, options=options)


df_prod_lmv = pd.read_xml(SAVE_PATH + 'Lakemedelsfakta_produktdokument_1_1.xml')
df_prod_lmv = df_prod_lmv[['ProduktNamn','Företag','ProduktLänk']]
df_prod_lmv = df_prod_lmv.dropna()
df = pd.DataFrame(columns=['product', 'company', 'ATC', 'active_drug', 'approval_date'])
for ind, row in df_prod_lmv.iterrows():
    row['product'] = row['ProduktNamn']
    row['company'] = row['Företag']
    url = row['ProduktLänk']
    # Make a request to the website
    driver.get(url)
    # Get the page source after the JavaScript has executed 
    time.sleep(1)
    page_source = driver.page_source 
 
    # Use BeautifulSoup to parse the HTML 
    soup = BeautifulSoup(page_source, 'html.parser') 
    # Find all company elements based on the provided CSS selector
    try:
        row['ATC'] = soup.select('#content > div > ng-component > div > div.lmf-details-page__main > div.lmf-details-page__main__information > div:nth-child(2) > div')[0].text
    except Exception as e:
        row['ATC'] = 'not found'
    #content > div > ng-component > div > div.lmf-details-page__main > div.lmf-details-page__main__information > div:nth-child(2) > div
    #row['approval_date'] = soup.select('#content > div > ng-component > div > div.lmf-details-page__main > accordion-item:nth-child(5) > div > div.accordion-item__content.html > div:nth-child(5) > div > lmf-label > div')[0].text
    #soup.select('#content > div > ng-component > div > div.lmf-details-page__main > accordion-item:nth-child(5) > div > div.accordion-item__content.html > div:nth-child(11) > div > lmf-label')
    try: 
        row['active_drug'] = soup.select('#content > div > ng-component > div > div.lmf-details-page__main > accordion-item:nth-child(6) > div > div.accordion-item__content.html > div > table > tbody > tr:nth-child(1) > td > div > div > ul > li:nth-child(1) > a')[0].text
    except Exception as e:
        row['active_drug'] = 'not found'
    df.loc[ind] = row[['product', 'company', 'ATC', 'active_drug']]

driver.quit()