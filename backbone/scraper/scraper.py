# -*- coding: utf-8 -*-

import time, os
import random
import requests
from bs4 import BeautifulSoup
from logger_tt import getLogger
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy
from selenium.webdriver.common.proxy import ProxyType
import time
from tenacity import retry, wait_exponential, wait_random_exponential
import pandas as pd
import re

log = getLogger(__name__)

DOWNLOAD = False
SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'

decision_dict = {'generell-subvention': 'full', 'begransad-subvention': 'limited', 'avslag-och-uteslutningar': 'rejected'}
decision_name_dict = {'generell-subvention': 'generell', 'begransad-subvention': 'begransad', 'avslag-och-uteslutningar': 'avslag-och-uteslutningar'}

proxies = {
    'http': 'http://gmjorrwi:qgsmml2xrmyn@185.199.229.156:7492'
}


options = webdriver.ChromeOptions()
options.proxy = Proxy({ 'proxyType': ProxyType.MANUAL, 'httpProxy' : 'http://gmjorrwi:qgsmml2xrmyn@185.199.229.156:7492'})
options.browser_version = 'stable'
options.page_load_strategy = 'normal'
options.timeouts = { 'script': 5000 }
options.timeouts = { 'pageLoad': 5000 }
options.timeouts = { 'implicit': 5000 }
options.platform_name = 'any'
options.accept_insecure_certs = True
options.add_argument("--headless")
#service = webdriver.ChromeService(executable_path=SAVE_PATH+
#                                  '/chromedriver-win64/chromedriver.exe')
service = webdriver.ChromeService(executable_path=SAVE_PATH+
                                  '/chromedriver_134/chromedriver.exe')
driver = webdriver.Chrome(service=service, options=options)

BASE_URL = "https://www.tlv.se"
generell_URL = "https://www.tlv.se/beslut/beslut-lakemedel/generell-subvention.html?start=20230101-00000000-AM&end=20231231-235959999-PM"

headers={
    'User-Agent':
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'
}

@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def get_html(url): 
    response = requests.get(url, headers=headers, proxies=proxies) 
    response.encoding = 'utf-8'
    return BeautifulSoup(response.text, 'html.parser')

@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def get_file(link, file_name):
    response = requests.get(link, headers=headers, proxies=proxies)   
    # Write content in pdf file
    pdf = open(file_name, 'wb')
    pdf.write(response.content)
    pdf.close()
    log.info("File downloaded: " + file_name)

@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def get_LMV_data():
    log.info('Scrsping LMV')
    base_url = 'https://www.lakemedelsverket.se'
    #content > div > ng-component > div > div.lmf-list-page__main.ng-star-inserted > div.lmf-list-page__main__form > div.lmf-list-page__main__form__content > xhtmlstring > div > div:nth-child(31) > epi-property > link-list-block > div > ul > li > link-item > a > div > div.link-list-block__list__item__anchor__text
    url = 'https://www.lakemedelsverket.se/sv/sok-lakemedelsfakta?activeTab=3'
    # Make a request to the website
    driver.get(url)
    # Get the page source after the JavaScript has executed 
    time.sleep(1)
    page_source = driver.page_source 

    # Use BeautifulSoup to parse the HTML 
    soup = BeautifulSoup(page_source, 'html.parser') 
    log.info('Got the page')
    driver.quit()

    link_element = soup.find('a',href=re.compile(r'(.*.xlsx)'))#"/4aacba/globalassets/excel/Lakemedelsprodukter.xlsx")
    doc_link = link_element['href']
    # Get the url
    get_file(base_url + doc_link, 'LMV.xlsx')