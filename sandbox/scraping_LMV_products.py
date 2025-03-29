# -*- coding: utf-8 -*-
"""
Created on Wed Mar 13 09:32:36 2024

@author: Fredrik
"""
import requests
from bs4 import BeautifulSoup
import csv
import string
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy
from selenium.webdriver.common.proxy import ProxyType


options = webdriver.ChromeOptions()

options.proxy = Proxy({ 'proxyType': ProxyType.MANUAL, 'httpProxy' : 'http://gmjorrwi:qgsmml2xrmyn@185.199.229.156:7492'})
driver = webdriver.Chrome(options=options)
proxies = {
    'http': 'http://gmjorrwi:qgsmml2xrmyn@185.199.229.156:7492'
}


def get_product_info(row):
    url = row['ProduktLänk']
    # Make a request to the website
    response = requests.get(url, proxies=proxies)
    response.encoding = 'utf-8'
    driver.get(url)
    # Get the page source after the JavaScript has executed 
    page_source = driver.page_source 
 
    # Use BeautifulSoup to parse the HTML 
    soup = BeautifulSoup(page_source, 'html.parser') 

    # Find all company elements based on the provided CSS selector
    row['ATC'] = soup.select('#content > div > ng-component > div > div.lmf-details-page__main > div.lmf-details-page__main__information > div:nth-child(2) > div')
    row['approval_date'] = soup.select('#content > div > ng-component > div > div.lmf-details-page__main > accordion-item:nth-child(5) > div > div.accordion-item__content.html > div:nth-child(5) > div > lmf-label > div')
    row['active_drug'] = soup.select('#content > div > ng-component > div > div.lmf-details-page__main > accordion-item:nth-child(6) > div > div.accordion-item__content.html > div > table > tbody > tr:nth-child(1) > td > div > div > ul > li:nth-child(1) > a')

    driver.quit()

    return row

