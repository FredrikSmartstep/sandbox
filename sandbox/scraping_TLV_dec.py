# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 16:26:36 2024

@author: Fredrik
"""
import time, os
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from parse_file import parse_file
from parse_decision_file import parse_decision_file
import logging

DOWNLOAD = False

logger = logging.getLogger(__name__)

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
    logger.info("File downloaded: " + product_name)

if __name__ == '__main__':
    logging.basicConfig(filename='scraping_TLV.log', level=logging.DEBUG)
    logger.info('Started')
    
    df_info_tot = pd.DataFrame(columns={'drug_name':str,'ATC':str,'company':str,'indication':str,
                                       'decision_date':str,'comparators':str,'application_type':str,
                                       'experts':str, 'diarie_nr':str})
    df_work_tot = pd.DataFrame()
    df_info_dec_tot = pd.DataFrame()
    could_not_read = []
    df_not_read = pd.DataFrame(columns={'file':str, 'reason':str})
    for decision_type in ['generell-subvention','begransad-subvention','avslag-och-uteslutningar']:
        for year in range(2010,2025):
            logger.info(year)
            logger.info(decision_type)
            #decision_type = 'avslag-och-uteslutningar' # begransad-subvention generell-subvention avslag-och-uteslutningar
            decision_type_name = decision_name_dict[decision_type]
            URL = "https://www.tlv.se/beslut/beslut-lakemedel/{}.html?start={}0101-00000000-AM&end={}1231-235959999-PM".format(decision_type, year, year)
            #begransad_URL = "https://www.tlv.se/beslut/beslut-lakemedel/begransad-subvention.html?start={}0101-00000000-AM&end={}1231-235959999-PM".format(year, year)
            
            soup = get_html(URL) 
            all_decisions = soup.select('ul')
            li = all_decisions[3].find_all('li')
        
            for company in li:
                product_name = company.find('a').text.strip().split()[0]
                date = ' '.join(company.find('a').find('div',class_="sol-article-item__date").text.strip().split()[1:])
                #svid12_113dd8531817fd054369feaf > ul > li:nth-child(1) > a > div.sol-article-item__date
                partial_link = company.find('a')['href']
                link = BASE_URL + partial_link
                number_of_docs = 4
                if DOWNLOAD:
                    decision_soup = get_html(link)
                    document_links = decision_soup.find('div', 
                                                    {"id":"Relateradinformation"}).parent.find('ul').find_all('li')
                    number_of_docs = len(document_links)
                
                for k in range(0, number_of_docs):
                    product_name = "".join(x for x in product_name if x.isalnum())
                    file_name = product_name + ' ' + date + ' ' + str(k) + '_' + decision_type_name + ".pdf"
                    logger.info(file_name)
                    file_exists = os.path.exists('./' + file_name)
                    
                    if file_exists:
                        logger.info('Found file: ' + product_name)
                    else:
                        if DOWNLOAD:
                            time.sleep(1 + random.random())
                            get_file(BASE_URL + document_links[k].find('a')['href'], file_name)
                        else:
                            break

                    # Any document may be a decision
                    df_inform, reason = parse_decision_file(file_name, product_name)
                    if df_inform.empty:
                        could_not_read.append(file_name)
                        df_not_read = pd.concat([df_not_read,pd.DataFrame({'file':file_name, 'reason':reason}, index=[0])], ignore_index=True)
                    else:
                        df_inform['decision'] = decision_dict[decision_type]
                        df_inform['document_type'] = 'decision'
                        df_info_dec_tot = pd.concat([df_info_dec_tot, df_inform], ignore_index=True)

                    if k>0:
                        df_information, df_work = parse_file(file_name)
                        if df_information.empty:
                            could_not_read.append(file_name)
                        else:
                            df_information['decision'] = decision_dict[decision_type]
                            df_information['document_type'] = 'basis'
                            df_info_tot = pd.concat([df_info_tot, df_information], ignore_index=True)
                            df_work_tot = pd.concat([df_work_tot, df_work], ignore_index=True)

    # TODO: Format to lower cse. replce /n with ' '
    df_not_read.to_csv("./not_read.csv", sep=';', index=False)  
    df_info_tot.to_csv('./files_{}.csv'.format(decision_type_name), sep=';', encoding='utf-8-sig')
    df_work_tot.to_csv('./work_tot_{}.csv'.format(decision_type_name), sep=';', encoding='utf-8-sig')
    #df_info_dec_tot.to_csv('./files_decision_{}.csv'.format(decision_type_name), sep=';', encoding='utf-8-sig')
    df_info_tot.to_csv('./files.csv', sep=';', encoding='utf-8-sig')
    df_work_tot.to_csv('./work_tot.csv', sep=';', encoding='utf-8-sig')
    df_info_dec_tot.to_csv('./files_decision.csv', sep=';', encoding='utf-8-sig')

    logger.info('Finished')

            #svid10_2f080b7e182629be22d8893
            #svid10_2d24f53418120eae3ff5fbf0 > div
            #svid10_2d24f53418120eae3ff5fbf0 > div > div
            #svid12_2d24f53418120eae3ff5fc29 > ul > li:nth-child(2) > a
            
        
        
    #svid12_113dd8531817fd054369feaf > ul > li:nth-child(5) > a > div.sol-article-item__content > span.sol-article-item__content__heading
    #svid12_113dd8531817fd054369feaf > ul > li:nth-child(2) > a > div.sol-article-item__content > span.sol-article-item__content__heading
    #'#svid12_113dd8531817fd054369feaf > ul > li:nth-child(3) > a > div.sol-article-item__content > span.sol-article-item__content__heading'