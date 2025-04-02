
import time, os
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from logger_tt import getLogger
import gzip
import re
import xml.etree.ElementTree as ET
from tenacity import retry, wait_exponential, wait_random_exponential
import pandas as pd
import json

SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'

def get_nt_products():
    df_rec = pd.read_excel(SAVE_PATH + 'nt_radet_rek_202503.xlsx')
    return list(df_rec['Produktnamn'])

DOWNLOAD = False

log = getLogger(__name__)

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

@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def get_html(url): 
    response = requests.get(url, headers=headers, proxies=proxies) 
    response.encoding = 'utf-8'
    return BeautifulSoup(response.text, 'html.parser')

@retry(wait=wait_random_exponential(multiplier=1, max=60))
def download_file(link, file_name):
    response = requests.get(link, headers=headers, proxies=proxies)   
    # Write content in pdf file
    pdf = open(file_name, 'wb')
    pdf.write(response.content)
    pdf.close()
    log.info("File downloaded: " + file_name)

# -----------------------
# TLV
#------------------------
def get_files(link_data, temp_dir):
    decision_soup = get_html(link_data['link'])
    # In some old cases there is no document!
    doc_list = decision_soup.find('div', 
                                    {"id":"Relateradinformation"}).parent.find('ul')
    if not doc_list:
        return False
    
    document_links = doc_list.find_all('li')
    number_of_docs = len(document_links)

    for k in range(0, number_of_docs):
        product_name = "".join(x for x in link_data['product'] if x.isalnum())
        file_name = product_name + ' ' + link_data['date'] + ' ' + str(k) + '_' + link_data['decision'] + ".pdf"
        log.info('Downloading ' + file_name)
        time.sleep(1 + random.random())
        download_file(BASE_URL + document_links[k].find('a')['href'], os.path.join(temp_dir, file_name))

    return True

def get_pharma_reimbursement_links():
    # Goes through the TLV website and gathers links to all reimbursement dossier pages
    links = []
    
    for decision_type in ['generell-subvention','begransad-subvention','avslag-och-uteslutningar']:
        for year in range(2010,2025):
            log.info(year)
            log.info(decision_type)
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
                links.append({'link': BASE_URL + partial_link, 'product': product_name, 'date': date, 'decision': decision_type_name})
    return links


def get_nt_assessment_links():
    # These documents are not collected in the same manner. We have to resort to the sitemap
    
    # get sitemap
    URL_SITE_MAP = 'https://www.tlv.se/sitemap1.xml.gz'
    response = requests.get(URL_SITE_MAP, headers=headers, proxies=proxies)  
    with open(SAVE_PATH+'sitemap.xml.gz', 'wb') as f:
        f.write(response.content)

    with gzip.open(SAVE_PATH+'sitemap.xml.gz', 'rb') as f:
        file_content = f.read()
        with open(SAVE_PATH+'sitemap.xml', 'wb') as f:
            f.write(file_content)

    tree = ET.parse(SAVE_PATH+'sitemap.xml')
    root = tree.getroot()
    root_str = ET.tostring(root, encoding="unicode")
    # get the links to the evaluation pages 
    raw_links = re.findall('https.*arkiv-avslutade-halso.*html', root_str)

    df_rec = get_NT_deals_df()
    prods = list(df_rec['product'])
    raw_links_2 = [[x for x in re.findall('https.*' + li + '.*html', root_str, flags=re.I)] for li in prods]
    raw_links_2 = [x for y in raw_links_2 for x in y]
    resulting_list = list(raw_links)
    resulting_list.extend(x for x in raw_links_2 if x not in raw_links)

    links = []
    for link in resulting_list:
        m = re.match('\d{4}-\d{2}-\d{2}', link)
        if m:
            datum = m[0]
        else:
            datum = 'no date'

        links.append({'link': link, 'product': 'For NT', 'date': datum, 'decision': 'no decision'})

    return links

# -----------------------
# NT
#------------------------
def get_NT_recommendation_links():
    # Goes through the TLV website and gathers links to all reimbursement dossier pages
    #selector = "#scroll2 > table > tbody"
    URL = "https://samverkanlakemedel.se/lakemedel---ordnat-inforande/nt-radets-rekommendationer"

    soup = get_html(URL) 
    #all_decisions = soup.select(selector)
    soup_str= str(soup)
    _list = re.findall(r'\[\{\"prod.*\}\]',soup_str)
    nt_rec = json.loads(_list[0])
    
    links = []
    for link in nt_rec:
        links.append({'link': link['url'], 'product': link['productName'], 'date': link['publishDate'], 'decision': link['recommendation']})

    return links

def get_NT_follow_up_links():
    URL = 'https://samverkanlakemedel.se/lakemedel---ordnat-inforande/uppfoljningsrapporter'
    soup = get_html(URL) 
    #all_decisions = soup.select(selector)
    soup_str= str(soup)
    m = re.findall(r'\[\{\"prod.*\}\]',soup_str)
    json.loads(m[0])
    follow_ups = json.loads(m[0])
    links = []
    for link in follow_ups:
        links.append({'link': link['url'], 'product': link['productName'], 'date': link['publishDate'], 'decision': 'no decision', 'document_type': 'NT follow-up report'})

    return links

def get_NT_archieved_decision_links():
    # TODO Bring these in. How to ahdnel the fact that they are no longer valid? An additional flag variable: status (current, archieved) in the table? No, 'decision': decision + ' archieved'
    pass

def get_NT_early_assessment_links():

    URL = "https://samverkanlakemedel.se/lakemedel---ordnat-inforande/nt-radets-rekommendationer"
    # TODO: Extract and link these reports to a medicinal product or ATC? They are using the atc code
    # Can use the same approach as above
    pass

def get_NT_deals_df():
    URL = 'https://samverkanlakemedel.se/lakemedel---ordnat-inforande/avtal'
    soup = get_html(URL) 
    soup_str= str(soup)
    _list = re.findall(r'\[\{\"id.*\}\]',soup_str)
    m=re.findall(r'\[\{\"id.*\}\]',soup_str) # prod
    json.loads(m[0])
    deals = json.loads(m[0])
    df_deals = pd.DataFrame(deals)
    df_deals = df_deals.rename(columns={'atc_code':'ATC', 'deal_part':'company', 'recipe_or_requisition':'recipe_type', 'deal_start':'start', 'deal_end': 'end', 'longer_option_to': 'option'})
    df_deals = df_deals.drop(columns={'id','name', 'url','substance'})
    return df_deals


def get_NT_recommendation_df():
    URL = "https://samverkanlakemedel.se/lakemedel---ordnat-inforande/nt-radets-rekommendationer"

    soup = get_html(URL) 
    #all_decisions = soup.select(selector)
    soup_str= str(soup)
    _list = re.findall(r'\[\{\"prod.*\}\]',soup_str)
    nt_decisions = json.loads(_list[0])
    df_rec = pd.DataFrame(nt_decisions)
    df_rec = df_rec.rename(columns={'productName':'product', 'substances':'active_drug', 'atc':'ATC','url': 'URL', 'publishDate':'date'})
    return df_rec

def get_NT_no_assessment_df():
    URL = "https://samverkanlakemedel.se/lakemedel---ordnat-inforande/lakemedel-som-inte-ska-samverkas-nationellt"
    soup = get_html(URL) 
    table = soup.find_all('table')
    df_no_ass = pd.read_html(str(table))[0]
    df_no_ass = df_no_ass.rename(columns={'Läkemedel':'product', 'Substans':'active_drug', 'Indikation':'indication','Motivering':'reason', 'Datum':'date'})
    return df_no_ass