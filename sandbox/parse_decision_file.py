# -*- coding: utf-8 -*-

import fitz
import pandas as pd
import re
import json
import time
import openai
from openai import OpenAI

import logging
logger = logging.getLogger(__name__)

from sandbox.scraping_methods import get_text_3, get_text_2, get_date, get_line, get_next_line, \
get_clean_block_list, get_drug_from_table, get_info_from_table, extract_sentences_with_word, get_next, get_blocks_in_between

def parse_decision_file(file, product_name):
    doc = fitz.open(file) 
    logger.info('parsing :' + file)
    # Page 0
    page = doc[0]  
    try:
        words = page.get_text('words', sort=True)
        text = page.get_text('text')
        blocks = page.get_text('blocks')
        df_words = pd.DataFrame(words)
        df_blocks = pd.DataFrame(blocks)
        block_list = list(df_blocks.iloc[:,4])
        # Decision?
        word_list = list(df_words.iloc[:,4])
        fin = 'FINOSE' in word_list
        decision_file = 'BESLUT' in word_list[0:25] or 'SÖKANDE' in word_list[0:25] or 'PART' in word_list[0:25] or 'FÖRETAG' in word_list[0:25]
        basis = not decision_file
        if not basis: # Let's make sure by testing also:
            basis = ('förslag' in word_list) or ('Förslag' in word_list) or ('FÖRSLAG' in word_list)
        if basis or fin:
            logger.info('Not a decision document')
            return pd.DataFrame(), 'Not a decision document'
        # Get decision date
        decision_date = get_date('Datum', block_list).strip()
        # get diarie
        diarie_nr = get_next('beteckning', block_list, r'(\d*[\/]\d*)').strip()
        index = [idx for idx, s in enumerate(block_list) if 'beteckning' in s]

        if not diarie_nr:
            logger.info('index: ' + str(len(index)))
            diarie_nr = get_text_2('beteckning', df_blocks)
        if not diarie_nr:
            logger.info('still: ' + diarie_nr)
            diarie_nr = get_next('till beslut', block_list, r'(\d*[\/]\d*)').strip()
        # get company
        company = get_line('SÖKANDE', df_blocks).strip()
        if len(company)<3:
            company = get_line('FÖRETAG',df_blocks)
        if len(company)<3:
            company = get_next_line('SÖKANDE',block_list).strip()
        # get limitation
        limitations = get_text_2('Begränsningar', df_blocks)
        # get drug
        drug_name = None
        forms = None
        strengths = None
        tab = page.find_tables()
        if tab.tables:
            drug_name = get_drug_from_table(tab)   
            forms = get_info_from_table(tab,'Form')
            strengths = get_info_from_table(tab,'Styrka')                   
        if not drug_name: # moved to 
            page = doc[1]
            tab = page.find_tables()
            if tab.tables:
                drug_name = get_drug_from_table(tab)  
                forms = get_info_from_table(tab,'Form')
                strengths = get_info_from_table(tab,'Styrka')   
        if not drug_name: # some old decisions lack table
            drug_name = product_name
    except Exception as e: # This may happen if, e.g., the document has been created by scanning
        logger.error(e, exc_info=True)
        logging.critical(msg='Could not read the document.', exc_info=True) # This may happen if, e.g., the document has been created by scanning
        return pd.DataFrame(), 'Could not read the document'
    
    logger.info(diarie_nr)
    try:
        block_list = get_clean_block_list(doc)
        application_text = get_blocks_in_between(r'^Ansökan', r'^Utredning', block_list, get_last=False)
        application_type = 'new drug'
        pattern = r'ingår sedan|finns'
        resubmission = bool(len(re.findall(pattern, application_text, re.IGNORECASE)))
        pattern = r'ändring'
        changed_decision = bool(len(re.findall(pattern, application_text, re.IGNORECASE)))
        pattern = r'ny[a]? indikation'
        new_indication = bool(len(re.findall(pattern, application_text, re.IGNORECASE)))
        pattern = r'ny[a]? [bered|styrk]'
        new_form = bool(len(re.findall(pattern, application_text, re.IGNORECASE)))
        pattern = r'tillfäll'
        temporary = bool(len(re.findall(pattern, application_text, re.IGNORECASE)))
        pattern = r'utgå'
        removed = bool(len(re.findall(pattern, application_text, re.IGNORECASE)))

        total_text = chr(12).join([page.get_text() for page in doc])
        #b_clean_list = get_clean_block_list(doc)
        #total_text = chr(12).join([c.replace('\n','') for c in b_clean_list])
        decision_text = ''
        pattern = r"{}(.*)".format('TLV gör följande bedömning') 
        if bool(re.search(pattern, total_text, flags=re.I|re.DOTALL)):
            decision_text = re.findall(pattern, total_text, flags=re.DOTALL)[0].strip() 
        if decision_text=='':
            pattern = r"{}(.*)".format('TLV bedömer') 
            if bool(re.search(pattern, total_text, flags=re.I|re.DOTALL)):
                decision_text = re.findall(pattern, total_text, flags=re.DOTALL)[0].strip()
        if decision_text=='':
            pattern = r"{}(.*)".format('TLV gör nu bedömningen') 
            if bool(re.search(pattern, total_text, flags=re.I|re.DOTALL)):
                decision_text = re.findall(pattern, total_text, flags=re.DOTALL)[0].strip()
        if decision_text=='':
            pattern = r"{}(.*)".format('TLV:s bedömning') 
            if bool(re.search(pattern, total_text, flags=re.I|re.DOTALL)):
                decision_text = re.findall(pattern, total_text, flags=re.DOTALL)[0].strip()
        
        pattern = r"(.*){}".format('hur man överklagar') # This may be missing (ex Palexia_2 2014)
        if bool(re.search(pattern, decision_text, flags=re.I|re.DOTALL)):
            decision_text = re.findall(pattern, decision_text, flags=re.I|re.DOTALL)[0].strip() 
        pattern = r"(.*){}".format('Tillämpliga bestämmelser') 
        if bool(re.search(pattern, decision_text, flags=re.I|re.DOTALL)):
            decision_text = re.findall(pattern, decision_text, flags=re.I|re.DOTALL)[0].strip() 
        
        #decision_text = get_blocks_in_between(r'TLV gör följande bedömning', r'Se nedan hur man överklagar', b_clean_list, get_last=False)
        
        comparator_texts = extract_sentences_with_word(decision_text, 'jämförelsealt')
        comparator = chr(12).join([c.replace('\n','') for c in comparator_texts])

        severity_texts = extract_sentences_with_word(total_text, 'svårighetsgrad')
        severity = chr(12).join([c.replace('\n','') for c in severity_texts])


        indication_texts = extract_sentences_with_word(total_text, 'avsett för ')
        if not indication_texts:
            indication_texts = extract_sentences_with_word(total_text, 'används för')
        if not indication_texts:
            indication_texts = extract_sentences_with_word(total_text, 'för behandling av')
        if not indication_texts:
            indication_texts = extract_sentences_with_word(total_text, 'vid behandl')
        if not indication_texts:
            indication_texts = extract_sentences_with_word(total_text, 'incider')
        if not indication_texts:
            indication_texts = get_text_3('UTREDNING I ÄRENDET', block_list, r"{} ?\n([^\n]*)") 
        
        indication = chr(12).join([c.replace('\n','') for c in indication_texts])

        qaly_texts = extract_sentences_with_word(decision_text, 'QALY')
        if not qaly_texts:
            qaly_texts = extract_sentences_with_word(decision_text, 'kvalitetsjusterat')
        qaly_text = chr(12).join([c.replace('\n','') for c in qaly_texts])
        qaly_texts_comp = [t for t in qaly_texts if bool(re.search(r'företag',t,re.IGNORECASE))]
        qaly_text_comp = chr(12).join([c.replace('\n','') for c in qaly_texts_comp])
        try:
            QALY_comp = re.findall(r'\d+[\,]?\d+', qaly_text_comp)[0]
        except:
            QALY_comp = 'not presented'
        qaly_texts_TLV = [t for t in qaly_texts if bool(re.search(r'TLV',t,re.IGNORECASE))]
        qaly_text_TLV = chr(12).join([c.replace('\n','') for c in qaly_texts_TLV])
        try:
            QALY_TLV = re.findall(r'\d+[\,]?\d+', qaly_text_TLV)[0]
        except:
            try:
                QALY_TLV = re.findall(r'\d+[\,]?\d+', qaly_text)[0]
            except:
                QALY_TLV = 'not presented'

        three_part_found = bool(len(re.findall('trepart|sidoöverenskommelse', decision_text, re.IGNORECASE)))

        indirect_comp = bool(len(re.findall('indirekta jämföre|indirekt jämför', decision_text, re.IGNORECASE)))

        biosim = bool(len(re.findall('biosim', decision_text, re.IGNORECASE)))

        CM  = bool(len(re.findall('kostnadsmi|kostnadsjämförelse', decision_text, re.IGNORECASE)))
        CEA = bool(len(re.findall('kostnad per kvalitetsjusterat|kostnaden per kvalitetsjusterat|QALY|kostnadseffekt|kostnadsnytto', decision_text, re.IGNORECASE)))
        if (CEA == CM):
            type_of_analysis = 'Check CEA'
        else:
            type_of_analysis = 'CEA' if CEA else 'CM'
    except Exception as e:
        logging.critical(msg='Could not extract decision text.', exc_info=True) # This may happen if, e.g., the document has been created by scanning
        return pd.DataFrame(), 'Could not extract decision text'
    
    # df_information = pd.DataFrame([{'drug_name':drug_name,'company':company,
    #                                 'severity':severity,'three_part_deal':three_part_found,'limitations':limitations,
    #                                     'decision_date':decision_date,'comparators':comparator,
    #                                     'diarie_nr':diarie_nr, 'decision_summary':decision_text,
    #                                     'type_of_analysis':type_of_analysis,
    #                                     'indirect_comp':indirect_comp,'QALY_comp':QALY_comp,'QALY_TLV':QALY_TLV}])
    # application_type
    # r'ingår sedan'
    # r'tillfällig subvention'
    # utöka 
    # r'nya? indikation'
    # r'nya? styrk'
    # r'nya? beredningsform'

    # decsion_makers
    # diverging_opinion = 'skiljaktig mening'

    # pediatric_cohort

    df_information = pd.DataFrame([{'drug_name':drug_name,'ATC':'not present','active substance':'not present','company':company,
                                    'indication': indication,'severity':severity,'three_part_deal':three_part_found,
                                    'forms':forms,'strengths':strengths, 'annual_turnover': None,
                                        'decision_date':decision_date,'comparators':comparator,'application_type':'not present',
                                        'experts':'not present', 'diarie_nr':diarie_nr, 'decision_summary':decision_text,
                                        'type_of_analysis':type_of_analysis, 'application_text':application_text,
                                        'indirect_comp':indirect_comp,'QALY_comp':QALY_comp,'QALY_TLV':QALY_TLV,
                                        'biosim': biosim}])
    return df_information, 'Ok'


def get_indication(name, ICD):
    """Get the indication name and ICD"""
    return json.dumps({"name": name, "ICD": ICD})
    
tools = [
        {
            "type": "function",
            "function": {
                "name": "get_indication",
                "description": "Get the indication name and ICD",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "The name of the medical indication",
                        },
                        "ICD": {"type": "string", "description": "The ICD code of the medical indication"},
                    },
                    "required": ["name", "ICD"],
                },
            },
        }
    ]

def get_response(messages, simple=True): 

# "Present a summary of the products of {}, including medical indications (also ICD code), product names and development stage (if appropriate). List all products and medical indications. Specifically investigate their website for info: {}".format(name, url)
    # chat completion without streaming
    try:
        if simple:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=messages,
                temperature=0.5,
                max_tokens=64,
                top_p=1)
        else:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo-1106",
                messages=messages,
                response_format={"type": "json_object"},
                temperature=0.5,
                max_tokens=64,
                top_p=1
            )
    except openai.APIConnectionError as e:
        logger.info("The server could not be reached")
        logger.info(e.__cause__)  # an underlying Exception, likely raised within httpx.
        return 'Could not get data'
    except openai.RateLimitError as e:
        logger.info("A 429 status code was received; we should back off a bit.")
        time.sleep(5)
        return 'Could not get data'
    except openai.APIStatusError as e:
        logger.info("Another non-200-range status code was received")
        logger.info(e.status_code)
        logger.info(e.response)
        return 'Could not get data'
    return response.choices[0].message.content

def translate_spec(description):
    return [
        {
            "role": "system",
            "content": (
                "Translate the swedish text to english."
            ),
        },
        {
            "role": "user",
            "content": (description
            ),
        },
    ]

def ICD_spec(description):
    return [
        {
            "role": "system",
            "content": (
                "Extract the medical indication from the text you are presented. Be concise and only present the medical indication(s) in English and the ICD code as a list in JSON format with keys: medical indication name and ICD."
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]





