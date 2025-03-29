# -*- coding: utf-8 -*-

import fitz
import pandas as pd
import re
import json
import time
import numpy as np
import openai
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)

from scraping_methods import get_next, get_text_2, get_text, get_text_4, get_clean_block_list, get_blocks_in_between, extract_sentences_with_word

def parse_file(file):
    doc = fitz.open(file) #  bed2402-22_kymriah_67-2022 bes210923_ontozry_underlag bes220519_evrenzo_3698-2021_underlag bes190926_idacio_underlag

    df_info_tot = pd.DataFrame(columns={'drug_name':str,'ATC':str,'company':str,'indication':str,
                                       'decision_date':str,'comparators':str,'application_type':str,
                                       'experts':str, 'diarie_nr':str})
    # Page 0
    page = doc[0]
    try:
        words = page.get_text('words', sort=True)
        blocks = page.get_text('blocks')
        df_words = pd.DataFrame(words)
        df_blocks = pd.DataFrame(blocks)
        word_list = list(df_words.iloc[:,4])
        block_list = list(df_blocks.iloc[:,4])
        # check if not a NT/council bsis
        sub = 'subvention' in word_list
        ny = 'Nyansökan' in word_list
        fin = 'FINOSE' in word_list
        en = 'assessment' in word_list
        tech = 'Medicinteknisk' in word_list
        # Get indication
        indication = get_text_4('indikation', df_blocks)
        if not indication:
            indication = get_text_4('Indikation', df_blocks)
        if not indication:
            indication = get_next('indikation', block_list)
        if not indication:
            indication = get_next('Indikation', block_list)

        drug = get_text_2('dömning av', df_blocks)
        
        if not drug:
            drug = get_text_2('Underlag för beslut i landstingen', df_blocks)
        if not drug:
            drug = get_text_2('Underlag för beslut i regionerna', df_blocks)
        if not drug:
            drug = get_text_2('Klinikläkemedel', df_blocks)
        if not drug:
            drug = get_text_2('tilläggsanalyser', df_blocks)  
        if drug:
            try:
                drug_name = re.findall(r'(.*) \(',drug)[0]
                active_drug_name = re.findall(r'.*\((.*)\)',drug)[0]
            except:
                drug_name = drug
                active_drug_name = ''
        decision_date = get_text_2('underlag\:', df_blocks)
        
        # Get diarie nr
        diarie_nr = word_list[word_list.index('Diarienummer:') + 1] if 'Diarienummer:' in word_list else None
        if not diarie_nr:
            diarie_nr = word_list[word_list.index('Dnr') + 1] if 'Dnr' in word_list else None
        if not diarie_nr:
            diarie_nr = word_list[word_list.index('Dnr:') + 1] if 'Dnr:' in word_list else None
    except Exception as e:
        logging.critical(e, msg='Could not read document.', exc_info=True)
        return pd.DataFrame(), pd.DataFrame()
    if sub or fin or en or ny or tech:
        logger.info('fuck up')
        return pd.DataFrame(), pd.DataFrame()
    
    # get diarie number, company, review committee, board of experts 
    # These should be available for all modi of the report
    page = doc[1]
    words = page.get_text('words', sort=True)
    blocks = page.get_text('blocks')
    df_words = pd.DataFrame(words)
    df_blocks = pd.DataFrame(blocks)

    page = doc[2]
    words_2 = page.get_text('words', sort=True)
    blocks_2 = page.get_text('blocks')
    df_words_2 = pd.DataFrame(words_2)
    df_blocks_2 = pd.DataFrame(blocks_2)

    page = doc[3]
    words_3 = page.get_text('words', sort=True)
    blocks_3 = page.get_text('blocks')
    df_words_3 = pd.DataFrame(words_3)
    df_blocks_3 = pd.DataFrame(blocks_3)

    df_words = pd.concat([pd.concat([df_words, df_words_2]),df_words_3])
    df_blocks = pd.concat([pd.concat([df_blocks, df_blocks_2]), df_blocks_3])
    word_list = list(df_words.iloc[:,4])

    if 'FINOSE' in word_list or 'Medicinteknikuppdraget' in word_list:
        return pd.DataFrame(), pd.DataFrame()

    if not diarie_nr:
        diarie_nr = word_list[word_list.index('Diarienummer:') + 1] if 'Diarienummer:' in word_list else None

    block_list = list(df_blocks.iloc[:,4])

    company = get_text('^Företag(?!et)', df_blocks) # only works pre 23
    if not company:
        try:
            company = word_list[word_list.index('Företag:') + 1]
        except Exception as e:
            company = ''
            logger.info('cannot find company here') 
    # May be non-existing
    df_work = pd.DataFrame()
    try:
        index = [idx for idx, s in enumerate(block_list) if 'Arbetsgrupp' in s][0]
        work_group = re.split(', | och ', block_list[index][block_list[index].startswith('Arbetsgrupp:') and len('Arbetsgrupp:'):])
        pattern = r"\((.*)\)" 
        titles = []
        for w in work_group:
            if re.findall(pattern, w):
                title = re.findall(pattern, w)[0]
            else:
                title = ''
            titles.append(title)
        #titles = [re.findall(pattern, w)[0] for w in work_group if re.findall(pattern, w)]
        pattern = r"([^\()]*)" 
        names = [re.findall(pattern, w)[0].strip() for w in work_group]
        df_work = pd.DataFrame({'name':names, 'title':titles, 'diarie_nr': diarie_nr})
        # index = [idx for idx, s in enumerate(block_list) if 'Arbetsgrupp' in s][0]
        # work_group = re.split(', | och ', block_list[index][block_list[index].startswith('Arbetsgrupp:') and len('Arbetsgrupp:'):])
        # pattern = r"\((.*)\)" 
        # titles = []
        # for w in work_group:
        #     if re.findall(pattern, w):
        #         title = re.findall(pattern, w)[0]
        #     else:
        #         title = ''
        # titles.append(title)
        # pattern = r"(.*)\(" 
        # names = [re.findall(pattern, w)[0].strip() for w in work_group]
        # df_work = pd.DataFrame({'name':names, 'title':titles, 'diarie_nr': diarie_nr})
        #index = [idx for idx, s in enumerate(block_list) if (('Kliniska' in s) or ('Klinisk' in s))][0]
        #logger.info(block_list[index])
    except:
        df_work = pd.DataFrame()
        #logger.info('cannot find workgroup here') 
    
    
    try:
        index = [idx for idx, s in enumerate(block_list) if (('Kliniska' in s) or ('Klinisk' in s))][0]
        pattern = r"([^.!?]*)"
        expert = re.findall(pattern, block_list[index])[0]
        expert_group = re.split(', | och ', expert).strip()#block_list[index][block_list[index].startswith('Kliniska experter:') and len('Kliniska experter:'):])
    except:
        expert = ''
        #logger.info('cannot find expert here') 

    b_clean_list = get_clean_block_list(doc)
    total_text = chr(12).join([c.replace('\n','').replace('-','') for c in b_clean_list])
    decision_summary = get_blocks_in_between(r'TLV:s bedömning|TLV:s centrala', r'Innehållsför|\.{5}\d', b_clean_list, get_last=False)
    if not decision_summary: # post -23
        decision_summary = get_blocks_in_between(r'Samlad bedömning', r'^\d |^\d\d', b_clean_list, get_last=False)
    # Three-part negotiation? 
    three_part_found = bool(len(re.findall('trepart', decision_summary)))
    # Cost-effectiveness or cost minimization?
    CEA = bool(len(re.findall('kostnad per kvalitetsjusterat|kostnaden per kvalitetsjusterat|QALY|kostnadseffektiv', decision_summary)))
    CM  = bool(len(re.findall('kostnadsmi|kostnadsjämförelse', decision_summary)))
    if (CEA == CM):
        type_of_analysis = 'Check CEA'
    else:
        type_of_analysis = 'CEA' if CEA else 'CM'
    
    comparator_texts = extract_sentences_with_word(decision_summary, 'jämförelsealt')
    comparator = chr(12).join([c.replace('\n','') for c in comparator_texts])

    severity_texts = extract_sentences_with_word(decision_summary, 'svårighetsgrad')
    if not severity_texts:
        severity_texts = extract_sentences_with_word(total_text, 'svårighetsgrad')
    severity = chr(12).join([c.replace('\n','') for c in severity_texts])

    qaly_texts = extract_sentences_with_word(decision_summary, 'QALY')
    if not qaly_texts:
        qaly_texts = extract_sentences_with_word(decision_summary, 'kvalitetsjusterat')
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
    

    three_part_found = bool(len(re.findall('trepart|sidoöverenskommelse', decision_summary, re.IGNORECASE)))

    indirect_comp = bool(len(re.findall('indirekta jämföre|indirekt jämför', decision_summary, re.IGNORECASE)))

    biosim = bool(len(re.findall('biosim', decision_summary, re.IGNORECASE)))

    # df_information = pd.DataFrame([{'drug_name':drug_name,'company':company,'indication':indication,'severity':severity,
    #                                 'three_part_deal':three_part_found,'decision_date':decision_date,'comparators':comparator,
    #                                 'application_type':'klinikläkemedel','experts':expert, 'diarie_nr':diarie_nr, 
    #                                 'decision_summary':decision_summary,'type_of_analysis':type_of_analysis, 
    #                                 'indirect_comp':indirect_comp,'QALY_comp':QALY_comp,'QALY_TLV':QALY_TLV}])
    
    df_information = pd.DataFrame([{'drug_name':drug_name,'ATC':'not present','active substance':active_drug_name,'company':company,
                                    'indication':indication,'severity':severity,'three_part_deal':three_part_found,
                                        'forms':None,'strengths':None, 'annual_turnover': None,
                                        'decision_date':decision_date,'comparators':comparator,'application_type':'not present',
                                        'experts':expert, 'diarie_nr':diarie_nr, 'decision_summary':decision_summary,
                                        'type_of_analysis':type_of_analysis, 'application_text':'',
                                        'indirect_comp':indirect_comp,'QALY_comp':QALY_comp,'QALY_TLV':QALY_TLV,
                                        'biosim': biosim}])
    

    
    #df_information = pd.merge(df_information, df_work, on='diarie_nr')
    df_info_tot = pd.concat([df_info_tot, df_information], ignore_index=True)
    return df_information, df_work


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





