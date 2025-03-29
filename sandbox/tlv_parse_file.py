# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import fitz
import pandas as pd
import re
import json
import time
import numpy as np
import openai
from openai import OpenAI
from secret import secrets

doc = fitz.open("./bes190926_idacio_underlag.pdf") #  bed2402-22_kymriah_67-2022 bes210923_ontozry_underlag bes220519_evrenzo_3698-2021_underlag bes190926_idacio_underlag

df_info_tot = pd.DataFrame(columns={'drug_name':str,'ATC':str,'company':str,'indication':str,
                                       'decision_date':str,'comparators':str,'application_type':str,
                                       'experts':str, 'diarie_nr':str})
#%%


client = OpenAI(api_key=secrets.open_ai_key, 
                timeout=60.0)

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
        print("The server could not be reached")
        print(e.__cause__)  # an underlying Exception, likely raised within httpx.
        return 'Could not get data'
    except openai.RateLimitError as e:
        print("A 429 status code was received; we should back off a bit.")
        time.sleep(5)
        return 'Could not get data'
    except openai.APIStatusError as e:
        print("Another non-200-range status code was received")
        print(e.status_code)
        print(e.response)
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
#%%
def get_text(letters, df_blocks):
    try:
        text = df_blocks[df_blocks.iloc[:,4].str.contains(letters)].iloc[:,4].values[0]
        pattern = r"\n(.*)" 
        return re.findall(pattern, text, flags=re.DOTALL)[0].strip() 
    except:
        print('Cannot find: ' + letters)
        return ''
#%%
# Page 0
page = doc[0]
words = page.get_text('words', sort=True)
text = page.get_text('text')
blocks = page.get_text('blocks')
df_words = pd.DataFrame(words)
df_blocks = pd.DataFrame(blocks)
# Get indication
indication = get_text('^Utv.*', df_blocks)
# Get diarie nr
word_list = list(df_words.iloc[:,4])
diarie_nr = word_list[word_list.index('Diarienummer:') + 1]
# Get decision date - no, see next page
#decision_date = word_list[word_list.index('nämndmöte:') + 1]
#%%
# Extract indications (name and ICD) and translate text to english
indication_en = get_response(translate_spec(indication))
print(indication)
print(indication_en)
ICD = get_response(ICD_spec(indication_en), simple=False)
j_ICD = json.loads(ICD)
print(j_ICD)
for key in j_ICD.keys():
    print(key)
df_ICD = pd.read_json(j_ICD)
#%%
# get diarie number, company, review committee, board of experts 
# These should be available for all modi of the report
page = doc[1]
words = page.get_text('words', sort=True)
text = page.get_text('text')
blocks = page.get_text('blocks')
df_words = pd.DataFrame(words)
df_blocks = pd.DataFrame(blocks)

page = doc[2]
words_2 = page.get_text('words', sort=True)
text_2 = page.get_text('text')
blocks_2 = page.get_text('blocks')
df_words_2 = pd.DataFrame(words_2)
df_blocks_2 = pd.DataFrame(blocks_2)

df_words = pd.concat([df_words, df_words_2])
df_blocks = pd.concat([df_blocks, df_blocks_2])

block_list = list(df_blocks.iloc[:,4])


company = get_text('^Fö.*', df_blocks)
drug_name = get_text('^Varu.*', df_blocks)
active_drug_name = get_text('^Akti.*', df_blocks)
ATC_code = get_text('^ATC.*', df_blocks)
severity = get_text('^Sjukdomens.*', df_blocks)
application_type = get_text('^Typ.*', df_blocks)
decision_date = get_text('^Sis.*', df_blocks)
comparator = get_text('^Rele.*', df_blocks)
number_of_patients = get_text('^Anta*', df_blocks)
pattern = r"(\d+\s\d+|\d+)" 
number_of_patients = re.findall(pattern, number_of_patients)

found_work_group = False
try:
    index = [idx for idx, s in enumerate(block_list) if 'Arbetsgrupp' in s][0]
    work_group = re.split(', | och ', block_list[index][block_list[index].startswith('Arbetsgrupp:') and len('Arbetsgrupp:'):])
    pattern = r"\((.*)\)" 
    titles = [re.findall(pattern, w)[0] for w in work_group]
    pattern = r"(.*)\(" 
    names = [re.findall(pattern, w)[0].strip() for w in work_group]
    df_work = pd.DataFrame({'name':names, 'title':titles, 'diarie_nr': diarie_nr})
    index = [idx for idx, s in enumerate(block_list) if (('Kliniska' in s) or ('Klinisk' in s))][0]
    print(block_list[index])
    pattern = r":(.*)\."
    expert = re.findall(pattern, block_list[index])[0]
    expert_group = re.split(', | och ', expert).strip()#block_list[index][block_list[index].startswith('Kliniska experter:') and len('Kliniska experter:'):])
    found_work_group = True
except:
    print('cannot find workgroup/expert here') 
#t = re.search(r'(?<= .+)',block_list[index]).group(0)
#group =  ) 

# This is for 2023 and forward
try:
    word_list = list(df_words.iloc[:,4])
    company = word_list[word_list.index('Företag:') + 1]
    diarie_nr = word_list[word_list.index('Diarienummer:') + 1]
except Exception as e:
    print(e)
    print('cannot find company here') 

#%%
def get_decision_summary(page):
    words = page.get_text('words', sort=True)
    text = page.get_text('text')
    df_words = pd.DataFrame(words)
    word_list = list(df_words.iloc[:,4])
    # Check if decision summary
    decision_summary = ''
    pattern = r"(TLV.*)"
    if word_list[0]=='TLV:s':
        decision_summary = re.findall(pattern, text, flags=re.DOTALL)[0]
    return decision_summary
#%%
page = doc[3]
decision_summary = get_decision_summary(page)
print(decision_summary)
if not decision_summary:
    decision_summary = get_decision_summary(doc[4])
# Thrre-part negotiation? 
three_part_found = bool(len(re.findall('trepart', decision_summary)))
# Cost-effectiveness or cost minimization?
CEA = bool(len(re.findall('kostnad per kvalitetsjusterat|kostnaden per kvalitetsjusterat|QALY|kostnadseffektiv', decision_summary)))
CM  = bool(len(re.findall('kostnadsmi|kostnadsjämförelse', decision_summary)))
if (CEA == CM):
    type_of_analysis = 'Check CEA'
else:
    type_of_analysis = 'CEA' if CEA else 'CM'
#%%
df_information = pd.DataFrame([{'drug_name':drug_name,'ATC':ATC_code,'company':company,'indication':indication,
                                       'decision_date':decision_date,'comparators':comparator,'application_type':application_type,
                                       'experts':expert, 'diarie_nr':diarie_nr, 'decision_summary':decision_summary,
                                       'type_of_analysis':type_of_analysis}])
df_information = pd.merge(df_information, df_work, on='diarie_nr')
df_info_tot = pd.concat([df_info_tot, df_information], ignore_index=True)

