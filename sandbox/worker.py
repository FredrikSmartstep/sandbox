import os
import re
import openai
import EventHandler
import json
from logger_tt import getLogger
#from json_model2 import HTA_Document, HTA_Agency, Product, Company, PICO, Analysis, Staff, Expert, Decision_Maker, Indication_Simplified
from pydantic_models import HTA_Document, PICO, HTA_Document_Staff, Team, PICOs_Partial_Agency, PICOs_Partial_Company,\
    PICO_Analysis_Cost_Company, PICO_Analysis_Cost_Agency, Costs
import document_splitting as ds
from typing import Iterable
import time
import json
from openai import OpenAI
import instructor
import sys
from secret import secrets

client2 = OpenAI(
    api_key=secrets.open_ai_key,
    max_retries=4, 
    timeout=40.0)

client_instructor = instructor.from_openai(OpenAI(
    api_key=secrets.open_ai_key, 
    max_retries=4, 
    timeout=60.0))

log = getLogger(__name__)

TRAINING_BASE = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/test2/'

VECTOR_STORE_ID_DECISIONS = 'vs_ImbIGqIEGw2gVV3dNT6Ki7tk'   # This is the one I've been using without much success: 'vs_yqbDXJu9OTA0GDZqc5mJ7BKn'

MAX_NR_OF_RETRIES = 5

MAX_WAIT_TIME_STORAGE = 10

def replacer_company(res, res_pico_comp):
    k=0
    for pico in res_pico_comp['picos_company']:
        # If no HE result at all for this pico, add it!
        if len(res['picos'])<(k+1):
            res['picos'].append(pico)
            # costs 
            res['picos'][k]['analysis'] = {}
            res['picos'][k]['analysis']['costs'] = []
            res['picos'][k]['analysis']['costs'].append({
                'assessor': 'company',
                'product': 'product',
                **pico['costs_company_product']})
            res['picos'][k]['analysis']['costs'].append({
                'assessor': 'company',
                'product': 'comparator',
                **pico['costs_company_comparator']})
            res['picos'][k]['analysis']['costs'].clear()
            res['picos'][k]['analysis']['costs'].append({
                'assessor': 'company',
                'product': 'product',
                **pico['costs_company_product']})
            #del res['picos'][k]['analysis']['costs_company_product']
        else:
            res['picos'][k]['indication'] = pico['indication']
            res['picos'][k]['comparators_company'] = pico['comparator_company']
            res['picos'][k]['comparator_modus_company'] = pico['comparator_modus_company']
            res['picos'][k]['comparator_reason_company'] = pico['comparator_reason_company']
            res['picos'][k]['analysis']['QALY_gain_company'] = pico['QALY_gain_company']
            res['picos'][k]['analysis']['QALY_total_cost_company'] = pico['QALY_total_cost_company']
            res['picos'][k]['analysis']['ICER_company'] = pico['ICER_company']
            res['picos'][k]['analysis']['comparison_method'] = pico['comparison_method']
            res['picos'][k]['analysis']['indirect_method'] = pico['indirect_method']
            res['picos'][k]['analysis']['costs'].clear()
            res['picos'][k]['analysis']['costs'].append({
                'assessor': 'company',
                'product': 'product',
                **pico['costs_company_product']})
            res['picos'][k]['analysis']['costs'].append({
                'assessor': 'company',
                'product': 'comparator',
                **pico['costs_company_comparator']})  
        k=k+1

    return res

def replacer_agency(res, res_pico_agency):
    k=0
    for pico in res_pico_agency['picos_agency']:
        res['picos'][k]['analysis']['comparators_agency'] = pico['comparator_agency']
        res['picos'][k]['analysis']['comparator_modus_agency'] = pico['comparator_modus_agency']
        res['picos'][k]['analysis']['comparator_reason_agency'] = pico['comparator_reason_agency']
        res['picos'][k]['analysis']['QALY_gain_agency_lower'] = pico['QALY_gain_agency_lower']
        res['picos'][k]['analysis']['QALY_gain_agency_higher'] = pico['QALY_gain_agency_higher']
        res['picos'][k]['analysis']['QALY_total_cost_agency_lower'] = pico['QALY_total_cost_agency_lower']
        res['picos'][k]['analysis']['QALY_total_cost_agency_higher'] = pico['QALY_total_cost_agency_higher']
        res['picos'][k]['analysis']['ICER_agency_lower'] = pico['ICER_agency_lower']
        res['picos'][k]['analysis']['ICER_agency_higher'] = pico['ICER_agency_higher']

        res['picos'][k]['analysis']['costs'].append({
            'assesser': 'agency',
            'product': 'product',
            **pico['costs_agency_product']})
        res['picos'][k]['analysis']['costs'].append({
            'assesser': 'agency',
            'product': 'comparator',
            **pico['costs_agency_comparator']})  

        res['picos'][k]['analysis']['efficacy_summary'] = pico['efficacy_summary']
        res['picos'][k]['analysis']['safety_summary'] = pico['safety_summary']
        res['picos'][k]['analysis']['decision_summary'] = pico['decision_summary']
        res['picos'][k]['analysis']['uncertainty_assessment_clinical'] = pico['uncertainty_assessment_clinical']
        res['picos'][k]['analysis']['uncertainty_assessment_he'] = pico['uncertainty_assessment_he']
        k=k+1

    return res    

class Worker:

    def __init__(self, client, model = 'gpt-4o-mini'):
        log.info("Creating worker")
        self.client = client_instructor # TODO OBS client2
        self.model = model
        self.create_assistant(model=model)
        self.vector_store = None
        self.message_file = None
        # try to load vector store
        #self.load_vector_store('vs_VELUbeKoZcO5lXSDhUd8lx8z')#VECTOR_STORE_ID_DECISIONS) # going with the default
        #if not self.vector_store:
        #    log.info("Vector store loading failed. Creating new")
        #    self.create_vector_store('TLV_decisions', TRAINING_BASE)

        #self.attach_vector_store_to_assistant()
    
    def clean_up(self):
        self.delete_file()
        self.delete_vs()
        self.delete_assistant()

    def delete_file(self):

        response = self.client.files.delete(self.message_file.id)
        log.info('Deleted file ' + self.message_file.filename + ': ' + str(response))

    def delete_vs(self):
        response = self.client.beta.vector_stores.delete(vector_store_id=self.vector_store.id)
        log.info('Deleted vector store: ' + self.vector_store.name + ': ' + str(response))

    def delete_assistant(self):
        response = self.client.beta.assistants.delete(self.assistant.id)
        log.info(response)

    def initial_parsing(self):
        # sometimes it fail. I think is is due to not accessing the file even though it is said to be ready. Let's try a wait and give it a go again
        title = 'not found'
        self.run_parsing()
        ass_results, annot = self.get_results()
        #data, annot = parser.parse_file(file_dir + file)
        #res = self.json_parse(ass_results)
        res = self.run_json_parsing(ass_results, HTA_Document)
        res_json = res.model_dump()
        log.info(res_json)
        if not res:
            time.sleep(3)
            return None
        title = res.title
        #nr_of_picos = len(res['population_cohorts'])
        
        if title=='not found':
            time.sleep(3)
            return None
        return res_json

    @staticmethod
    def json_parse(data):
        try:
            json_data = json.loads(data)
        except Exception as e:
            log.error('JSON parsing failed', exc_info=sys.exc_info())
            json_data = None
        return json_data

    def run_and_parse(self, message, additional_instructions = None):
        res = {}
        attempt = 1
        while (not res) and (attempt<MAX_NR_OF_RETRIES):
            log.info('Run and parse attempt ' + str(attempt))
            self.run_parsing(additional_messages=message, additional_instructions=additional_instructions)
            ass_results, annot = self.get_results()
            res = self.json_parse(ass_results)
            attempt = attempt + 1
        return res

    def parse_file_2(self, file_path):
        results_doc = None
        try: 
            if self.vector_store:
                deleted_vector_store = self.client.beta.vector_stores.delete(vector_store_id=self.vector_store.id)
            
            self.add_files(file_path)
            # split file
            self.create_vector_store(file_path.split('/')[-1])
            
            #self.attach_vector_store_to_assistant() # not going to do that. Instead we use the thread and attach the vs to it

            self.create_original_message_2()# elf.create_meta_data_message() #self.create_original_message()#create_basic_message()
            log.info('Message: \n' + self.message[0]["content"])
            self.create_thread()
            
            attempt = 0
            res = None
            while ((attempt<MAX_NR_OF_RETRIES) and not res):
                attempt = attempt + 1
                log.info('Parsing attempt nr ' + str(attempt))
                res = self.initial_parsing()
            if not res:
                log.error('Unable to parse file ' + file_path)
                return None
                
            #except Exception as e:
            #    log.error(e, exc_info=True)
            #if title=='not found':
            #    log.info('Couldnt find file for some reason. Dumping and rexreating assistant')
            #    self.delete_assistant()
            #    self.create_assistant(model=self.model)

            # See igf possible to find more picos
            #log.info('Reparsing to include more PICO results')
            #self.rerun_parsing()
            #ass_results, annot = self.get_results()
            #res = json.loads(ass_results)

            # if basic info is missing
            #nr_of_picos = len(res['population_cohorts'])
            #if nr_of_picos==0:
            #    self.run_parsing_w_addon(additional_messages=self.create_basic_message())
            #    ass_results_basic, annot = self.get_results()
            #    res_basic = json.loads(ass_results_basic)

            # Let's add the rest #first two chapters (Background and Disesse info)
            
            # Try to find basic info
            log.info('Getting info')
            res_info = self.run_and_parse(self.query_if_requested_info())
            #self.run_parsing(additional_messages=self.query_if_requested_info())
            #ass_results_more_info, annot = self.get_results()
            #res_info = self.json_parse(ass_results_more_info)
            res.update(res_info)

            log.info('Getting details')
            res_details = self.run_and_parse(self.create_details_message())
            #self.run_parsing(additional_messages=self.create_details_message())
            #ass_results_details, annot = self.get_results()
            #res_details = self.json_parse(ass_results_details)
            res.update(res_details)

            log.info('Getting indications')
            res_ind = self.run_and_parse(self.query_about_indications())
            #self.run_parsing(additional_messages=self.query_about_indications())
            #ass_results_ind, annot = self.get_results()
            #res_ind = self.json_parse(ass_results_ind)

            if len(res_ind['indications'])==0:
                log.info('Parser missed indications and populations. redo')
                res_ind = self.run_and_parse(message=self.query_about_indications(), additional_instructions='You missed extracting indications and population cohorts. Please have another go.')
                #ass_results_ind, annot = self.get_results()
                #res_ind = self.json_parse(ass_results_ind)
        
            # get icds
            indications = {'indications':[]}
            for ind in res_ind['indications']:
                log.info('Get ind: ' + ind['indication'])
                icd10 = self.get_response(self.get_ICD_code(ind['indication']))
                # truncate to only one decimal
                icd10 = re.sub(r'(?<=\.\d)\d','',icd10)
                log.info('Got ICD: ' + icd10)
                indications['indications'].append({'icd10':icd10,'severity':ind['severity']})
            res.update(indications)
            #log.info('After indication retrival')
            #log.info(res)
            
            # SKIP
            #log.info('Getting HE')
            #res_HE = self.run_and_parse(self.create_HE_message())
            #self.run_parsing(additional_messages=self.create_HE_message())
            #ass_results_HE, annot = self.get_results()
            #res_HE = self.json_parse(ass_results_HE)
            #res.update(res_HE)
            
            # SKIP
            #log.info('Getting form')
            #res_form = self.run_and_parse(self.query_about_form())
            #self.run_parsing(additional_messages=self.query_about_form())
            #ass_results_form, annot = self.get_results()
            #res_form = self.json_parse(ass_results_form)

            if len(res['staff'])<2: # moved it up since message was ignored and pico message was run agani?
                log.info('Missing staff. Having a second go')
                #self.run_parsing(additional_messages=self.find_staff_message())
                self.run_parsing(additional_messages=self.find_staff_message())
                
                ass_results_staff, annot = self.get_results()
                res_staff = self.run_json_parsing(ass_results_staff, Team)
                res_staff_json = res_staff.model_dump()
                log.info(res_staff_json)
                #res_staff = self.json_parse(ass_results_staff)
                res['staff'] = res_staff_json#['staff']
                #res['presenter_to_the_board'] = res_staff['presenter_to_the_board']
                #res['other_participants'] = res_staff['other_participants']
            
            # HE results
            # Let's add chapter 3
            log.info('Get the picos')
            res_pico_comp = self.run_and_parse(self.create_pico_message_company())
            #self.run_parsing(additional_messages=self.create_pico_message_company())
            #ass_results_pico_comp, annot = self.get_results()
            #res_pico_comp = self.json_parse(ass_results_pico_comp)

            res_pico_agency = self.run_and_parse(self.create_pico_message_agency())
            #self.run_parsing(additional_messages=self.create_pico_message_agency())
            #ass_results_pico_agency, annot = self.get_results()
            #res_pico_agency = self.json_parse(ass_results_pico_agency)

            #res_pico_comp = self.run_json_parsing(ass_results_pico_comp, PICO_Analysis_Cost_Company)
            #res_pico_comp = res_pico_comp.model_dump()
            #log.info(res_pico_comp)
            #res_pico_agency = self.run_json_parsing(ass_results_pico_agency, PICO_Analysis_Cost_Agency)
            #res_pico_agency = res_pico_agency.model_dump()
            #log.info(res_pico_agency)
            
            # make sure we got all the picos
            if len(res_pico_agency['picos_agency'])<len(res_ind['population_cohorts']):
                log.info('we missed some agency picos')
                res_pico_agency = self.run_and_parse(message=self.create_pico_message_agency(), additional_instructions='I think you missed some of the PICO:s. Please have another go.')
                #ass_results_pico_agency, annot = self.get_results()
                #res_pico_agency = self.json_parse(ass_results_pico_agency)

            if len(res_pico_comp['picos_company'])<len(res_ind['population_cohorts']):
                log.info('we missed some company picos')
                res_pico_comp = self.run_and_parse(message=self.create_pico_message_company(), additional_instructions='I think you missed some of the PICO:s. Please have another go.')
                #ass_results_pico_comp, annot = self.get_results()
                #res_pico_comp = self.json_parse(ass_results_pico_comp)
            
            # Make sure we got all the ICER, QALY_gain and QUALy_costs:
            if res['analysis']=='cost-effectiveness':
                icer_exists = True
                qaly_gain = True
                qaly_cost = True
                for pico in res_pico_agency['picos_agency']:
                    icer_exists = icer_exists & bool(pico['ICER_agency_lower'])
                    qaly_gain = qaly_gain & bool(pico['QALY_gain_agency_lower'])
                    qaly_cost = qaly_cost & bool(pico['QALY_total_cost_agency_lower'])
        
                if not (icer_exists & qaly_cost & qaly_gain):
                    log.info('we missed some agency qalys')
                    res_pico_agency = self.run_and_parse(message=self.create_pico_message_agency(), additional_instructions='I think you missed to extract ICER and/or QALY gains or costs for some of the PICO:s. Please have another go.')
                    #ass_results_pico_agency, annot = self.get_results()
                    #res_pico_agency = self.json_parse(ass_results_pico_agency)
            else:
                drug_costs_exists = True
                total_costs_exists = True
                
                for pico in res_pico_agency['picos_agency']:
                    drug_costs_exists = drug_costs_exists & bool(pico['costs_agency_product']['drug_cost'])
                    total_costs_exists = total_costs_exists & bool(pico['costs_agency_product']['total_treatment_cost'])
                
                if not (drug_costs_exists & total_costs_exists):
                    log.info('we missed some agency costs')
                    res_pico_agency = self.run_and_parse(message=self.create_pico_message_agency(), additional_instructions='I think you missed to extract drug and treatment costs for some of the PICO:s. Please have another go.',)
                    #ass_results_pico_agency, annot = self.get_results()
                    #res_pico_agency = self.json_parse(ass_results_pico_agency)

                drug_costs_exists_comp = True
                total_costs_exists_comp = True
                
                for pico in res_pico_comp['picos_company']:
                    drug_costs_exists_comp = drug_costs_exists_comp & bool(pico['costs_company_product']['drug_cost'])
                    total_costs_exists_comp = total_costs_exists_comp & bool(pico['costs_company_product']['total_treatment_cost'])
                
                if not (drug_costs_exists_comp & total_costs_exists_comp):
                    log.info('we missed some company costs')
                    res_pico_comp = self.run_and_parse(message=self.create_pico_message_company(), additional_instructions='I think you missed to extract drug and treatment costs for some of the PICO:s. Please have another go.',)
                    #ass_results_pico_comp, annot = self.get_results()
                    #res_pico_comp = self.json_parse(ass_results_pico_comp)
            # final check:
            if len(res_pico_agency['picos_agency'])!=len(res_pico_comp['picos_company']):
                if len(res_pico_agency['picos_agency'])<len(res_pico_comp['picos_company']):
                    log.info('we still missed some agency picos')
                    res_pico_agency = self.run_and_parse(message=self.create_pico_message_agency(), additional_instructions='You missed some of the PICO:s that you identified for the company. Please have another go.')
                else:
                    log.info('we still missed some company picos')
                    res_pico_comp = self.run_and_parse(message=self.create_pico_message_company(), additional_instructions='You missed some of the PICO:s that you identified for the agency. Please have another go.')
                
            
            # Let's get the summaries once more
            log.info('Get the summaries')
            res_sum = self.run_and_parse(self.create_summary_message())
            #self.run_parsing(additional_messages=self.create_summary_message())
            #ass_results_sum, annot = self.get_results()
            #res_sum = self.json_parse(ass_results_sum)
            res.update(res_sum) 
                
            #log.info('Get any trial info')
            #self.run_parsing_w_addon(additional_messages=self.create_trial_message_company())
            #ass_results_trial_comp, annot = self.get_results()
            
            res = replacer_company(res, res_pico_comp)
            res = replacer_agency(res, res_pico_agency)

            # Assign icd codes for each pico
            for k in range(0,len(res['picos'])):
                icd = self.get_response(self.get_ICD_code(res['picos'][k]['indication']))
                # truncate to only one decimal
                icd = re.sub(r'(?<=\.\d)\d','',icd)
                log.info('PICO icd code: ' + icd)
                res['picos'][k]['icd_code'] = icd
                log.info('Added PICO ICD: ' + res['picos'][k]['icd_code'])

            log.info('Before pydantic extraction')
            log.info(json.dumps(res))
            #return ass_results, ass_results_pico_comp, ass_results_pico_agency, ass_results_trial_comp
            log.info('Extracting document')
            results_doc = self.run_json_parsing(res, HTA_Document)
        
        except Exception as e:
            log.error('Parsing failed', exc_info=sys.exc_info())
            results_doc = 'knas'
        
        finally:
            self.clean_up()
            return results_doc
        
        log.info('Our HTADocument: \n')
        log.info(results_doc.model_dump_json()) #pydantic adds _json to this method

   

    def create_assistant(self, model="gpt-4o-mini"):
        log.info("Creating assistant")
        # OBS client2
        self.assistant = client_instructor.beta.assistants.create(
        instructions="""You are a professional health economist. 
            You will be presented a document in Swedish describing 
            the reasons for a reimbursement decision for a medicinal product. 
            You should use your tools to extract info. Be meticulous and 
            make sure you get all the requested information. 
            It is more important to get the details right than to deliver a result fast. 
            You will also be presented with a vector store named 'TLV_decisions'. 
            It should only be used to derive patterns and structure of the documents. 
            """,
        model=model,
        temperature=0.2,
        tools= [
            {"type": "file_search"}
        ]
        )

    def run_json_parsing(self, input, format):
        completion = None
        log.info('Running JSON parsing')
        attempt = 1
        while attempt<MAX_NR_OF_RETRIES:
            try:
                completion=client_instructor.chat.completions.create(
                model="gpt-4o-2024-08-06", # mini does not manage
                response_model=format,
                messages=[
                    {"role": "system", "content": "You are a useful assistant. Extract information according to the speficied response model"},#Convert the user input into JSON. Make sure to include the nested objects, such as Product and HTA_Agency.
                    {"role": "user", "content": f"Extract:\n{input}"}
                ]
                )
                return completion#.choices[0].message
            except Exception as e:
                attempt = attempt + 1
                time.sleep(attempt)
                log.error('JSON parsing failed.', exc_info=sys.exc_info())
        return completion#.choices[0].message


    def run_json_parsing_2(self, ass_results, ass_results_pico_comp, ass_results_pico_agency, ass_results_trial_comp, format):
        completion=client_instructor.chat.completions.create(
        model="gpt-4o-2024-08-06",
        response_model=format,
        messages=[
            {"role": "system", "content": "You are a useful assistant. Extract information according to the speficied response model from the provided JSON objects"},#Convert the user input into JSON. Make sure to include the nested objects, such as Product and HTA_Agency.
             {"role": "user", "content": [
                 {
                  "type": "text",
                  "value": ass_results
                },
                    {
                    "type": "text",
                    "value": ass_results_pico_comp
                },
                {
                    "type": "text",
                    "value": ass_results_pico_agency
                },
                {
                    "type": "text",
                    "value": ass_results_trial_comp
                }
             ]
             }
        ],
        # response_format={
        #     "type": "json_schema",
        #     "json_schema": {
        #         "name": "HTA_Document",
        #         "strict": True,
        #         }
        #     }
        )
        return completion#.choices[0].message
    
    def get_response(self, messages): 
    # "Present a summary of the products of {}, including medical indications (also ICD code), product names and development stage (if appropriate). List all products and medical indications. Specifically investigate their website for info: {}".format(name, url)
        # chat completion without streaming
        try:
            # OBS client2
            response = client2.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0.5,
                max_tokens=1000,
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
    
    @staticmethod
    def get_ICD_code(description):
        return [
            {
                "role": "system",
                "content": (
                    "Get the ICD10 code for this medical indication. Be concise and only present the ICD code."
                ),
            },
            {
                "role": "user",
                "content": (description),
            },
        ]
    
    @staticmethod
    def get_ICD_codes(description):
        return [
            {
                "role": "system",
                "content": (
                    "Get the ICD10 code for the medical indications described in the text. Be concise and only present the ICD codes as a comma-separated list [icd1, icd2,...]."
                ),
            },
            {
                "role": "user",
                "content": (description),
            },
        ]

    def create_json_assistant(self):
        log.info("Creating JSON assistant")
        self.assistant = self.client.beta.assistants.create(
        instructions="""You are a helpful assistant. 
            You will be presented some text. Use your tools to return a JSON structured output. 
            """,
        model="gpt-4o-mini",
        temperature=0.2,
        tools= [
            {"type": "file_search"}
        ], 
        response_format= HTA_Document
        )

    def create_vector_store(self, name):
        # Creates a VS and adds the first file
        log.info("Vector store creation named {} initiated".format(name))
        try:
            self.vector_store = self.client.beta.vector_stores.create(
                name=name,
                chunking_strategy= {
                    "type" : 'static',
                    "static" : {
                    "max_chunk_size_tokens":150, #200,
                    "chunk_overlap_tokens":70, #100
                    }
                },
                 file_ids=[self.message_file.id],
                )
            start_time = time.time()
            current_time = time.time()
            response = self.client.beta.vector_stores.retrieve(vector_store_id=self.vector_store.id)
            while (response.file_counts.completed<1) and (current_time-start_time)<MAX_WAIT_TIME_STORAGE:
                time.sleep(0.5)
                response = self.client.beta.vector_stores.retrieve(vector_store_id=self.vector_store.id)
                current_time = time.time()
                log.info('Waiting for storage')
            if (current_time-start_time)>MAX_WAIT_TIME_STORAGE:
                raise Exception('Unable to create vector store in reasonable time')
            log.info('Store status: ' + response.status)
            log.info('Number of files in store: ' + str((response.file_counts.completed)))
        except Exception as e:
            log.error('could not create auto vector store', exc_info=sys.exc_info())

    def create_vector_store_other(self, name, file_dir):
        log.info("Vector store creation named {} initiated".format(name))
        try:
            self.vector_store = self.client.beta.vector_stores.create(
                name=name,
                chunking_strategy= {
                    "type" : 'static',
                    "static" : {
                    "max_chunk_size_tokens":200,
                    "chunk_overlap_tokens":100
                    }
                }
                )
            for root, dirs, files in os.walk(file_dir, topdown=False):
                file_streams = [open(path, "rb") for path in files]

            file_batch = self.client.beta.vector_stores.file_batches.upload_and_poll(
                vector_store_id=self.vector_store.id, files=file_streams)
            
            log.info('Store status: ' + file_batch.status)
            log.info('Number of files in store: ' + str(file_batch.file_counts))
        except Exception as e:
            log.error('could not create vecor store', exc_info=sys.exc_info())


    def load_vector_store(self, id = VECTOR_STORE_ID_DECISIONS):
        log.info('Loading vector store')
        try:
            self.vector_store = self.client.beta.vector_stores.retrieve(vector_store_id=id)
            log.info('Succesfully loaded vector store: ' + self.vector_store.name + ' with ' + str(self.vector_store.file_counts.completed) + ' files')
        except openai.NotFoundError as e:
            log.error('Could not find vector store', exc_info=sys.exc_info())
        except Exception as err:
            log.error(f"Unexpected {err=}, {type(err)=}")
            raise
        

    def attach_vector_store_to_assistant(self):
        log.info('Attaching vector store to assistant')
        self.assistant = self.client.beta.assistants.update(
                assistant_id=self.assistant.id,
                tool_resources={"file_search": {"vector_store_ids": [self.vector_store.id]}})
        
    def add_files(self, file_path):
        log.info('Adding file ' + file_path)
        stored_file = self.client.files.create(
                    file=open(file_path, "rb"), purpose="assistants")
        self.message_file = stored_file
        log.info('Succesfully added ' + file_path)
            

    def add_files_to_vs(self, files):
        nr_of_files = len(files)
        batch_add = self.client.beta.vector_stores.file_batches.create_and_poll(
                vector_store_id=self.vector_store.id,
                chunking_strategy= {
                            "type" : 'static',
                            "static" : {
                            "max_chunk_size_tokens":150, #200,
                            "chunk_overlap_tokens":70, #100
                            }
                        },
                file_ids=[f.id for f in files]
                )
        nr_completed = 0
        wait = 0
        while (nr_completed<nr_of_files) and (wait<10):
            wait = wait + 1
            time.sleep(1)
            vector_store_file_batch = self.client.beta.vector_stores.file_batches.retrieve(
                vector_store_id=self.vector_store.id,
                batch_id=batch_add.id
                )
            nr_completed = vector_store_file_batch.file_counts.completed
            log.info('Nr of completed files: ' + str(nr_completed))
                     
    
    def create_original_message_2(self):

# El om analystyp
#  Fel om osäkerhetsbedömninb
# severity känns också osäker
# , most often Tandvårds- och läkemedelsförmånsverket, TLV)
# TODO: add to he: indication (Name of indication),
        log.info('Creating original message')

        #title = 'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Odomzo 25 mars 2022 0_avslag-och-uteslutningar.pdf'
        self.message = [
            {
            "role": "user",
            "content": 
                """Please extract 
                title (Title of the document. See text in section 'SAKEN' on the front page), 
                company (applicant. Only company name), 
                product name, 
                diarie number (sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning'),
                date (Found at the top of the first page. Format YYYY-MM-DD),
                agency (name of the agency evaluating the product),
                decision (fully reimbursed/limited reimbursement/rejected), 
                currency (currency used in the document for prices and costs),
                requested_complement (yes/no. If TLV requested the company to complement with additional information),
                requested_information (what type of information TLV requested the company to complement the application with if applicable),
                requested_complement_submitted (yes/no/NA. If applicable whether the company submitted the requested complementary info),
                indications (list of:
                  indication (medical indications the medicinal product is evaluated for. The indication information is usually found in section 'Ansökan' and/or in the section 'Utredning i ärendet'),
                  severity (the associated severity (low/moderate/high/very high/varying/not assessed) of this indication. The severity assessment is usually found in the sub section 'TLV gör följande bedömning' 
                  in a sentence like 'TLV bedömer (?:att )?svårighetsgraden')
                ), 
                limitations (list of limitations that applies to the reimbursement. May be none),
                previously_licensed_medicine (yes/no. Whether the active ingredient or the drug previously was a licensed medicine. Infomration found in section 'TLV gör följande bedömning' or in the section 'Utredning i ärendet'),
                analysis (cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost benefit analysis (sv: kostnadsnyttoanalys)), 
                form (list of form, strength, AIP and AUP of the medicinal product, the AIP and AUP have a blank char between hundreds and thousands, e.g. 100 000,00 means one hundred thousand),
                population_cohorts (list of population cohorts. Usually found in section 'Utredning i ärendet':
                    population (name or description of population),
                    incidence (how many get diagnosed with this indication every year according to the report),
                    prevalance (how many have this condition according to the report)
                ),
                staff (list of people involved in the assessment and decision making. They are usually found in the section starting with "Detta beslut har fattats av Nämnden för läkemedelsförmåner hos TLV" and in the sentence starting with "Följande ledamöter har deltagit i beslutet:
                    name (name of each person),
                    profession (the profession, if provided, of this person),
                    dissent (yes/no/NA, possible dissent on the decision),
                    role (could be decision makers (swedish 'ledamöter'), presenter_to_the_board (swedish "föredragare") or non-voting board member 
                    (usually found in the sentence following the description of the presenter)
                ),
                
                from the file named '{}'. If you cannot find the file, return title as 'not found' and the rest of the requested output as blanks. 
                Information that you cannot find should be returned as blank in the corresponding field.
                Make sure you get all the PICOs included in the picos output and all trials referred to by the company in trials_company output.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename),
            # Attach the new file to the message.
            #"attachments": [
            #  { "file_id": message_file.id, "tools": [{"type": "file_search"}] }
            #],
            }
        ]


    def create_meta_data_message(self):

# El om analystyp
#  Fel om osäkerhetsbedömninb
# severity känns också osäker
# , most often Tandvårds- och läkemedelsförmånsverket, TLV)
        log.info('Creating original message')

        #title = 'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Odomzo 25 mars 2022 0_avslag-och-uteslutningar.pdf'
        self.message = [
            {
            "role": "user",
            "content": 
                """Please extract: 
                title (Title of document. See text in section 'SAKEN' on the front page), 
                company (applicant. Only company name), 
                product name, 
                diarie number (sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning'),
                date (Found at the top of the first page. Format YYYY-MM-DD),
                agency (name of the agency evaluating the product),
                decision (fully reimbursed/limited reimbursement/rejected), 
                currency (currency used in the document for prices and costs),
                
                from the file named '{}'. If you cannot find the file, return title as 'not found' and the rest of the requested output as blanks. 
                Information that you cannot find should be returned as blank in the corresponding field.
                Make sure you get all the PICOs included in the picos output and all trials referred to by the company in trials_company output.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename)
            }
        ]

    def create_details_message(self):

# El om analystyp
#  Fel om osäkerhetsbedömninb
# severity känns också osäker
# , most often Tandvårds- och läkemedelsförmånsverket, TLV)
        log.info('Creating details message')

        #title = 'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Odomzo 25 mars 2022 0_avslag-och-uteslutningar.pdf'
        return [{
            "role": "user",
            "content": 
                """Please extract: 
                limitations (list of limitations that applies to the reimbursement. May be none),
                previously_licensed_medicine (yes/no. Whether the active ingredient or the drug previously was a licensed medicine. Infomration found in section titled TLV gör följande bedömning or in the section titled Utredning i ärendet),
                analysis (what type of anaysis. cost-effectiveness, if cost per QALY is referred to in the text, cost-minimization (if the anaysis is about minimizing costs, sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys)), 
                form (list of form, strength, AIP and AUP of the medicinal product, the AIP and AUP have a blank char between hundreds and thousands, e.g. 100 000,00 means one hundred thousand)

                from the file named '{}'.
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename)
        }]

    def create_HE_message(self):

# El om analystyp
#  Fel om osäkerhetsbedömninb
# severity känns också osäker
# , most often Tandvårds- och läkemedelsförmånsverket, TLV)
# TODO: Added in dicvation to he: indication (Name of indication),
        log.info('Creating HE message')

        #title = 'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Odomzo 25 mars 2022 0_avslag-och-uteslutningar.pdf'
        return [
            {
            "role": "user",
            "content": 
                """Please extract: 
                picos (list of, for each PICO (combination of assessed Population cohort, Intervention, Control/Comparator, Outcome):
                    population (population this treatment option was evalauted for),
                    severity (the severity assessment is usually in a sentence like 'TLV bedömer (?:att )?svårighetsgraden'),
                    pediatric (pediatric population, yes/no),
                    prevalence (how many have this condition according to the report),
                    incidence (how many get diagnosed with this indication every year according to the report),
                    intervention (short description of the evaluated treatment),
                    comparators_company (list of name(s) of the product(s) that the company has compared the product against. Usually in section 'Utredning i ärendet'), 
                    comparator_modus_company (treatment regime used in the comparison by the company. Usually in section 'Utredning i ärendet'),
                    comparator_reason_company (reason provided by the company for using these comparators. Usually in section 'Utredning i ärendet'),
                    comparators_agency (list of name(s) of the product(s) that the TLV has compared the product against), 
                    comparator_modus_agency (treatment regime used in the comparison by TLV. Usually in the sub section 'TLV gör följande bedömning'),
                    comparator_reason_agency (reason provided by TLV for using these comparators. Usually in the sub section 'TLV gör följande bedömning'),
                    QALY_gain_company (the number of gained quality-adjusted life years (QALY) as calculated by the company if applicable. Usually in section 'Utredning i ärendet'),
                    QALY_total_cost_company (the total cost for the gained quality-adjusted life years (QALY) as calculated by the company if applicable. Usually in section 'Utredning i ärendet'),
                    ICER_company (the cost of a quality-adjusted life year (QALY) as calculated by the company if applicable. Usually in section 'Utredning i ärendet'),
                    QALY_gain_agency_lower (the lower number of gained quality-adjusted life years (QALY) as calculated by TLV if applicable. Usually in the sub section 'TLV gör följande bedömning'),
                    QALY_gain_agency_higher (the higher number of gained quality-adjusted life years (QALY) as calculated by TLV if applicable. Usually in the sub section 'TLV gör följande bedömning'),
                    QALY_total_cost_agency_lower (the lowest total cost for the gained quality-adjusted life years (QALY) as calculated by TLV if applicable. Usually in the sub section 'TLV gör följande bedömning'),
                    QALY_total_cost_agency_higher (the highest total cost for the gained quality-adjusted life years (QALY) as calculated by TLV if applicable. Usually in the sub section 'TLV gör följande bedömning'),
                    ICER_agency_lower (the lower cost of a quality-adjusted life year (QALY) as calculated by the TLV if applicable. Usually in the sub section 'TLV gör följande bedömning'),
                    ICER_agency_higher (the higher cost of a quality-adjusted life year (QALY) as calculated by the TLV if applicable. Usually in the sub section 'TLV gör följande bedömning'),
                    comparison_method (direct/indirect. If the statistical comparison between the drug and the comparators was made using a direct or indirect method),
                    indirect_method (for example Bayesian network meta-analysis, if applicable),
                    trials_company (list of all referred trials the company is referring to related to this PICO: 
                                    title of paper (if applicable), 
                                    number_of_patients (number of patients in the trial), 
                                    number_of_controls (number of patients in control arm, if applicable),
                                    indication (medical indication of the patients), 
                                    duration,
                                    phase (phase I or II or III or IV)
                                    meta-analysis (yes/no),
                                    randomized (yes/no),
                                    controlled (yes/no),
                                    blinded (single/double/no),
                                    primary_outcome_variable,
                                    results (list of outcome variables in all arms and if significance was achieved. Include p-value),
                                    safety (list of adverse events and side effects in all arms) 
                    ), 
                    costs (list of costs derived by the company and TLV for the product and comparator treatments:
                        assessor (company or agency. Who made the estimate),
                        product (intervention or comparator. Which treatment the costs relate to),
                        drug_cost (Total drug costs for the product),
                        other_costs (Other associated costs for the product),
                        total_treatment_cost (Total cost for the treatment using the product)
                    ),
                    efficacy_summary (A brief summary no longer than three sentences of TLV:s assessment of the product's efficacy for this pico),  
                    safety_summary (A brief summary no longer than three sentences of TLV:s assessment of the product's safety profile for this pico),  
                    decision_summary (A brief summary no longer than three sentences of TLV:s reasons for their decision for this pico),
                    uncertainty_assessment_clinical (how TLV assess the uncertainty of the clinical results presented by the company for this indication (low/medium/high/very high/not assesed)),
                    uncertainty_assessment_he (how TLV assess the uncertainty of the health economic results presented by the company for this indication (low/medium/high/very high/not assessed)),
                ),

                from the file named '{}'.
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename)
            }
        ]

    def find_staff_message(self):

        log.info('Creating staff message')
        return [
            {
            "role": "user",
            "content": 
                """Please extract 
                staff (list of people involved in the assessment and decision making. They are usually found in the section starting with "Detta beslut har fattats av Nämnden för läkemedelsförmåner hos TLV" and in the sentence starting with "Följande ledamöter har deltagit i beslutet):
                    name (name of each person),
                    profession (the profession, if provided, of this person),
                    dissent (yes/no/NA, possible dissent on the decision),
                    role (could be decision makers (swedish 'ledamöter'), presenter_to_the_board (swedish "föredragare") or non-voting board member 
                    (usually found in the sentence following the description of the presenter)),
                from the file named '{}'.
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename),
            }
        ]
    


    def query_if_requested_info(self):
        log.info('Did TLV ask for info?')
        return [
            {
            "role": "user",
            "content": 
                """Did TLV ask for more information from the company in the file named '{}'? Please extract
                    requested_complement (yes/no, if TLV requested the company to complement with additional information),
                    requested_information (what type of information TLV requested the company to complement the application nwith, if applicable),
                    requested_complement_submitted (yes/no/NA, if applicable, whether the company submitted the requested complmentary info),
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename),
            }
        ]

    def query_about_form(self):
        log.info('What forms?')
        return [
            {
            "role": "user",
            "content": 
                """On first page of the file named '{}' there is often a table listing the form and strength of the product that TLV has approved for reimbursement. Please extract
                    form_approved (list of form, strength, AIP and AUP of the medicinal product thatr were approved for reimbursement. The AIP and AUP have a blank char between hundreds and thousands, e.g. 100 000,00 means one hundred thousand),
                On the second page there may also be a table listing which forms and strengths that the company wanted reimbursed. Please extract
                    form_submitted (list of form, strength, AIP and AUP of the forms of the medicinal product that the company applied reimbursement for. The AIP and AUP have a blank char between hundreds and thousands, e.g. 100 000,00 means one hundred thousand),
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename),
            }
        ]
    
    def query_about_indications(self):
        log.info('What indications?')
        return [
            {
            "role": "user",
            "content": 
                """What medical indications and patient populations are evalauted in the file named '{}'? Please extract
                
                indications (list of medical indications the medicinal product is evaluated for and the associated severity (low, moderate, high, very high, varying, not assessed) assigned by TLV of each indication. 
                The indication information is usually found in section 'Ansökan', in the section 'Utredning i ärendet' and the severity assessment usually in the sub section 'TLV gör följande bedömning'. 
                The severity assessment is usually in a sentence like 'TLV bedömer (?:att )?svårighetsgraden'), 
                
                population_cohorts (list of population cohorts and their incidence and prevalance. Usually found in section 'Utredning i ärendet'),
                
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename),
            }
        ]

    def create_basic_message(self):
        log.info('Creating basic message')
        self.message = [ {
            "role": "user",
            "content": 
               """Please extract 
                title, 
                company (applicant. Only company name), 
                product name, 
                diarie number (sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning'),
                date (Found at the top of the first page. Format YYYY-MM-DD),
                decision (fully reimbursed, limited reimbursement, rejected), 
                currency (currency used in the document for prices and costs),
                requested_complement (yes/no, if TLV requested the company to complement with additional information),
                requested_information (what type of information TLV requested the company to complement the application nwith, if applicable),
                requested_complement_submitted (yes/no/NA, if applicable, whether the company submitted the requested complmentary info),
                indication (list of medical indications the medicinal product is evaluated for 
                and the associated severity (low, moderate, high, very high, varying, not assessed) of each indication. 
                The indication information is usually found in section 'Ansökan', in the section 'Utredning i ärendet' 
                and the severity assessment usually in the sub section 'TLV gör följande bedömning'. 
                The severity assessment is usually in a sentence like 'TLV bedömer (?:att )?svårighetsgraden'), 
                limitation (list of limitations, if any, that applies to the reimbursement),
                previously_licensed_medicine (yes/no. Whether the active ingredient or the drug previously was a licensed medicine. Infomration found in section 'TLV gör följande bedömning' or in the section 'Utredning i ärendet'),
                analysis (cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost benefit analysis (sv: kostnadsnyttoanalys)), 
                form (list of form, strength, AIP and AUP of the medicinal product, the AIP and AUP have a blank char between hundreds and thousands, e.g. 100 000,00 means one hundred thousand),
                population_cohorts (list of population cohorts and their incidence and prevalance. Usually found in section 'Utredning i ärendet'),
                decision_makers (list of names of decision makers and their profession and possible dissent on the decision), 
                presenter_to_the_board (name and title of presenter), and
                other_participants (list of names and professions of additional case managers beyond the presenter, 
                usually in the sentence following the description of the presenter.) 
                from the file named '{}'. If you cannot find the file, return title as 'not found' and the rest of the requested output as blanks. 
                Information that you cannot find should be returned as blank in the corresponding field.
                Make sure you get all the PICOs included in the picos output and all trials referred to by the company in trials_company output.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename),
            }
        ]

    def create_pico_message_company(self):
        # for the population_cohorts you found.
        log.info('Creating pico message company')
        return [ {
            "role": "user",
            "content": 
                """From the file, please extract the following company provided information (usually found in section 'Utredning i ärendet') for the population_cohorts you found. 
                picos_company: For each pico (combination of assessed Population_cohort, Intervention, Control/Comparator, Outcome), include:
                        pico_nr (a unique number (1,2...) for each pico),
                        indication (Name of indication),
                        population (population description),
                        prevalance (How many have this condition according to the report),
                        incidence (How many get diagnosed with this indication every year according to the report),
                        comparator_company (Name of the product that the company has compared the product against. Usually in section 'Utredning i ärendet'), 
                        comparator_modus_company (treatment regime used in the comparison by the company. Usually in section 'Utredning i ärendet'),
                        comparator_reason_company (reason provided by the company for using these comparators. Usually in section 'Utredning i ärendet'),
                        outcome_measure_company (Outcome measure used by the company. Usually in section 'Utredning i ärendet'),  
                        QALY_gain_company (the number of gained quality-adjusted life years (QALY) as calculated by the company, if applicable. Usually in section 'Utredning i ärendet'),
                        QALY_total_cost_company (the total cost for the gained quality-adjusted life years (QALY) as calculated by the company, if applicable. Usually in section 'Utredning i ärendet'),
                        ICER_company (the cost of a quality-adjusted life year (QALY) as calculated by the company, if applicable. Usually in section 'Utredning i ärendet'),
                        comparison_method (direct/indirect, if the statistical comparison between the drug and the comparators was made using a direct or indirect method),
                        indirect_method (for example Bayesian network meta-analysis, if applicable),
                        costs_company_product: 
                            drug_cost (Total drug costs for the product according to the company),
                            other_costs (Other associated costs for the product according to the company),
                            total_treatment_cost (Total cost for the treatment using the product according to the company),
                        costs_company_comparator: 
                            drug_cost (Total drug costs for the comparator according to the company),
                            other_costs (Other associated costs for the comparator according to the company),
                            total_treatment_cost (Total cost for the treatment using the comparator according to the company),
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in JSON format without any Markdown or code block formatting in english""", #JSON format without any Markdown or code block formatting ///plain text
            # Attach the new file to the message.
            #"attachments": [
            #  { "file_id": message_file.id, "tools": [{"type": "file_search"}] }
            #],
            }
        ]

    def create_pico_message_agency(self):
        # 
        log.info('Creating pico message agency')
        return [ {
            "role": "user",
            "content": 
                """"From the file, please extract the following TLV assessments (usually found in section 'TLV gör följande bedömning') for the population_cohorts you found.
                picos_agency: For each pico (combination of assessed Population_cohort, Intervention, Control/Comparator, Outcome), include:
                        pico_nr (the pico number this corresponds to),
                        indication (Name of indication),
                        population (population description),
                        severity (The associated severity (low/moderate/high/very high/varying/not assessed) of this indication. Usually in the sub section 'TLV gör följande bedömning'. 
                        The severity assessment is usually in a sentence like 'TLV bedömer (?:att )?svårighetsgraden'),
                        comparator_agency (Name of the product that the TLV has compared the product against), 
                        comparator_modus_agency (treatment regime used in the comparison by TLV. Usually in the sub section 'TLV gör följande bedömning'),
                        comparator_reason_agency (reason provided by TLV for using these comparators. Usually in the sub section 'TLV gör följande bedömning'),
                        outcome_measure_agency (Outcome measure used by the TLV. Usually in the sub section 'TLV gör följande bedömning'),  
                        QALY_gain_agency_lower (the lower number of gained quality-adjusted life years (QALY) as calculated by TLV, if applicable. Usually in the sub section 'TLV gör följande bedömning'. If only one value is provided then write this value for both the lower and higher variable),
                        QALY_gain_agency_higher (the higher number of gained quality-adjusted life years (QALY) as calculated by TLV, if applicable. Usually in the sub section 'TLV gör följande bedömning'),
                        QALY_total_cost_agency_lower (the lowest total cost for the gained quality-adjusted life years (QALY) as calculated by TLV, if applicable. Usually in the sub section 'TLV gör följande bedömning'. If only one value is provided then write this value for both the lower and higher variable),
                        QALY_total_cost_agency_higher (the highest total cost for the gained quality-adjusted life years (QALY) as calculated by TLV, if applicable. Usually in the sub section 'TLV gör följande bedömning'),
                        ICER_agency_lower (the lower cost of a quality-adjusted life year (QALY) as calculated by the TLV, if applicable. Usually in the sub section 'TLV gör följande bedömning'. If only one value is provided then write this value for both the lower and higher variable),
                        ICER_agency_higher (the higher cost of a quality-adjusted life year (QALY) as calculated by the TLV, if applicable. Usually in the sub section 'TLV gör följande bedömning'),
                        costs_agency_product: 
                            drug_cost (Total drug costs for the product according to TLV),
                            other_costs (Other associated costs for the product according to TLV),
                            total_treatment_cost (Total cost for the treatment using the product according to TLV),
                        costs_agency_comparator: 
                            drug_cost (Total drug costs for the comparator according to TLV),
                            other_costs (Other associated costs for the comparator according to TLV),
                            total_treatment_cost (Total cost for the treatment using the comparator according to TLV,
                        efficacy_summary (A brief summary, no longer than three sentences, of TLV:s assessment of the product's efficacy for this indication),  
                        safety_summary (A brief summary, no longer than three sentences, of TLV:s assessment of the product's safety profile for this indication),  
                        decision_summary (A brief summary, no longer than three sentences, of TLV:s reasons for their decision for this indication),
                        uncertainty_assessment_clinical (how TLV assess the uncertainty of the clinical results presented by the company for this indication (low,/medium/high/ery high/not assessed)),
                        uncertainty_assessment_he (how TLV assess the uncertainty of the health economic results presented by the company for this indication (low/medium/high/very high/not assessed)),
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in JSON format without any Markdown or code block formatting in english""",
                }
        ]

    def create_summary_message(self):

# El om analystyp
#  Fel om osäkerhetsbedömninb
# severity känns också osäker
# , most often Tandvårds- och läkemedelsförmånsverket, TLV)
        log.info('Creating summary message')

        #title = 'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Odomzo 25 mars 2022 0_avslag-och-uteslutningar.pdf'
        return [{
            "role": "user",
            "content": 
                """Please summarize the outcome of the agency assessment in terms of: 
                limitations (list of limitations that applies to the reimbursement. May be none),
                efficacy_summary (A brief summary, no longer than three sentences, of TLV:s assessment of the product's efficacy),  
                safety_summary (A brief summary, no longer than three sentences, of TLV:s assessment of the product's safety profile),  
                decision_summary (A brief summary, no longer than three sentences, of TLV:s reasons for the reimbursement decision),
                from the file named '{}'.
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_file.filename)
        }]

    def create_trial_message_company(self):
        log.info('Creating trial message company')
        return [ {
            "role": "user",
            "content": 
                """From the file, please extract the following company provided information (usually found in section 'Utredning i ärendet') for the population_cohorts you found.
                trials: For each population_cohort, list all trials the company is referring to),
                        pico_nr (The pico this trial relates to),
                        title (title of paper if applicable), 
                        number_of_patients (number of patients in the trial), 
                        number_of_controls (number of patients in control arm, if applicable),
                        indication (medical indication of the patients), 
                        duration (the duration of the trial),
                        phase (phase I, II, III or IV),
                        meta-analysis (yes/no),
                        randomized (yes/no),
                        controlled (yes/no),
                        blinded (single, double, no),
                        primary_outcome_variable),
                        results (list of outcome variables in all arms and if significance was achieved, include p-value),
                        safety (description of adverse events, side effects in all arms), 
                        
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in JSON format without any Markdown or code block formatting in english""", #JSON format without any Markdown or code block formatting ///plain text
            # Attach the new file to the message.
            #"attachments": [
            #  { "file_id": message_file.id, "tools": [{"type": "file_search"}] }
            #],
            }
        ]
    
    def create_thread(self, message=''):
        log.info('Creating thread')
        self.thread = self.client.beta.threads.create(
            messages= self.message,
            tool_resources= {
                "file_search": {
                    "vector_store_ids": [
                        self.vector_store.id
                    ]
                }
            }
        )

    def run_parsing(self, additional_messages=None, additional_instructions=None):
        log.info('Running parsing')
        attempt = 1
        while attempt<MAX_NR_OF_RETRIES:
            try:
                with self.client.beta.threads.runs.stream(
                    thread_id=self.thread.id,
                    assistant_id=self.assistant.id,
                    additional_messages=additional_messages,
                    additional_instructions= additional_instructions,
                    event_handler=EventHandler.EventHandler()
                ) as stream:
                    stream.until_done()
                # Let's make sure it finished correctly
                runs = self.client.beta.threads.runs.list(self.thread.id)
                run = runs.data[0]
                if run.status!='completed':
                    raise(Exception('OpenAI run failed. Status: ' + run.status + ' with reason ' + run.incomplete_details.reason))
                else:
                    # we're done
                    break
            except Exception as e:
                attempt = attempt + 1
                time.sleep(attempt)
                log.error('OpenAI streaming failed.', exc_info=sys.exc_info())
                 # Let's kill that run
                runs = self.client.beta.threads.runs.list(self.thread.id)
                # Seems the most recent one is nr 0
                run = runs.data[0]
                q=0
                for r in runs:
                    q=q+1
                    log.info('Run ' +str(q) + ' status is ' + r.status)
                if (run.status!='cancelled') & (run.status!='completed') & (run.status!='failed') & (run.status!='expired'):
                    run = self.client.beta.threads.runs.cancel(
                        thread_id=self.thread.id,
                        run_id=runs.data[0].id
                        )
                    wait = 0
                    while (run.status!='cancelled') & (run.status!='completed') & (run.status!='failed') & (run.status!='expired') & (wait<16):
                        time.sleep(wait)
                        wait = wait + 2
                        run = self.client.beta.threads.runs.retrieve(
                            thread_id=self.thread.id,
                            run_id=runs.data[0].id
                            )
                        log.info('waiting for run to cancel')
                log.info('Trying again. Attempt: ' + str(attempt))

    def run_formatted_parsing(self, additional_messages=None, additional_instructions=None, format = None):
        log.info('Running parsing')
        attempt = 1
        while attempt<MAX_NR_OF_RETRIES:
            try:
                with self.client.beta.threads.runs.stream(
                    thread_id=self.thread.id,
                    assistant_id=self.assistant.id,
                    response_model=format,
                    additional_messages=additional_messages,
                    additional_instructions= additional_instructions,
                    event_handler=EventHandler.EventHandler(),
                ) as stream:
                    stream.until_done()
                break
            except Exception as e:
                attempt = attempt + 1
                time.sleep(attempt)
                log.error('OpenAI streaming failed.', exc_info=sys.exc_info())
                log.info('Trying again. Attempt: ' + str(attempt))
     

    def run_parsing_w_addon(self, additional_messages=None):
        log.info('Running parsing')
        try:
            with self.client.beta.threads.runs.stream(
                thread_id=self.thread.id,
                assistant_id=self.assistant.id,
                additional_messages=additional_messages,
                event_handler=EventHandler.EventHandler(),
            ) as stream:
                stream.until_done()
        except Exception as e:
            log.error('OpenAI streaming failed.', exc_info=sys.exc_info())
    
    def rerun_parsing_indications(self, additional_messages=None):
        log.info('Rerunning parsing indications')
        try:
            with self.client.beta.threads.runs.stream(
                thread_id=self.thread.id,
                assistant_id=self.assistant.id,
                additional_instructions='You missed extracting indications and population cohorts. Please have another go.',
                additional_messages=additional_messages,
                event_handler=EventHandler.EventHandler(),
            ) as stream:
                stream.until_done()
        except Exception as e:
            log.error('OpenAI streaming failed.', exc_info=sys.exc_info())

    def rerun_parsing_picos(self, additional_messages=None):
        log.info('Rerunning parsing picos')
        try:
            with self.client.beta.threads.runs.stream(
                thread_id=self.thread.id,
                assistant_id=self.assistant.id,
                additional_instructions='I think you missed some of the PICO:s. Please have another go.',
                additional_messages=additional_messages,
                event_handler=EventHandler.EventHandler(),
            ) as stream:
                stream.until_done()
        except Exception as e:
            log.error('OpenAI streaming failed.', exc_info=sys.exc_info())

    def rerun_parsing_icer(self, additional_messages=None):
        log.info('Running parsing')
        try:
            with self.client.beta.threads.runs.stream(
                thread_id=self.thread.id,
                assistant_id=self.assistant.id,
                additional_instructions='I think you missed to extract ICER and/or QALY gains or costs for some of the PICO:s. Please have another go.',
                additional_messages=additional_messages,
                event_handler=EventHandler.EventHandler(),
            ) as stream:
                stream.until_done()
        except Exception as e:
            log.error('OpenAI streaming failed.', exc_info=sys.exc_info())

    def rerun_parsing_costs(self, additional_messages=None):
        log.info('Running parsing')
        try:
            with self.client.beta.threads.runs.stream(
                thread_id=self.thread.id,
                assistant_id=self.assistant.id,
                additional_instructions='I think you missed to extract drug and treatment costs for some of the PICO:s. Please have another go.',
                additional_messages=additional_messages,
                event_handler=EventHandler.EventHandler(),
            ) as stream:
                stream.until_done()
        except Exception as e:
            log.error('OpenAI streaming failed.', exc_info=sys.exc_info())


    def rerun_parsing(self, thread=None):
        log.info('Running parsing to find more picos')
        try:
            with self.client.beta.threads.runs.stream(
                thread_id=self.thread.id,
                additional_instructions='Are you sure you got all population_cohorts? Please rerun and see if you can find more info in this regard from the document.',
                assistant_id=self.assistant.id,
                event_handler=EventHandler.EventHandler(),
            ) as stream:
                stream.until_done()
        except Exception as e:
            log.error('OpenAI streaming failed.', exc_info=sys.exc_info())


    def rerun_missing_parsing(self, thread=None):
        log.info('Running parsing')
        try:
            with self.client.beta.threads.runs.stream(
                thread_id=self.thread.id,
                additional_instructions='Seems you missed picos for one or several of the population_cohorts. Please rerun and see if you can find more info in this regard from the document.',
                assistant_id=self.assistant.id,
                event_handler=EventHandler.EventHandler()
            ) as stream:
                stream.until_done()
        except Exception as e:
            log.error('OpenAI streaming failed.', exc_info=sys.exc_info())
            # Let's kill that run
            runs = self.client.beta.threads.runs.list(self.thread.id)
            run = self.client.beta.threads.runs.cancel(
                thread_id=self.thread.id,
                run_id=runs.data[-1].id
                )
            wait = 0
            while (run.status!='cancelled' & run.status!='completed' & run.status!='failed' & run.status!='expired' & wait<16):
                time.sleep(2)
                wait = wait + 2
                run = self.client.beta.threads.runs.retrieve(
                    thread_id=self.thread.id,
                    run_id=runs.data[-1].id
                    )
                log.info('waiting for run to cancel')

    def get_results(self):
        res = None
        annotated_citations = None
        log.info('Retrieving the result')
        attempt = 1
        while attempt<MAX_NR_OF_RETRIES:
            try:
                # Get the last message from the thread
                message = self.client.beta.threads.messages.retrieve(
                    thread_id=self.thread.id,
                    message_id=self.client.beta.threads.messages.list(thread_id=self.thread.id,order="desc").data[0].id
                )
                message_text_object = message.content[0]
                message_text_content = message_text_object.text.value  

                runs = self.client.beta.threads.runs.list(self.thread.id)
                k=0
                for r in runs:
                    k=k+1
                    log.info('Prompt tokens used in run ' + str(k) + ': ' + str(r.usage.prompt_tokens))
                    log.info('Total tokens usedin run ' + str(k) + ': ' + str(r.usage.total_tokens))
                
                #log.info('Result before citation matching')
                #log.info(message_text_content)
                annotations = message_text_object.text.annotations  

                # Create a list to store annotations with a dictionary for citation replacement
                annotated_citations = []
                citation_replacements = {}

                # Iterate over the annotations, retrieve file names, and store the details
                for index, annotation in enumerate(annotations):
                    log.info('Annotation nr ' + str(index))
                    annotation_number = index + 1

                    # Retrieve the file name using the file ID
                    file_info = self.client.files.retrieve(annotation.file_citation.file_id)
                    file_name = file_info.filename

                    annotation_details = {
                        "number": annotation_number,
                        "text": f"[{annotation_number}]",
                        "file_name": file_name,
                        "start_index": annotation.start_index,
                        "end_index": annotation.end_index,
                    }
                    annotated_citations.append(annotation_details)
                    citation_replacements[annotation.text] = f"[{annotation_number}]"

                # Replace the inline citations in the message text with numbered identifiers
                for original_text, replacement_text in citation_replacements.items():
                    message_text_content = message_text_content.replace(original_text, replacement_text)

                log.info('Result')
                log.info(message_text_content)
                
                res = message_text_content #self.json_parse(message_text_content)
                res = res.replace('\n','')

                log.info('\nSources')
                for annotation in annotated_citations:
                    log.info(f"Annotation {annotation['number']}:")
                    log.info(f"  File Name: {annotation['file_name']}")
                    log.info(f"  Character Positions: {annotation['start_index']} - {annotation['end_index']}")
                    log.info("")  # Add a blank line for readability            
            
            except Exception as e:
                attempt = attempt + 1
                time.sleep(attempt)
                log.error('OpenAI streaming failed.', exc_info=sys.exc_info())
                log.info('Trying again. Attempt: ' + str(attempt))
            
            finally:
                return res, annotated_citations
        