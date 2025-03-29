import os
import re
import openai
import EventHandler
import json
from logger_tt import getLogger
#from json_model2 import HTA_Document, HTA_Agency, Product, Company, PICO, Analysis, Staff, Expert, Decision_Maker, Indication_Simplified
from pydantic_models import HTA_Document_Basis, PICO, HTA_Document_Staff, Team, PICOs_Partial_Agency, PICOs_Partial_Company,\
    PICO_Analysis_Cost_Company, PICO_Analysis_Cost_Agency, Costs, Panel, PICOs, PICOs_comp, PICOs_ag, Analysis_List_Comp, Analysis_List_Ag, Analysis_List, Trials, References
import document_splitting as ds
from typing import Iterable
import time
import json
from openai import OpenAI
import instructor
import sys
import tempfile
from secret import secrets
SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'


client2 = OpenAI(
    api_key=secrets.open_ai_key,
    max_retries=4, 
    timeout=40.0)

client_instructor = instructor.from_openai(OpenAI(
    api_key=secrets.open_ai_key, 
    max_retries=4, 
    timeout=60.0))

log = getLogger(__name__)

MAX_NR_OF_RETRIES = 5

MAX_WAIT_TIME_STORAGE = 10

def replacer_company(res, res_pico_comp):
    k=0
    for pico in res_pico_comp['picos']:
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
    for pico in res_pico_agency['picos']:
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

class Worker_basis:

    def __init__(self, client, doc_type = 'basis', model = 'gpt-4o'):
        log.info("Creating worker for basis")
        self.doc_type = doc_type
        self.client = client_instructor # TODO OBS client2
        self.model = model
        self.create_assistant(model=model)
        self.vector_store = None
        self.message_files = []
    
    def clean_up(self):
        self.delete_files()
        if self.vector_store:
            self.delete_vs()
        self.delete_assistant()

    def delete_files(self):
        for f in self.message_files:
            response = self.client.files.delete(f['id'])
            log.info('Deleted file ' + f['filename'] + ': ' + str(response))

    def delete_vs(self):
        response = self.client.beta.vector_stores.delete(vector_store_id=self.vector_store.id)
        log.info('Deleted vector store: ' + self.vector_store.name + ': ' + str(response))

    def delete_assistant(self):
        response = self.client.beta.assistants.delete(self.assistant.id)
        log.info(response)

    def get_ICD(self, res):
        indications = {'indications':[]}
        for ind in res['indications']:
            log.info('Get ind: ' + ind['indication'])
            icd_10 = self.get_response(self.get_ICD_code(ind['indication']))
            # truncate to only one decimal
            icd_10 = re.sub(r'(?<=)\.\d*','', icd_10) # we only need the first part
            log.info('Got ICD: ' + icd_10)
            indications['indications'].append({'icd_10':icd_10,'severity':ind['severity']})
        return indications

    @staticmethod
    def json_parse(data):
        try:
            # sometimes shit is added 
            if data[0:3]=='```':
                log.info('Fixing shit')
                data = re.sub('```|json', '',data)
            #data = data.split('{')[-1]
            #data = data.split('}')[0]
            #data = '{' + data + '}'
            log.info('Fixed:')
            log.info(data)
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
    
    def initial_parsing(self):
        # sometimes it fail. I think is is due to not accessing the file even though it is said to be ready. Let's try a wait and give it a go again
        title = 'not found'
        self.run_parsing()
        ass_results, annot = self.get_results()
        #data, annot = parser.parse_file(file_dir + file)
        res_json_pre = self.json_parse(ass_results)
        # Get the icd and convert to proper indications form
        indications = self.get_ICD(res_json_pre)
        res_json_pre.update(indications)
        res_json_pre = json.dumps(res_json_pre)
        res = self.run_json_parsing(res_json_pre, HTA_Document_Basis)
        res_json = res.model_dump()
        log.info('After initial parsing and pydantic transfer')
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

    def parse_file_2(self, file_path):
        results_doc = None
        self.file_path = file_path
        try: 
            if self.vector_store:
                deleted_vector_store = self.client.beta.vector_stores.delete(vector_store_id=self.vector_store.id)
            self.add_files(file_path)

            self.create_auto_vector_store(file_path.split('/')[-1])
            #self.attach_vector_store_to_assistant()  Nope, adding to thread instedad 
            if self.doc_type=='basis':
                self.create_basis_message()
            else:
                self.create_basis_NT_message()

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
            # Update with correct title
            res['title'] =  file_path.split('/')[-1].split('.')[0]   

            log.info('Get staff')
            self.run_parsing(additional_messages=self.find_staff_message())    
            ass_results_staff, annot = self.get_results()
            res_staff = self.run_json_parsing(ass_results_staff, Panel)
            res_staff_json = res_staff.model_dump()
            log.info(res_staff_json)
            #res_staff = self.run_and_parse(self.find_staff_message())
            res.update(res_staff_json) 
            log.info('After adding staff')
            log.info(res)

            log.info('Get the summaries')
            self.run_parsing(additional_messages=self.create_summary_message())    
            ass_results_sum, annot = self.get_results()
            res_sum = self.run_json_parsing(ass_results_sum, HTA_Document_Basis)
            #res_sum = self.run_and_parse(self.create_summary_message())
            res_sum_json = res_sum.model_dump()
            res_sum_json = {k: res_sum_json[k] for k in ('analysis', 'efficacy_summary', 'safety_summary','decision_summary',
                                          'uncertainty_assessment_clinical', 'uncertainty_assessment_he',
                                          'limitations', 'requested_complement', 'requested_information',
                                          'requested_complement_submitted', 'previously_licensed_medicine')} # otherwise the extra empty variables are overwrting the good stuff from initial parsing
            res.update(res_sum_json) 
            log.info('After adding summaries')
            log.info(res)

            log.info('Getting indications')
            res_ind = self.run_and_parse(self.query_about_indications())

            if len(res_ind['indications'])==0:
                log.info('Parser missed indications and populations. redo')
                res_ind = self.run_and_parse(message=self.query_about_indications(), additional_instructions='You missed extracting indications and population cohorts. Please have another go.')
        
            # get icds
            #indications = {'indications':[]}
            indications =self.get_ICD(res_ind)
            res.update(indications)
            log.info('After adding inds')
            log.info(res)

            if len(self.message_files)>1: # TODO Finer chunking for results chapter
                self.add_files_to_vs(self.message_files[1:])
            else:
                log.info('Lacking chapters for file {}'.format(self.file_path))

            res = self.get_refs(res)

            # PICOs
            log.info('Get the picos')
            self.run_parsing(additional_messages=self.create_pico_summary_company())    
            ass_results_pico, annot = self.get_results()
            res_pico_comp = self.run_json_parsing(ass_results_pico, PICOs_comp)
            res_pico_comp_json = res_pico_comp.model_dump()
            #log.info('Before: ')
            #log.info(res_pico_comp_json)
            #res_pico_str = json.dumps(res_pico_comp_json)
            #res_pico_str = res_pico_str.replace("'","\'")
            #log.info('After: ')
            #res_pico_comp_json = json.loads(res_pico_str)
            log.info(res_pico_comp_json)

            self.run_parsing(additional_messages=self.create_pico_summary_agency())    
            ass_results_pico2, annot = self.get_results()
            res_pico_agency = self.run_json_parsing(ass_results_pico2, PICOs_ag)
            res_pico_agency_json = res_pico_agency.model_dump()

            # to avoid overwriting when mergin the comp and agency picos
            res_pico_comp_json_reduced = {'picos': []}
            for row in res_pico_comp_json['picos']:
                res_pico_comp_json_reduced['picos'].append({k: v for k, v in row.items() if k in ['indication', 'population', 'prevalance', 'incidence',
                                                                                       'pediatric', 'co_medication', 'intervention','comparator_company',
                                                                                       'comparator_modus_company', 'comparator_reason_company', 'outcome_measure_company']})
            res_pico_agency_json_reduced = {'picos': []}
            for row in res_pico_agency_json['picos']:
                res_pico_agency_json_reduced['picos'].append({k: v for k, v in row.items() if k in ['indication', 'population', 'severity', 'comparator_agency',
                                                                                           'comparator_modus_agency', 'comparator_reason_agency', 'outcome_measure_agency']})
            #res_pico_comp = self.run_and_parse(self.create_pico_summary_company())
            #res_pico_agency = self.run_and_parse(self.create_pico_summary_agency())
            k=0
            for entry_comp, entry_ag in zip(res_pico_comp_json_reduced['picos'], res_pico_agency_json_reduced['picos']):
                log.info('pico comp to add: ')
                log.info(entry_comp)
                log.info('pico ag to add: ')
                log.info(entry_ag)
                res['picos'].append({**entry_comp,**entry_ag})
                res['picos'][k]['analysis'] = [] # Initiate, populate later
                k=k+1

            #res_pico = PICO(indication=res_pico_agency.picos[0].indication
            #res_pico_comp_json.update(res_pico_agency_json)
            #log.info(json.dumps(res_pico_comp_json))
            #res = replacer_company(res, res_pico_comp_json)
            #res = replacer_agency(res, res_pico_agency_json) 

            # Assign icd codes for each pico
            for k in range(0,len(res['picos'])):
                icd = self.get_response(self.get_ICD_code(res['picos'][k]['indication']))
                # truncate to only one decimal
                icd = re.sub(r'(?<=)\.\d*','', icd)
                log.info('PICO icd code: ' + icd)
                res['picos'][k]['icd_code'] = icd
                res['picos'][k]['population']['icd_code'] = icd

            log.info('After adding picos')
            log.info(res)

            log.info('Get the analysis')
            self.run_parsing(additional_messages=self.create_analysis_summary_company())    
            ass_results_an, annot = self.get_results()
            log.info('The analysis result')
            log.info(ass_results_an)
            pico_list = [int(x['pico_nr'])-1 for x in json.loads(ass_results_an)['analyses']] # need to start at 0
            res_analysis_comp = self.run_json_parsing(ass_results_an, Analysis_List_Comp)
            res_analysis_comp_json = res_analysis_comp.model_dump()

            self.run_parsing(additional_messages=self.create_analysis_summary_agency())    
            ass_results_an2, annot = self.get_results()
            res_analysis_agency = self.run_json_parsing(ass_results_an2, Analysis_List_Ag)
            res_analysis_agency_json = res_analysis_agency.model_dump()
            #res_analysis_comp = self.run_and_parse(self.create_analysis_summary_company())
            #res_analysis_agency = self.run_and_parse(self.create_analysis_summary_agency())

            # to avoid overwriting when merging the comp and agency analyses
            res_analysis_comp_json_reduced = {'analyses':[]}
            costs = {'costs':[]}
            k=0
            for row in res_analysis_comp_json['analyses']:
                res_analysis_comp_json_reduced['analyses'].append({k: v for k, v in row.items() if k in ['QALY_gain_company', 'QALY_total_cost_company', 'ICER_company', 
                                                                                                         'comparison_method','indirect_method', 'co_medication', 'intervention',
                                                                                                         'comparator_company','comparator_modus_company', 'comparator_reason_company', 
                                                                                                         'outcome_measure_company']})
                res_analysis_comp_json_reduced['analyses'][k]['costs'] = row['costs'] # returns a list of costs with two entries one for the product and one for the comparator 
                k=k+1                

            res_analysis_agency_json_reduced = {'analyses':[]}
            k=0
            for row in res_analysis_agency_json['analyses']:
                res_analysis_agency_json_reduced['analyses'].append({k: v for k, v in row.items() if k in ['QALY_gain_agency_lower', 'QALY_gain_agency_higher', 
                                                                                                           'QALY_total_cost_agency_lower', ' QALY_total_cost_agency_higher',
                                                                                                           'ICER_agency_lower', 'ICER_agency_higher'
                                                                                                            'efficacy_summary', ' safety_summary', 'decision_summary',
                                                                                                            'uncertainty_assessment_clinical', 'uncertainty_assessment_he']})
                res_analysis_agency_json_reduced['analyses'][k]['costs'] = row['costs']
                k=k+1 
                
            k=0
            for entry_comp, entry_ag in zip(res_analysis_comp_json_reduced['analyses'], res_analysis_agency_json_reduced['analyses']):
                log.info('analysis comp to add: ')
                log.info(entry_comp)
                log.info('analysis ag to add: ')
                log.info(entry_ag)
                costs = [entry_ag['costs'], entry_comp['costs']]
                res['picos'][pico_list[k]]['analysis'] = dict({**entry_comp,**entry_ag})
                res['picos'][pico_list[k]]['analysis']['costs'] = costs
                k=k+1

            log.info('Get the trials')
            self.run_parsing(additional_messages=self.create_research_summary())    
            ass_results_trials, annot = self.get_results()
            pico_list = [int(x['pico_nr'])-1 for x in json.loads(ass_results_trials)['trials']]
            res_trials = self.run_json_parsing(ass_results_trials, Trials)
            res_trials_json = res_trials.model_dump()
            log.info('Trial')
            log.info(res_trials_json)
            # Initialize
            for p in res['picos']:
                p['analysis']['trials'] = []
            k=0
            for t in res_trials_json['trials']:
                res['picos'][pico_list[k]]['analysis']['trials'].append(t)
                k=k+1

            # sometimes some refs are missed in the first run
            res = self.get_refs(res)

            log.info('Before pydantic extraction')
            log.info(json.dumps(res))

            with open(SAVE_PATH + "before_pydant.json", "w") as outfile:
                outfile.write(json.dumps(res))
            #return ass_results, ass_results_pico_comp, ass_results_pico_agency, ass_results_trial_comp
            log.info('Extracting document')
            results_doc = self.run_json_parsing(res, HTA_Document_Basis)

            with open(SAVE_PATH + "after_pydant.json", "w") as outfile:
                outfile.write(json.dumps(results_doc.model_dump_json()))
        
        except Exception as e:
            log.error('Parsing failed', exc_info=sys.exc_info())
            results_doc = None
        
        finally:
            self.clean_up()
            return results_doc

   
    def get_refs(self, res):
        log.info('Get the references')
        self.run_parsing(additional_messages=self.get_references())
        ass_results_refs, annot = self.get_results() 
        res_refs = self.run_json_parsing(ass_results_refs, References)
        res_refs_json = res_refs.model_dump()

        res.update(res_refs_json) 
        log.info('After adding references')
        log.info(res)
        return res

    def create_assistant(self, model="gpt-4o-mini"): #"gpt-4o-mini"
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
                model="gpt-4o-2024-08-06",
                response_model=format,
                messages=[
                    {"role": "system", "content": "You are a useful assistant. Extract information according to the speficied response model. Make sure to include the nested objects, such as "},#Convert the user input into JSON. Make sure to include the nested objects, such as Product and HTA_Agency.
                    {"role": "user", "content": f"Extract:\n{input}"}
                ]
                )
                return completion#.choices[0].message
            except Exception as e:
                attempt = attempt + 1
                time.sleep(attempt)
                log.error('JSON parsing failed for file {}'.format(self.file_path), exc_info=sys.exc_info())
        return completion#.choices[0].message

    
    def get_response(self, messages): 
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
    
    def create_auto_vector_store(self, name):
        log.info("Auto vector store creation named {} initiated".format(name))
        try:
            self.vector_store = self.client.beta.vector_stores.create(
                name=name,
                chunking_strategy= {
                    "type" : 'static',
                    "static" : {
                    "max_chunk_size_tokens":4096, #800 even more expensive,#400 even more expensive,2000 works well but expensive?,#150, #200,
                    "chunk_overlap_tokens": 1000#200#200, 00#70, #100
                    }
                },
                 file_ids=[self.message_files[0]['id']],
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

        
    def attach_vector_store_to_assistant(self):
        log.info('Attaching vector store to assistant')
        self.assistant = self.client.beta.assistants.update(
                assistant_id=self.assistant.id,
                tool_resources={"file_search": {"vector_store_ids": [self.vector_store.id]}})


    def add_files(self, file_path):
        # start by splitting the file 
        with tempfile.TemporaryDirectory() as tmpdirname:
            generated_files = ds.split_preamble_and_chapters_safe(file_path, tmpdirname)
            # add each chapter file
            for f in generated_files:
                with open(f, "rb") as file:
                    log.info('Adding file ' + f)
                    stored_file = json.loads(self.client.files.create(file=file, purpose="assistants").to_json())
                    
                    self.message_files.append(stored_file)
                    log.info('Succesfully added ' + f)


    def add_files_to_vs(self, files):
        nr_of_files = len(files)
        batch_add = self.client.beta.vector_stores.file_batches.create_and_poll(
                vector_store_id=self.vector_store.id,
                chunking_strategy= {
                            "type" : 'static',
                            "static" : {
                            "max_chunk_size_tokens":150,#400 may be too high. Misses second population for Lynparza,#150 works well but expensive, #200,
                            "chunk_overlap_tokens":70#150#70, #100
                            }
                        },
                file_ids=[f['id'] for f in files]
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
                     
        
    def create_basis_message(self):
        log.info('Creating basis message')
        self.message = [
            {
            "role": "user",
            "content": 
                """Please extract: 
                title (Title of the document. On the front page), 
                company (applicant. Only company name. Usually found in a table on page 2 or 3. Capitalize each word in the company name), 
                product name (only the trade name of the product), 
                diarie number (sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning' or 'diarienummer'),
                date (Usually found on the first page. Format YYYY-MM-DD. Look for 'Datum för nämndmöte'),
                agency (name of the agency evaluating the product), 
                currency (currency used in the document for prices and costs),
                application_type (Usually found in a table on page 2 or 3),
                latest_decision_date (sv. 'Sista beslutsdag'. Usually found in a table on page 2 or 3),
                indications (list of:
                  indication (medical indications the medicinal product is evaluated for. The indication information is usually found in a table on page 2 or 3),
                  severity (the associated severity (low/moderate/high/very high/varying/not assessed) of this indication. The severity assessment is usually found in a table on page 2 or 3 (sv. 'svårighetsgrad')),
                  prevalence (number of affected persons. Usually found in a table on page 2 or 3)
                ), 
                comparators (list of product to compare against, comparators, sv. 'Relevant jämförelsealternativ'. Usually found in a table on page 2 or 3),
                annual_turnover (estimated annual turnover. Usually found in a table on page 2 or 3),
                threee_part_deal (yes/no. Whether a three-part negotiation (sv. 'treparts' or 'sidoöverenskommelse') took place),
                from the file named '{}'. If you cannot find the file, return title as 'not found' and the rest of the requested output as blanks. 
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files[0]['filename']),
            }
        ]

    def create_basis_NT_message(self):
        log.info('Creating basis_NT message')
        self.message = [
            {
            "role": "user",
            "content": 
                """Please extract: 
                title (Title of the document. On the front page), 
                company (applicant. Only company name. Usually found in a table on page 2 or 3. Capitalize each word in the company name), 
                product name (only the trade name of the product), 
                diarie number (sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning' or 'diarienummer'),
                date (Usually found on the first page. Format YYYY-MM-DD. Look for 'Datum för beslut av underlag'),
                agency (name of the agency evaluating the product), 
                currency (currency used in the document for prices and costs),
                application_type (set to 'NT-council evaluation'),
                indications (list of:
                  indication (medical indications the medicinal product is evaluated for. The indication information is usually found in a table on page 2 or 3),
                  severity (the associated severity (low/moderate/high/very high/varying/not assessed) of this indication. The severity assessment is usually found in a table on page 2 or 3 (sv. 'svårighetsgrad')),
                  prevalence (number of affected persons. Usually found in a table on page 2 or 3)
                ),  
                comparators (list of product to compare against, comparators, sv. 'Relevant jämförelsealternativ'. Usually found in 'TLV:s bedömning och sammanfattning'),
                from the file named '{}'. If you cannot find the file, return title as 'not found' and the rest of the requested output as blanks. 
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files[0]['filename']),
            }
        ]

    def find_staff_message(self):

        log.info('Creating staff message')
        return [
            {
            "role": "user",
            "content": 
                """Please extract 
                staff (list of people involved in the assessment. They are usually found at the bottom of page 2 or 3 following the word 'Arbetsgrupp': 
                    name (name of each person),
                    profession (the profession, if provided, of this person),
                    role (set to 'assessor'),
                    dissent (set to no)
                ),
                experts (list of possible engaged experts. usually found at the bottom of page 2 or 3 following the word 'Klinisk expert': 
                    name (name of each person),
                    profession (the profession, if provided, of this person)
                ) 
                from the file named '{}'.
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files[0]['filename']),
            }
        ]

    def create_summary_message(self):
        log.info('Creating summary message')
        return [
            {
            "role": "user",
            "content": 
                """Please extract
                analysis (cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost benefit analysis (sv: kostnadsnyttoanalys)), 
                efficacy_summary (A brief summary no longer than three sentences of TLV:s assessment of the product's efficacy),  
                safety_summary (A brief summary no longer than three sentences of TLV:s assessment of the product's safety profile),  
                decision_summary (A brief summary no longer than three sentences of TLV:s reasons for their decision),  
                uncertainty_assessment_clinical (how TLV assess the uncertainty of the clinical results presented by the company (low,/medium/high/ery high/not assessed)),
                uncertainty_assessment_he (how TLV assess the uncertainty of the health economic results presented by the company (low/medium/high/very high/not assessed)),
                limitations (list of limitations that applies to the reimbursement. May be none. ),
                requested_complement (yes/no. If TLV requested the company to complement with additional information),
                requested_information (what type of information TLV requested the company to complement the application with if applicable),
                requested_complement_submitted (yes/no/NA. If applicable whether the company submitted the requested complementary info),
                previously_licensed_medicine (yes/no. Whether the active ingredient or the drug previously was a licensed medicine. Infomration found in section 'TLV gör följande bedömning' or in the section 'Utredning i ärendet'),
                from the file named '{}'. You can expect to fins most of this information in the section 'TLV:s bedömning och sammanfattning' or 'TLV:s centrala utgångspunkter och bedömningar'. If you cannot find the file, return title as 'not found' and the rest of the requested output as blanks. 
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files[0]['filename']),
            }
        ]
    
    def get_references(self):
        log.info('Creating reference message')
        return [ {
            "role": "user",
            "content": 
                """"From the files in the vector store, extract the references. Specifically, look in the reference list (sv. Referenser) to get:
                references: 
                    ref_nr (a unique number (1,2...) for each reference),
                    authors,
                    title,
                    journal,
                    vol,
                    pages,
                    month,
                    year,
                    url (if available),
                Make sure to include all. Go through the text and all the references made and validate that all references are included in the list. Information that you cannot find should be returned as blank in the corresponding field.
                Respond in JSON format without any Markdown or code block formatting in english""",
                }
        ]
    

    def create_pico_summary_agency(self):
            log.info('Creating pico message')
            return [ {
                "role": "user",
                "content": 
                    """"From the files in the vector store, please extract the following TLV assessments.
                    picos_agency: For each pico (combination of assessed Population_cohort, Intervention, Control/Comparator, Outcome), include:
                            pico_nr (the pico number this corresponds to),
                            indication (Name of indication),
                            population:
                                description (population description),
                                pediatric (yes/no. If the population cohort covers pediatric patients),
                                adolescent (yes/no),
                                adult (yes/no),
                                elderly (yes/no) 
                                gender (M/F/both),
                                ethnic_considerations (if relevant),
                                genetic_factors (inherited and somatic mutations),
                                family_history (yes/no),
                                sub_diagnosis (name of sub-diagnosis if relevant),
                                disease_stage,
                                biomarker_status,
                                co_morbidities,
                                previous_treatment,
                                response_to_treatment,
                                lifestyle_factors,
                                psychosocial_factors,
                                special_conditions,
                            severity (The associated severity (low/moderate/high/very high/varying/not assessed) of this indication. The severity assessment is usually in a sentence like 'TLV bedömer (?:att )?svårighetsgraden'),
                            comparator_agency (Name of the product that the TLV has compared the product against), 
                            comparator_modus_agency (treatment regime used in the comparison by TLV),
                            comparator_reason_agency (reason provided by TLV for using these comparators),
                            outcome_measure_agency (Outcome measure used by the TLV),  
                            Information that you cannot find should be returned as blank in the corresponding field.
                    Respond in JSON format without any Markdown or code block formatting in english""",
                    }
            ]

    def create_pico_summary_company(self):
        # treatment response: https://pmc.ncbi.nlm.nih.gov/articles/PMC7046919/#:~:text=The%20objective%20response%20to%20treatment,PD)%20(Table%201).
        log.info('Creating pico message company')
        return [ {
            "role": "user",
            "content": 
                """From the files in the vector store, please extract the following company provided information.
                picos_company: For each pico (combination of assessed Population_cohort, Intervention, Control/Comparator, Outcome), include:
                        pico_nr (a unique number (1,2...) for each pico),
                        indication (Name of indication),
                        population:
                            description (population description),
                            demographics:
                                pediatric (yes/no. If the population cohort covers pediatric patients),
                                adolescent (yes/no),
                                adult (yes/no),
                                elderly (yes/no) 
                                gender (M/F/both),
                            ethnic_considerations (if relevant),
                            genetic_factors (inherited and somatic mutations),
                            family_history (yes/no),
                            sub_diagnosis (name of sub-diagnosis if relevant),
                            disease_stage (disease progression stages 1-5 like so, stage 1 mild/early, stage 2 moderate/stable, stage 3 severe/progressive, stage 4 very severe/advanced, stage 5 terminal/end-stage),
                            biomarker_status,
                            co_morbidities,
                            previous_treatment,
                            response_to_treatment (complete response CR, partial response PR, stable disease SD and progressive disease PD),
                            lifestyle_factors,
                            psychosocial_factors,
                            special_conditions,

                        prevalance (How many have this condition according to the report),
                        incidence (How many get diagnosed with this indication every year according to the report),
                        
                        co_medication (name of other product that was used in combination with the evalauted product),
                        intervention (short description of the evaluated treatment),
                        comparator_company (Name of the product that the company has compared the product against), 
                        comparator_modus_company (treatment regime used in the comparison by the company),
                        comparator_reason_company (reason provided by the company for using these comparators),
                        outcome_measure_company (Outcome measure used by the company),  
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in JSON format without any Markdown or code block formatting in english""", #JSON format without any Markdown or code block formatting ///plain text
            }
        ]
    
    def create_analysis_summary_agency(self):
        log.info('Creating aznalysis message agency')
        return [ {
            "role": "user",
            "content": 
                """For each pico, please extract the following TLV assessments from the files in the vector store:
                analyses:
                        pico_nr,
                        QALY_gain_agency_lower (the lower number of gained quality-adjusted life years (QALY) as calculated by TLV, if applicable),
                            QALY_gain_agency_higher (the higher number of gained quality-adjusted life years (QALY) as calculated by TLV, if applicable),
                            QALY_total_cost_agency_lower (the lowest total cost for the gained quality-adjusted life years (QALY) as calculated by TLV, if applicable),
                            QALY_total_cost_agency_higher (the highest total cost for the gained quality-adjusted life years (QALY) as calculated by TLV, if applicable),
                            ICER_agency_lower (the lower cost of a quality-adjusted life year (QALY) as calculated by the TLV, if applicable.),
                            ICER_agency_higher (the higher cost of a quality-adjusted life year (QALY) as calculated by the TLV, if applicable),
                            costs_agency_product: 
                                drug_cost (Total drug costs for the product according to TLV),
                                other_costs (Other associated costs for the product according to TLV),
                                total_treatment_cost (Total cost for the treatment using the product according to TLV),
                                cost_type (cost per year, per month, per day or per treatment),
                            costs_agency_comparator: 
                                drug_cost (Total drug costs for the comparator according to TLV),
                                other_costs (Other associated costs for the comparator according to TLV),
                                total_treatment_cost (Total cost for the treatment using the comparator according to TLV,
                                cost_type (cost per year, per month, per day or per treatment),
                            efficacy_summary (A brief summary, no longer than three sentences, of TLV:s assessment of the product's efficacy for this indication),  
                            safety_summary (A brief summary, no longer than three sentences, of TLV:s assessment of the product's safety profile for this indication),  
                            decision_summary (A brief summary, no longer than three sentences, of TLV:s reasons for their decision for this indication),
                            uncertainty_assessment_clinical (how TLV assess the uncertainty of the clinical results presented by the company for this indication (low,/medium/high/ery high/not assessed)),
                            uncertainty_assessment_he (how TLV assess the uncertainty of the health economic results presented by the company for this indication (low/medium/high/very high/not assessed)),
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in JSON format without any Markdown or code block formatting in english""", #JSON format without any Markdown or code block formatting ///plain text
            }
        ]
    
    def create_analysis_summary_company(self):
        log.info('Creating analysis message company')
        return [ {
            "role": "user",
            "content": 
                """For each pico, please extract the following company provided information from the files in the vector store:
                analyses:
                        pico_nr,
                        QALY_gain_company (the number of gained quality-adjusted life years (QALY) as calculated by the company, if applicable),
                        QALY_total_cost_company (the total cost for the gained quality-adjusted life years (QALY) as calculated by the company, if applicable),
                        ICER_company (the cost of a quality-adjusted life year (QALY) as calculated by the company, if applicable),
                        comparison_method (direct/indirect, if the statistical comparison between the drug and the comparators was made using a direct or indirect method),
                        indirect_method (for example Bayesian network meta-analysis, if applicable),
                        costs_company_product: 
                            drug_cost (Total drug costs for the product according to the company),
                            other_costs (Other associated costs for the product according to the company),
                            total_treatment_cost (Total cost for the treatment using the product according to the company),
                            cost_type (cost per year, per month, per day or per treatment)
                        costs_company_comparator: 
                            drug_cost (Total drug costs for the comparator according to the company),
                            other_costs (Other associated costs for the comparator according to the company),
                            total_treatment_cost (Total cost for the treatment using the comparator according to the company),
                            cost_type (cost per year, per month, per day or per treatment)
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in JSON format without any Markdown or code block formatting in english""", #JSON format without any Markdown or code block formatting ///plain text
            }
        ]
    
    def create_research_summary(self):
        # for the population_cohorts you found.
        log.info('Creating research summary')
        return [ {
            "role": "user",
            "content": 
                """From the files in the vector store, please extract information about presented research. Look in the reference list (sv. Referenser) to find all research papers. For each identified pico, list all trials referenced:
                trials: 
                        pico_nr (The pico this trial relates to),
                        ref_nr (the number of the reference paper this trial was published in),
                        title (title of paper if applicable), 
                        summary (a brief summary in at most four sentences of the research),
                        number_of_patients (number of patients in the trial), 
                        number_of_controls (number of patients in control arm, if applicable),
                        indication (medical indication of the patients), 
                        duration (the duration of the trial),
                        phase (phase I, II, III or IV),
                        meta-analysis (yes/no),
                        randomized (yes/no),
                        controlled (yes/no),
                        type_of_control (placebo/no treatment/active treatment/dose comparison/historical control),
                        blinded (single, double, no),
                        design (equivalence/noninferiority/superiority),
                        objective (the intended purpose of the trial, efficacy/safety/both efficacy and safety),
                        primary_outcome_variable (sometimes referred to as effektmått),
                        reference (full reference to the paper),
                        url (an url address to the paper if possible),
                        outcome_values: For each trial arm include the results for all investigated outcome variables or key metrics. Focus on quantitative results:
                            trial_arm (which trial arm the results relate to),
                            value (value achieved for the primary outcome variable),
                            significance_level (compared to the control arm),
                            outcome_measure: 
                                name (name of the primary variable),
                                units,
                        safety (description of adverse events, side effects in all arms), 
                        Information that you cannot find should be returned as blank in the corresponding field.
                Respond in JSON format without any Markdown or code block formatting in english"""
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
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files.filename),
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
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files.filename),
            }
        ]
    
    def query_about_indications(self):
        log.info('What indications?')
        return [
            {
            "role": "user",
            "content": 
                """What medical indications and patient populations are evalauted in the files in the vector store? Please extract
                
                indications (list of medical indications the medicinal product is evaluated for and the associated severity (low, moderate, high, very high, varying, not assessed) assigned by TLV of each indication. 
                The indication information is usually found on the first page. 
                The severity assessment (low/moderate/high/very high/varying/not assessed) is usually found in a table on page 2 or 3 (sv. 'svårighetsgrad') in the file named '{}'),
                The prevalence (number of affected persons) is usually found in the same table',
                
                population_cohorts (list of population cohorts and their incidence and prevalance),
                
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files[0]['filename']),
            }
        ]

    
    def create_thread(self, message=''):
        log.info('Creating thread')
        self.thread = self.client.beta.threads.create(
            messages= self.message,
            tool_resources= {
                "file_search": {
                    "vector_store_ids": [self.vector_store.id]
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
                    if run.incomplete_details:
                        log.error('reason: ' + run.incomplete_details.reason)
                    raise(Exception('OpenAI run failed. Status: ' + run.status))
                else:
                    # we're done
                    break
            except Exception as e:
                attempt = attempt + 1
                time.sleep(attempt)
                log.error('OpenAI streaming failed for file {} with message {}'.format(self.file_path, additional_messages), exc_info=sys.exc_info())
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
                        run_id=runs.data[0].id,
                        timeout=120
                        )
                    wait = 0
                    while (run.status!='cancelled') & (run.status!='completed') & (run.status!='failed') & (run.status!='expired') & (wait<60):
                        time.sleep(wait)
                        wait = wait + 6
                        run = self.client.beta.threads.runs.retrieve(
                            thread_id=self.thread.id,
                            run_id=runs.data[0].id
                            )
                        log.info('waiting for run to cancel')
                    if (run.status!='cancelled') | (run.status!='completed') | (run.status!='failed') | (run.status!='expired'):
                        log.error('Unsuccesful in canceling failing parsing for file {} with message {}'.format(self.file_path, additional_messages)) #https://community.openai.com/t/assistant-api-cancelling-a-run-wait-until-expired-for-several-minutes/544100/4
                        # This is flimsy
                        attempt = MAX_NR_OF_RETRIES
                log.info('Trying again. Attempt: ' + str(attempt))

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
                    log.info('Total tokens used in run ' + str(k) + ': ' + str(r.usage.total_tokens))
                
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
                log.error('Failed to get results from OpenAI parsing for file ().'.format(self.file_path), exc_info=sys.exc_info())
                log.info('Trying again. Attempt: ' + str(attempt))
            
            finally:
                return res, annotated_citations
        