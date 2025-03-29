import os
import ntpath
import re
import openai
import EventHandler
import json
from logger_tt import getLogger
from pydantic_models_rev import HTA_Document, HTA_Document_Basis, Team, PICOs_comp, PICOs_ag, Analysis_List_Comp, Analysis_List_Ag, \
    Trials, References, HTA_Document_Extend, HTA_Document_NT_Extend, Panel2, MissingDataException, HTA_Summary, Indications_and_Population_Cohorts
import document_splitting as ds
from typing import Iterable
import time
import json
from openai import OpenAI
import instructor
import sys
import tempfile
from pypdf import PdfReader
import pandas as pd
from secret import secrets

class FoundDataException(Exception):
    pass

# create both assistants
# run with assistant_mini
# run validation
# share thread with assistant_expert
# run again with assistant_expert
# return 

DECISION_DICT = {'avslag-och-begransningar': 'rejected', 'avslag-och-uteslutningar': 'rejected', 'begransad': 'limited', 'generell': 'full', 'no decision': 'no decision'}


SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/combo/'

CHUNKING_STRATEGY_DECISION = {
    "type" : 'static',
    "static" : {
    "max_chunk_size_tokens":150,
    "chunk_overlap_tokens": 70
    }
}

CHUNKING_STRATEGY_PREAMBLE = {
    "type" : 'static',
    "static" : {
    "max_chunk_size_tokens":100,#4096, 
    "chunk_overlap_tokens": 40#1000
    }
}

CHUNKING_STRATEGY_CHAPTERS = {
    "type" : 'static',
    "static" : {
    "max_chunk_size_tokens":150,
    "chunk_overlap_tokens": 70
    }
}

client2 = OpenAI(
    api_key=secrets.open_ai_key,
    max_retries=4, 
    timeout=40.0)

client_instructor = instructor.from_openai(OpenAI(
    api_key=secrets.open_ai_key, 
    max_retries=4, 
    timeout=60.0))

log = getLogger(__name__)

MAX_NR_OF_RETRIES = 3

MAX_WAIT_TIME_STORAGE = 10

# for each parsing a quality score is calculated
# If the value isn not acceptsble, a second run is undetaken using gemeni. if it ius still unacceble a fukll rerun is done sung the 
# o4 model. If still not ok, add to unparseble df together with scorecard
# each mode has a dict with the quality -observed columns together with acceptance critiera.
# The acceptance critiera may be: populated (bool), 
# why cant we simpkly set a condiotion on the columns using pydantic? - Becsause the valid entries depend on the type of document.

class Worker_dossier:

    def __init__(self, client, doc_type = 'dossier', model = "gpt-4o-mini", dh=None): # "gpt-4o-mini" 'gpt-4o'
        log.info("Creating worker for " + doc_type)
        self.doc_type = doc_type
        self.client = client_instructor # TODO OBS client2
        self.model = model
        self.assistant_expert = self.create_assistant(model='gpt-4o')
        self.assistant_mini = self.create_assistant(model="gpt-4o-mini")
        self.main_thread = None
        self.expert_thread = None
        self.vector_store = None
        self.message_files = []
        self.dossier_nr = ''
        self.dh = dh
        self.quality = dict({'dossier': '', 'parseable': False, 'hta_document': False, 'hta_document_extend': False, 
                             'team': False, 'panel2': False, 'indications_and_population_cohorts': False, 'hta_summary': False,
                             'pico': False, 'analysis': False, 'trial': False, 'references': False})

    def clean_up(self):
        if self.message_files:
            self.delete_files()
        if self.vector_store: 
            self.delete_vs()
        self.delete_assistant(self.assistant_expert)
        self.delete_assistant(self.assistant_mini)

    def delete_file(self, f):
        response = self.client.files.delete(f['id'])
        log.info('Deleted file ' + f['filename'] + ': ' + str(response))

    def delete_files(self):
        for f in self.message_files:
            response = self.client.files.delete(f['id'])
            log.info('Deleted file ' + f['filename'] + ': ' + str(response))

    def delete_vs(self):
        response = self.client.vector_stores.delete(vector_store_id=self.vector_store.id)
        log.info('Deleted vector store: ' + self.vector_store.name + ': ' + str(response))

    def delete_assistant(self, assistant):
        response = self.client.beta.assistants.delete(assistant.id)
        log.info(response)

    def get_ICD(self, res):
        indications = {'indications':[]}
        for ind in res['indications']:
            if type(ind['indication']) is dict:
                ind_name = ind['indication']['indication']
            else:
                ind_name = ind['indication']
            log.info('Get ind: ' + ind_name)
            icd10_code = self.get_response(self.get_ICD_code(ind_name))
            # truncate to only one decimal
            icd10_code = re.sub(r'(?<=)\.\d*','', icd10_code) # we only need the first part
            log.info('Got ICD: ' + icd10_code)
            indications['indications'].append({'indication':{
                'indication': ind_name,
                'icd10_code':icd10_code,
            },  'severity':ind['severity']})
        return indications
    
    @staticmethod
    def fix_numbers(raw):
        raw = re.sub(' million| miljon', '000000', raw, flags=re.I)
        raw = re.sub('thousand', '000', raw, flags=re.I)
        raw = re.sub('[\.\,]', '', raw)
        raw = re.sub(' ', '', raw) 
        if re.search('\d*', raw):
            raw = re.search('[\d]*', raw)[0]
        return raw
    
    @staticmethod
    def clean_json_string(data):
        # sometimes shit is added 
        if data[0:3]=='```':
            log.info('Fixing shit')
            data = re.sub('```|json', '',data)
        return data

    @staticmethod
    def json_parse(data):
        # Fixes faulty json strings and converts to json
        try:
            # sometimes shit is added 
            if data[0:3]=='```':
                log.info('Fixing shit')
                data = re.sub('```|json', '',data)
            log.info('Fixed:')
            log.info(data)
            json_data = json.loads(data)
        except Exception as e:
            log.error('JSON parsing failed', exc_info=sys.exc_info())
            json_data = None
        return json_data

    
    def validated_parsing(self, model, message, output_type='pydantic'):
        # Runs the parsing and validates using the model validation set. If failed, the parsing is remade using the expert assistant
        #----------------------------
        def val_check(instance, data_present, rotten_apple, depth=0):
            # For nested models, investigate the children
            # One missing value is sufficient to return a false result
            # Only checking children of depth 1
            if not instance:
                return 0, 'self', 0
            if depth>1:
                return data_present, rotten_apple, 0
            for var in instance.Meta.validation_set:
                if type(var) is list:
                    depth = depth + 1
                    for li in var:
                        data_present, rotten_apple, depth = val_check(li, data_present, rotten_apple, depth)            
                else:
                    data_present_before = data_present
                    data_present = data_present*bool(instance.dict()[var])
                    if data_present_before and not data_present:
                        log.info('Parsing validation failed for {}'.format(var))
                        rotten_apple = var
            return data_present, rotten_apple, depth
        #---------------------------
        no_data = True
        attempts = 0
        assistant = self.assistant_mini
        thread = self.main_thread
        while no_data and (attempts<MAX_NR_OF_RETRIES):
            attempts = attempts + 1
            log.info('Validated parsing, attempt ' + str(attempts))
            self.run_parsing(assistant=assistant, thread=thread, additional_messages=message)
            ass_results, annot = self.get_results(thread)
            res_json_pre = self.json_parse(ass_results)
            if res_json_pre: 
                # back to string
                res_str = json.dumps(res_json_pre)
                # transfer to pydantic
                res_pyd = self.convert_to_pydantic(res_str, model)
                if res_pyd:
                    res_pyd_json = res_pyd.model_dump()
                    log.info('Before val_check')
                    log.info(res_pyd_json)

                data_present, rotten_apple,_ = val_check(instance=res_pyd, data_present=True, rotten_apple=None)
                no_data = not data_present
                if no_data:
                    message = [{
                        "role": "user",
                        "content": "Redo. The last response seems to lack extraction of {}.".format(rotten_apple)
                    }]       
            else:
                no_data = True
            #if no_data:    Let's wait with that
            #    log.info('Parsing validation failed. Redo with expert assistant')
            #    assistant = self.assistant_expert
            #    self.create_expert_thread()
            #    thread = self.expert_thread

        if output_type=='pydantic':
            return res_pyd, annot, no_data
        else:
            return res_json_pre, annot, no_data

    def get_analysis(self, entity):
        # Get the analysis results for both the company and the agency
        # ------------------
        # def run_anal_parsing(model, message):
        #     # TODO Fix to validated parsing
        #     self.run_parsing(assistant = self.assistant_mini, thread=self.main_thread, additional_messages=message)    
        #     ass_results_an, annot = self.get_results()
        #     log.info('The analysis result')
        #     log.info(ass_results_an)
        #     res_analys = self.json_parse(ass_results_an)['analyses']
        #     return res_analys
        def run_anal_parsing(model, message):
            res_json,_, no_data = self.validated_parsing(model, message, output_type='json')   
            log.info('The analysis result')
            log.info(res_json)
            res_analys = res_json['analyses']
            return res_analys, no_data
        # ------------------
        if entity=='company':
            analysis_message = self.create_analysis_summary_company()
            icer_keys = ['ICER_company']
            qaly_keys = ['QALY_gain_company']
            cost_key = 'costs_company_comparator'
            model = Analysis_List_Comp
        elif entity=='agency':
            analysis_message = self.create_analysis_summary_agency()
            icer_keys = ['ICER_agency_lower', 'ICER_agency_higher']
            qaly_keys = ['QALY_gain_agency_lower', 'QALY_gain_agency_higher']
            cost_key = 'costs_agency_comparator'
            model = Analysis_List_Ag
        else:
            return 'Wrong entity'
            
        res_analys, no_data = run_anal_parsing(model, analysis_message) 
        #input("Press Enter to continue...")
        for k in range(len(res_analys)):
            # fix the numbers
            for ik in icer_keys:
                if type(res_analys[k][ik])==str: # may already be a number
                    res_analys[k][ik] = self.fix_numbers(res_analys[k][ik])
            # Confirm that data was returned
            if res_analys[k]['analysis_type']=='cost-effectiveness' and (not res_analys[k][icer_keys[0]] or not res_analys[k][qaly_keys[0]]):# sufficient to check one icer
                log.info('Missing ICER or QALY')
                self.quality['analysis'] = False
                #res_analys = run_anal_parsing(model, self.reask_about_qualy()) # this hasn't worked out. Returns empty or removes summaries
            else: # analysis seems fine!
                self.quality['analysis'] = True

            if res_analys[k]['analysis_type']=='cost-minimization' and not res_analys[k][cost_key]['total_treatment_cost']:
                log.info('Missing costs')
                self.quality['costs'] = False
                #res_analys = run_anal_parsing(model, self.reask_about_costs()) 
            else:
                self.quality['costs'] = True

            pico_list = [int(x['pico_nr'])-1 for x in res_analys] # need to start at 0
            res_analysis_comp = self.convert_to_pydantic(json.dumps(res_analys), model)
            res_analysis_comp_json = res_analysis_comp.model_dump()

        return res_analysis_comp_json, pico_list
    
    def parse_and_add(self, res, message, model, additional_instructions = None):
        # This is the main entry to parsing and updates the result res with the new data
        res_, annot, no_data = self.validated_parsing(model, message)

        if no_data:
            log.error('Parsing failed in parse_and_add for ' + model.__name__)
            return None
        
        # back to json
        res_json = res_.model_dump()
        log.info(res_json)

        if res:
            res.update(res_json) 
        else:
            res = res_json
        
        log.info('After adding')
        log.info(res)
        
        self.quality[model.__name__.lower()] = True
        
        return res
    
    def initial_parsing(self):
        # sometimes it fails. I think is is due to not accessing the file even though it is said to be ready. Let's try a wait and give it a go again
        title = 'not found'
        res, annot, no_data = self.validated_parsing(HTA_Document, message=self.create_original_message_2())
        if no_data:
            return None
        res_json = res.model_dump()
        # Get the icd and convert to proper indications form
        indications = self.get_ICD(res_json)
        res_json.update(indications)
        # to string
        #res_json_pre = json.dumps(res_json_pre)
        #res = self.convert_to_pydantic(res_json_pre, HTA_Document)        
        #res_json = res.model_dump()
        log.info('After initial parsing and pydantic transfer')
        log.info(res_json)
        title = res.title
        self.dossier_nr = res.diarie_nr

        self.quality['dossier'] = self.dossier_nr
        #nr_of_picos = len(res['population_cohorts'])
        if title=='not found':
            return None
        else:
            self.quality['hta_document'] = True
        if self.dh:
            if self.dh.get_hta_with_diarie_nr_and_document_type(self.dossier_nr, self.doc_type):
                log.info('Found in DB after initial parsing')
                raise FoundDataException()
        
        return res_json
    

    def parse_decision_file(self, file_paths):
        file_nr = 0
        #while not res and (file_nr<(len(file_paths)-1)):
        #self.dossier_title = ntpath.basename(file_paths[file_nr]).split('.')[0]
        # -----------------------------------------------
        # The decision file
        # -----------------------------------------------
        # Initialization
        # Add vector store and file
        self.add_decision_file(file_paths[file_nr])
        self.create_auto_vector_store(self.dossier_title)
        self.main_thread = self.create_thread()
        # Do the basic parsing
        attempt = 0
        #while ((attempt<MAX_NR_OF_RETRIES) and not res):
        #    attempt = attempt + 1
        #    log.info('Parsing attempt nr ' + str(attempt))
        res = self.initial_parsing()
        if not res:
            # remove file
            #self.delete_file(self.message_files[0])
            #self.message_files = []
            # remove vs
            #self.delete_vs()
            log.error('Unable to parse file ' + self.dossier_title)
            log.info('Trying with the next')
            file_nr = file_nr + 1

            return None, None  

        if len(res['staff'])<20: 
            log.info('Missing staff. Having a second go')
            res_staff, annot, no_team_data = self.validated_parsing(Team, message=self.find_decision_staff_message())
            if not no_team_data:
                self.quality['team'] = True
            #self.run_parsing(additional_messages=self.find_staff_message())
            #self.run_parsing(additional_messages=self.find_decision_staff_message())
            
            #ass_results_staff, annot = self.get_results()
            #res_staff = self.convert_to_pydantic(ass_results_staff, Team)
            res_staff_json = res_staff.model_dump()
            log.info(res_staff_json)
            res.update(res_staff_json)

        return res, res_staff_json
    
    def get_file_paths(self, file_dir):
        file_paths = []
        for root, dirs, files in os.walk(file_dir, topdown=False):
            k=0
            for file in files[::-1]:
                k = k+1
                log.info('Getting file '+ str(k))
                file_paths.append(os.path.join(file_dir, file))
        file_paths.sort() # to likely get the decision file first
        return file_paths

    def parse_file_2(self, file_dir, url, decision):
        decision = DECISION_DICT[decision]
        # find decsion doc among list. Compare diarienr. Add odd diarie to own loop


        # combined parsing of both decision and (if available) basis document
        # first the decsion document is parsed to get the base data
        # then the basis document is added in parts (Preample and then chapter by chapter)
        # The base data is augmented by the parsing of the preamble
        # Then the meat of the infomration (picos, analysis, costs, trials, references) are parsed from the total context
        res = None 
        try:
            file_paths = self.get_file_paths(file_dir)
            self.dossier_title = ntpath.basename(file_paths[0]).split('.')[0]
            if self.doc_type=='dossier':
                log.info('Parsing dossier ' + self.dossier_title)
                res, res_staff_json = self.parse_decision_file(file_paths)
                self.basis_document_position = 1
            else: # nt-basis
                log.info('Parsing nt-basis ' + self.dossier_title)
                reader = PdfReader(file_paths[0])
                if reader.metadata.title:
                    self.dossier_title = reader.metadata.title
                self.basis_document_position = 0
            # ------------------------------------------------
            # Add (and chop up) the basis document
            # ------------------------------------------------
            has_basis = self.add_basis_files(file_paths, self.basis_document_position)
            if has_basis:
                # Parse the basics and augment the existing info
                if not self.vector_store: # this is for nt basis
                    self.create_auto_vector_store(self.dossier_title, auto=False)
                    self.main_thread = self.create_thread()
                # Add preamble
                self.add_files_to_vs([self.message_files[self.basis_document_position]], CHUNKING_STRATEGY_PREAMBLE)

                if self.doc_type=='dossier':
                    res = self.parse_and_add(res, self.create_basis_message(), HTA_Document_Extend)
                else: 
                    res = self.parse_and_add(res, self.create_basis_nt_message(), HTA_Document_NT_Extend)
                log.info('After adding extend')
                log.info(res)
                
                # compare diarie
                if self.doc_type=='dossier':
                    if not res['diarie_nr']==self.dossier_nr:
                        log.info('Basis document has a different diarie number: ' + str(res['diarie_nr']) + 'compared to basis' + self.dossier_nr)
                        #raise Exception('Differing diarie nr') # this is not necessarily wrong! Old TLV documents sometimes have a different diarie number for the basis document. Unclear why
                
                log.info('Get basis staff')
                res = self.parse_and_add(res, self.find_staff_message(), Panel2)
                # add the decsion tean
                if self.doc_type=='dossier':
                    res['staff'] = res['staff'] + res_staff_json['staff']
                log.info('After adding full team')
                log.info(res)
            else:
                log.info('Missing basis document for ' + self.dossier_title)

            log.info('Get the summaries')
            res = self.parse_and_add(res, self.create_summary_message(), HTA_Summary)
            #self.run_parsing(additional_messages=self.create_summary_message())    
            #ass_results_sum, annot = self.get_results()
            #res_sum = self.convert_to_pydantic(ass_results_sum, HTA_Summary)
            #res_sum = self.run_and_parse(self.create_summary_message())
            #res_sum_json = res_sum.model_dump()
            #res_sum_json = {k: res_sum_json[k] for k in ('analysis', 'efficacy_summary', 'safety_summary','decision_summary',
            #                            'uncertainty_assessment_clinical', 'uncertainty_assessment_he',
            #                            'limitations', 'requested_complement', 'requested_information',
            #                            'requested_complement_submitted', 'previously_licensed_medicine')} # otherwise the extra empty variables are overwrting the good stuff from initial parsing
            #res.update(res_sum_json) 
            log.info('After adding summaries')
            log.info(res)

            log.info('Getting indications')
            # TODO Enforce pydantic also on indications
            res = self.parse_and_add(res, self.query_about_indications(), Indications_and_Population_Cohorts)
            #res_ind = self.run_and_parse(self.query_about_indications())

            #if len(res_ind['indications'])==0:
            #    log.info('Parser missed indications and populations. redo')
            #    res_ind = self.run_and_parse(message=self.query_about_indications(), additional_instructions='You missed extracting indications and population cohorts. Please have another go.')
        
            # get icds
            indications = self.get_ICD(res)
            res.update(indications)
            log.info('After adding inds')
            log.info(res)
            # -------------------------------------
            # Get the rest...
            # -------------------------------------
            if len(self.message_files)>1: # TODO Finer chunking for results chapter
                self.add_files_to_vs(self.message_files[2:], CHUNKING_STRATEGY_CHAPTERS)
            else:
                log.info('Lacking chapters for file {}'.format(self.dossier_title))

            res = self.get_refs(res)

            # PICOs
            log.info('Get the picos')
            res_pico_comp,_, no_comp_pico = self.validated_parsing(PICOs_comp, message=self.create_pico_summary_company())
            #self.run_parsing(additional_messages=self.create_pico_summary_company())    
            #ass_results_pico, annot = self.get_results()
            #res_pico_comp = self.convert_to_pydantic(ass_results_pico, PICOs_comp)
            res_pico_comp_json = res_pico_comp.model_dump()
            log.info(res_pico_comp_json)

            res_pico_agency,_, no_agency_pico  = self.validated_parsing(PICOs_ag, message=self.create_pico_summary_agency())
            #self.run_parsing(additional_messages=self.create_pico_summary_agency())    
            #ass_results_pico2, annot = self.get_results()
            #res_pico_agency = self.convert_to_pydantic(ass_results_pico2, PICOs_ag)
            res_pico_agency_json = res_pico_agency.model_dump()

            # Compare lengths
            if len(res_pico_comp_json['picos'])<len(res_pico_agency_json['picos']):
                log.info('Missing company picos. Reasking')
                res_pico_comp,_, no_comp_pico = self.validated_parsing(PICOs_comp, message=self.assure_picos_collected('company'))
                #self.run_parsing(additional_messages=self.assure_picos_collected('company'))    
                #ass_results_pico, annot = self.get_results()
                #res_pico_comp = self.convert_to_pydantic(ass_results_pico, PICOs_comp)
                res_pico_comp_json = res_pico_comp.model_dump()
                log.info(res_pico_comp_json)
            elif len(res_pico_comp_json['picos'])>len(res_pico_agency_json['picos']):
                log.info('Missing agency picos. Redo')
                res_pico_agency,_, no_agency_pico = self.validated_parsing(PICOs_ag, message=self.assure_picos_collected('agency'))
                #self.run_parsing(additional_messages=self.assure_picos_collected('agency'))    
                #ass_results_pico2, annot = self.get_results()
                #res_pico_agency = self.convert_to_pydantic(ass_results_pico2, PICOs_ag)
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
            res['picos'] = []
            k=0
            for entry_comp, entry_ag in zip(res_pico_comp_json_reduced['picos'], res_pico_agency_json_reduced['picos']):
                log.info('pico comp to add: ')
                log.info(entry_comp)
                log.info('pico ag to add: ')
                log.info(entry_ag)
                res['picos'].append({**entry_comp,**entry_ag})
                res['picos'][k]['analysis'] = [] # Initiate, populate later
                k=k+1

            # Assign icd codes for each pico
            list_of_indications = [ind['indication'] for ind in indications['indications']]
            for k in range(0,len(res['picos'])):
                ind_name = res['picos'][k]['indication']
                if ind_name in list_of_indications:
                    icd = indications['indications'][list_of_indications.index(ind_name)]['icd10_code']
                else:
                    icd = self.get_response(self.get_ICD_code(ind_name))
                    # truncate to only one decimal
                    icd = re.sub(r'(?<=)\.\d*','', icd)
                res['picos'][k]['indication'] = {}
                log.info('PICO icd code: ' + icd)
                res['picos'][k]['indication']['indication'] = ind_name
                res['picos'][k]['indication']['icd10_code'] = icd

            log.info('After adding picos')
            log.info(res)
            self.quality['pico'] = (not no_comp_pico) & (not no_agency_pico)

            log.info('Get the analysis')
            res_analysis_comp_json, pico_list  = self.get_analysis('company')
            res_analysis_agency_json, _ = self.get_analysis('agency')
            """ self.run_parsing(additional_messages=self.create_analysis_summary_company())    
            ass_results_an, annot = self.get_results()
            log.info('The analysis result')
            log.info(ass_results_an)

            res_analys = self.json_parse(ass_results_an)['analyses']
            
            for k in range(len(res_analys)):
                # fix the numbers
                res_analys[k]['ICER_company'] = self.fix_numbers(res_analys[k]['ICER_company'])
                # Confirm that data was returned
                if res_analys[k]['analysis']=='cost-effectiveness' and not res_analys[k]['ICER_company']:
                    log.info('Missing ICER')
                    self.run_parsing(additional_messages=self.reask_about_qualy()) 
                    ass_results_an, annot = self.get_results()
                    log.info('The revised analysis result')
                    log.info(ass_results_an)
                elif res_analys[k]['analysis']=='cost-minimization' and not res_analys[k]['costs_company_comparator']['total_treatment_cost']:
                    log.info('Missing costs')
                    self.run_parsing(additional_messages=self.reask_about_costs()) 
                    ass_results_an, annot = self.get_results()
                    log.info('The revised analysis result')
                    log.info(ass_results_an)


            pico_list = [int(x['pico_nr'])-1 for x in res_analys] # need to start at 0
            res_analysis_comp = self.convert_to_pydantic(json.dumps(res_analys), Analysis_List_Comp)
            res_analysis_comp_json = res_analysis_comp.model_dump()

            self.run_parsing(additional_messages=self.create_analysis_summary_agency())    
            ass_results_an2, annot = self.get_results()
            res_analysis_agency = self.convert_to_pydantic(ass_results_an2, Analysis_List_Ag)
            res_analysis_agency_json = res_analysis_agency.model_dump() """
            #res_analysis_comp = self.run_and_parse(self.create_analysis_summary_company())
            #res_analysis_agency = self.run_and_parse(self.create_analysis_summary_agency())

            # to avoid overwriting when merging the comp and agency analyses
            res_analysis_comp_json_reduced = {'analyses':[]}
            costs = {'costs':[]}
            k=0
            for row in res_analysis_comp_json['analyses']:
                res_analysis_comp_json_reduced['analyses'].append({k: v for k, v in row.items() if k in ['analysis_type','QALY_gain_company', 'QALY_total_cost_company', 'ICER_company', 
                                                                                                        'comparison_method','indirect_method', 'co_medication', 'intervention',
                                                                                                        'comparator_company','comparator_modus_company', 'comparator_reason_company', 
                                                                                                        'outcome_measure_company']})
                res_analysis_comp_json_reduced['analyses'][k]['costs'] = row['costs'] # returns a list of costs with two entries one for the product and one for the comparator 
                k=k+1                

            res_analysis_agency_json_reduced = {'analyses':[]}
            k=0
            for row in res_analysis_agency_json['analyses']:
                res_analysis_agency_json_reduced['analyses'].append({k: v for k, v in row.items() if k in 
                                                                     ['QALY_gain_agency_lower', 'QALY_gain_agency_higher', 
                                                                    'QALY_total_cost_agency_lower', ' QALY_total_cost_agency_higher',
                                                                    'ICER_agency_lower', 'ICER_agency_higher',
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
                costs = entry_ag['costs'] + entry_comp['costs']
                res['picos'][pico_list[k]]['analysis'] = dict({**entry_comp,**entry_ag})
                res['picos'][pico_list[k]]['analysis']['costs'] = costs
                k=k+1

            log.info('Get the trials')
            res_trials_json,_, no_trial_data  = self.validated_parsing(Trials, message=self.create_research_summary(), output_type='json')
            # Initialize
            for p in res['picos']:
                p['analysis']['trials'] = []

            if not no_trial_data:    
                #self.run_parsing(assistant=self.assistant_mini, thread=self.main_thread ,additional_messages=self.create_research_summary())    
                #ass_results_trials, annot = self.get_results(self.main_thread)
                pico_list = [int(x['pico_nr'])-1 for x in res_trials_json['trials']]
                res_trials = self.convert_to_pydantic(json.dumps(res_trials_json), Trials) # this is to ensure we convert all yes/no to booleans
                res_trials_json = res_trials.model_dump()
                log.info('Trial')
                log.info(res_trials_json)
                
                k=0
                for t in res_trials_json['trials']:
                    res['picos'][pico_list[k]]['analysis']['trials'].append(t)
                    k=k+1
            else:
                self.quality['trials']=True

            # sometimes some refs are missed in the first run
            res = self.get_refs(res)

            log.info('Update with correct title {} and url {}'.format(self.dossier_title, url))
            # Update with correct title
            res['title'] =  self.dossier_title
            # Add url
            res['url'] = url
            #
            res['parsing_model'] = self.model
            # Fix dates
            if self.doc_type!='dossier':
                res['latest_decision_date'] = None
                reader = PdfReader(file_paths[0])
                if reader.metadata.creation_date:
                    res['date'] = reader.metadata.creation_date.strftime('%Y%m%d')
                else:
                    res['date'] = None

            if res['decision']!= decision:
                log.error('Parsing wonky. Extracted faulty decision')
            
            res['decision']= decision
            

            self.quality['parseable'] = self.quality['hta_document'] & self.quality['team'] & self.quality['indications_and_population_cohorts'] \
                                        & self.quality['hta_summary'] & self.quality['pico']# & self.quality['analysis'] 

            log.info('Before pydantic extraction')
            log.info(json.dumps(res))

            with open(SAVE_PATH + "before_pydant-"+re.sub('\/','-',self.dossier_nr)+".json", "w") as outfile:
                outfile.write(json.dumps(res))
            #return ass_results, ass_results_pico_comp, ass_results_pico_agency, ass_results_trial_comp
            # log.info('Extracting document')
            # # TODO: Do we need to? All the parts have already been validated? cant we send it as a json?
            # results_doc = self.convert_to_pydantic(res, HTA_Document_Basis)

            # with open(SAVE_PATH + "after_pydant.json", "w") as outfile:
            #     outfile.write(json.dumps(results_doc.model_dump_json()))
        except FoundDataException as f:
            log.info('Found')
            res = 'found'

        except Exception as e:
            log.error('Full parsing failed', exc_info=sys.exc_info())
        
        finally:
            if self.quality['parseable']:
                self.clean_up()
            return res, self.quality, self.vector_store

   
    def get_refs(self, res):
        log.info('Get the references')
        res_refs,_, no_references = self.validated_parsing(References, message=self.get_references())
        if not no_references:
            self.quality['references'] = True
        #self.run_parsing(additional_messages=self.get_references())
        #ass_results_refs, annot = self.get_results() 
        #res_refs = self.convert_to_pydantic(ass_results_refs, References)
        res_refs_json = res_refs.model_dump()

        res.update(res_refs_json) 
        log.info('After adding references')
        log.info(res)
        return res

    def create_assistant(self, model="gpt-4o-mini"): #"gpt-4o-mini"
        log.info("Creating assistant")
        # OBS client2
        return client_instructor.beta.assistants.create(
            instructions="""You are a professional health economist. 
                You will be presented a document in Swedish describing 
                the reasons for a reimbursement decision for a medicinal product. 
                You should use your tools to extract info. Be meticulous and 
                make sure you get all the requested information. 
                It is more important to get the details right than to deliver a result fast.""",
            model=model,
            #reasoning_effort= 'high', # default is medium high not supported for mini
            temperature=0.2,
            tools= [
                {"type": "file_search"}
            ]
        )

    def convert_to_pydantic(self, input, format):
        # converts json string input to a pydanti object using openai 
        completion = None
        log.info('Running JSON parsing')
        attempt = 1
        while attempt<MAX_NR_OF_RETRIES:
            try:
                completion=client_instructor.chat.completions.create(
                model="gpt-4o-mini", # "gpt-4o-mini"   "gpt-4o-2024-08-06"
                response_model=format,
                messages=[
                    {"role": "system", "content": "You are a useful assistant. Extract information according to the speficied response model. Make sure to include the nested objects."},#Convert the user input into JSON. Make sure to include the nested objects, such as Product and HTA_Agency.
                    {"role": "user", "content": f"Extract:\n{input}"}
                ]
                )
                return completion#.choices[0].message
            except Exception as e:
                attempt = attempt + 1
                time.sleep(attempt)
                log.error('Pydantic parsing failed for file {}'.format(self.dossier_title), exc_info=sys.exc_info())
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
    

    def create_auto_vector_store(self, name, auto=True):
        log.info("Auto vector store creation named {} initiated".format(name))
        try:
            if auto:
                self.vector_store = self.client.vector_stores.create(
                    name=name,
                    chunking_strategy= CHUNKING_STRATEGY_DECISION,
                    file_ids=[self.message_files[0]['id']], # The decision file 
                    )
            else:
                self.vector_store = self.client.vector_stores.create(
                    name=name)
            start_time = time.time()
            current_time = time.time()
            response = self.client.vector_stores.retrieve(vector_store_id=self.vector_store.id)
            while ((not auto)|response.file_counts.completed<1) and (current_time-start_time)<MAX_WAIT_TIME_STORAGE:
                time.sleep(0.5)
                response = self.client.vector_stores.retrieve(vector_store_id=self.vector_store.id)
                current_time = time.time()
                log.info('Waiting for storage')
            if (current_time-start_time)>MAX_WAIT_TIME_STORAGE:
                log.info('Store status: ' + response.status)
                raise Exception('Unable to create vector store in reasonable time')
            log.info('Store status: ' + response.status)
            log.info('Number of files in store: ' + str((response.file_counts.completed)))
        except Exception as e:
            log.error('could not create auto vector store', exc_info=sys.exc_info())

    # this seems to be unnnecessary since it is already attached to the thread    
    def attach_vector_store_to_assistant(self, assistant):
        log.info('Attaching vector store to assistant')
        return self.client.beta.assistants.update(
                assistant_id=assistant.id,
                tool_resources={"file_search": {"vector_store_ids": [self.vector_store.id]}})

    def add_decision_file(self, file_path):
        log.info('Adding decision file ' + file_path)
        stored_file = json.loads(self.client.files.create(
                    file=open(file_path, "rb"), purpose="assistants").to_json())
        self.message_files.append(stored_file)
        log.info('Succesfully added ' + file_path)

    def add_basis_files(self, file_paths, basis_doc_pos=1):
        log.info('Adding basis files')
        basis_file_found = False
        if (len(file_paths)<2) and self.doc_type=='dossier': # only valid for dossiers!!!!
            return False
        for fp in file_paths[basis_doc_pos:]: # Skipping the first as that is the decision file
            # start by splitting the file 
            with tempfile.TemporaryDirectory() as tmpdirname:
                generated_files = ds.split_preamble_and_chapters_safe(fp, tmpdirname)
                # add each chapter file
                if generated_files:
                    for f in generated_files:
                        with open(f, "rb") as file:
                            log.info('Adding file ' + f)
                            stored_file = json.loads(self.client.files.create(file=file, purpose="assistants").to_json())
                            
                            self.message_files.append(stored_file)
                            log.info('Succesfully added ' + f)
                    basis_file_found = True
        
        return basis_file_found

    def add_files_to_vs(self, files, chunking_strategy):
        nr_of_files = len(files)
        batch_add = self.client.vector_stores.file_batches.create_and_poll(
                vector_store_id=self.vector_store.id,
                chunking_strategy= chunking_strategy,
                file_ids=[f['id'] for f in files]
                )
        nr_completed = 0
        wait = 0
        while (nr_completed<nr_of_files) and (wait<10):
            wait = wait + 1
            time.sleep(1)
            vector_store_file_batch = self.client.vector_stores.file_batches.retrieve(
                vector_store_id=self.vector_store.id,
                batch_id=batch_add.id
                )
            nr_completed = vector_store_file_batch.file_counts.completed
            log.info('Nr of completed files: ' + str(nr_completed))

   #---------------------------------------
   # Messages
   # --------------------------------------

    def create_original_message_2(self):

        log.info('Creating original message')

        #title = 'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Kaftrio 27 januari 2022 0_avslag-och-uteslutningar.pdf' #'Odomzo 25 mars 2022 0_avslag-och-uteslutningar.pdf'
        return [
            {
            "role": "user",
            "content": 
                """Please extract 
                title (Title of the document. See text in section 'SAKEN' on the front page), 
                company (applicant. Only company name), 
                product name, 
                diarie nr (sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning'),
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
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files[0]['filename']),
            }
        ]                  
        
    def create_basis_message(self):
        log.info('Creating basis message')
        return [
            {
            "role": "user",
            "content": 
                """Please extract: 
                diarie_nr (diarienummer or sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning'),
                application_type (Usually found in a table on page 2 or 3),
                latest_decision_date (sv. 'Sista beslutsdag'. Usually found in a table on page 2 or 3),
                indications (list of:
                  indication (medical indications the medicinal product is evaluated for. The indication information is usually found in a table on page 2 or 3),
                  severity (the associated severity (low/moderate/high/very high/varying/not assessed) of this indication. The severity assessment is usually found in a table on page 2 or 3 (sv. 'svårighetsgrad')),
                  prevalence (number of affected persons. Usually found in a table on page 2 or 3)
                ), 
                comparators (list of product to compare against, comparators, sv. 'Relevant jämförelsealternativ'. Usually found in a table on page 2 or 3. Also look in the section 'TLV:s bedömning och sammanfattning'. If the company did not specify a comparator, say so.),
                annual_turnover (estimated annual turnover. Usually found in a table on page 2 or 3),
                threee_part_deal (yes/no. Whether a three-part negotiation (sv. 'treparts' or 'sidoöverenskommelse') took place),
                from the file named '{}'. If you cannot find the file, return title as 'not found' and the rest of the requested output as blanks. 
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files[self.basis_document_position]['filename']),
            }
        ]

    def create_basis_nt_message(self):
        log.info('Creating basis nt message')
        return [
            {
            "role": "user",
            "content": 
                """Please extract: 
                diarie_nr (diarienummer or sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning'),
                date (Format YYYY-MM-DD, usually on the front page),
                company (applicant. Only company name), 
                product name, 
                agency (name of the agency evaluating the product),
                application_type (Usually found in a table on page 2 or 3),
                latest_decision_date (sv. 'Sista beslutsdag'. Usually found in a table on page 2 or 3),
                indications (list of:
                  indication (medical indications the medicinal product is evaluated for. The indication information is usually found in a table on page 2 or 3),
                  severity (the associated severity (low/moderate/high/very high/varying/not assessed) of this indication. The severity assessment is usually found in a table on page 2 or 3 (sv. 'svårighetsgrad')),
                  prevalence (number of affected persons. Usually found in a table on page 2 or 3)
                ), 
                comparators (list of product to compare against, comparators, sv. 'Relevant jämförelsealternativ'. Usually found in a table on page 2 or 3. Also look in the section 'TLV:s bedömning och sammanfattning'. If the company did not specify a comparator, say so.),
                annual_turnover (estimated annual turnover. Usually found in a table on page 2 or 3),
                threee_part_deal (yes/no. Whether a three-part negotiation (sv. 'treparts' or 'sidoöverenskommelse') took place),
                from the file named '{}'. If you cannot find the file, return title as 'not found' and the rest of the requested output as blanks. 
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files[self.basis_document_position]['filename']),
            }
        ]

    def find_decision_staff_message(self):

        log.info('Creating decision staff message')
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
                Respond in a plain JSON format without any Markdown or code block formatting in english""".format(self.message_files[self.basis_document_position]['filename']),
            }
        ]

    def create_summary_message(self):
        log.info('Creating summary message')
        return [
            {
            "role": "user",
            "content": 
                """From the files in the vector store, please extract:
                analysis (cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys) or a combination), 
                efficacy_summary (A brief summary no longer than three sentences of TLV:s assessment of the product's efficacy),  
                safety_summary (A brief summary no longer than three sentences of TLV:s assessment of the product's safety profile),  
                decision_summary (A brief summary no longer than three sentences of TLV:s reasons for their decision or health economic assessment),  
                uncertainty_assessment_clinical (how TLV assess the uncertainty of the clinical results presented by the company (low,/medium/high/ery high/not assessed)),
                uncertainty_assessment_he (how TLV assess the uncertainty of the health economic results presented by the company (low/medium/high/very high/not assessed)),
                limitations (list of limitations that applies to the reimbursement. May be none. ),
                requested_complement (yes/no. If TLV requested the company to complement with additional information),
                requested_information (what type of information TLV requested the company to complement the application with if applicable),
                requested_complement_submitted (yes/no/NA. If applicable whether the company submitted the requested complementary info),
                previously_licensed_medicine (yes/no. Whether the active ingredient or the drug previously was a licensed medicine. 
                Information that you cannot find should be returned as blank in the corresponding field.
                Respond in a plain JSON format without any Markdown or code block formatting in english.""",
            }
        ]
    
    # TODO: Add 'backup' messages to be added during a second parsing attempt. Should include alternative formulation of the data request


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
                    """"From the files in the vector store, please extract the following TLV assessments. It may be helpful to look at the text chunks where the references have been cited in the documents.
                    picos_agency: For each pico (combination of assessed Population_cohort, Intervention, Control/Comparator, Outcome), include:
                            pico_nr (the pico number this corresponds to),
                            indication (Name of indication),
                            population:
                                description (population description),
                                pediatric (yes/no. If the population cohort covers pediatric patients),
                                adolescent (yes/no),
                                adult (yes/no),
                                elderly (yes/no) 
                                gender (Male/Female/All. If missing assign to All),
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
                """From the files in the vector store, please extract the following company provided information. It may be helpful to look at the text chunks where the references have been cited in the documents.
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
                                gender (Male/Female/All),
                            ethnic_considerations (if relevant),
                            genetic_factors (inherited and somatic mutations),
                            family_history (yes/no),
                            sub_diagnosis (name of sub-diagnosis if relevant),
                            disease_stage (disease progression stage, mild/early: stage 1, moderate/stable: stage 2, severe/progressive: stage 3, very severe/advanced: stage 4, terminal/end-stage: stage 5),
                            biomarker_status,
                            co_morbidities,
                            previous_treatment,
                            response_to_treatment (complete response: CR, partial response: PR, stable disease: SD or progressive disease: PD),
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
    
    def assure_picos_collected(self, entity):
        log.info('Dig into picos')
        return [ {
            "role": "user",
            "content": 
                """Are you absolutely sure you got all picos identified by the {}? Seems you found different number of picos for the company and the agency. Respond using the same format as you did for the previous query.""".format(entity), #JSON format without any Markdown or code block formatting ///plain text
            }
        ]

    def create_analysis_summary_agency(self):
        # QALY_total_cost_agency_lower (the lowest total cost for the gained quality-adjusted life years (QALY) as calculated by TLV, if applicable),
        # QALY_total_cost_agency_higher (the highest total cost for the gained quality-adjusted life years (QALY) as calculated by TLV, if applicable),
        log.info('Creating aznalysis message agency')
        return [ {
            "role": "user",
            "content": 
                """For each pico, please extract the following TLV assessments from the files in the vector store:
                analyses:
                        pico_nr,
                        analysis_type (cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys)), 
                        QALY_gain_agency_lower (the lower number of gained quality-adjusted life years (QALY) as calculated by TLV, if applicable),
                        QALY_gain_agency_higher (the higher number of gained quality-adjusted life years (QALY) as calculated by TLV, if applicable),
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
    
    def create_CE_analysis_summary_agency(self):
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
    
    def create_analysis_summary_company(self):# QALY_total_cost_company (the total cost for the gained quality-adjusted life years (QALY) as calculated by the company, if applicable),
        log.info('Creating analysis message company')
        return [ {
            "role": "user",
            "content": 
                """For each pico, please extract the following company provided information from the files in the vector store:
                analyses:
                        pico_nr,
                        analysis_type (cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys)),
                        QALY_gain_company (the number of gained quality-adjusted life years (QALY) as calculated by the company, if applicable),
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
    
    def create_CE_analysis_summary_company(self): 
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
                population_cohorts (list of population cohorts and their incidence and prevalance),
                
                Respond in a plain JSON format without any Markdown or code block formatting in english""",
            }
        ]
    
    def reask_about_qualy(self):
        log.info('QUALY was missing. Try again')
        return [
            {
            "role": "user",
            "content": 
                "The analysis was conducted using cost-effectiveness criteria. Still, some QALY gains and/or ICER values could be found in the last response. Please redo.",
            }
        ]

    def reask_about_costs(self):
        log.info('Costs were missing. Try again')
        return [
            {
            "role": "user",
            "content": 
                "The analysis was conducted using cost-minimization. Still, no total treatment cost values could be found in the last response. Please redo.",
            }
        ]
    


    def create_thread(self, message=[]):
        log.info('Creating thread')
        return self.client.beta.threads.create(
            messages= message,
            tool_resources= {
                "file_search": {
                    "vector_store_ids": [self.vector_store.id]
                }
            }
        )

    def create_expert_thread(self):
        # A thread cannot be updated with messages. Instead we create a new instance with previous main thread's messages
        thread_messages = self.client.beta.threads.messages.list(self.main_thread.id)
        log.info('Nr of messages in org thread: ' + str(len(thread_messages.data)))
        # Just feeding the messages to the thread constructor does not work. i get: openai.BadRequestError: Error code: 400 - {'error': {'message': "Unknown parameter: 'messages[0].id'.", 'type': 'invalid_request_error', 'param': 'messages[0].id', 'code': 'unknown_parameter'}}
        # Instead, let's chunk it up to a simpler form in alignment with the example in the api doc
        thread_history = [{'role': m.role, 
                               'content': m.content[0].text.value} for m in thread_messages.data].reverse() # For some reason the messages are created in reverse order in the thread
        self.expert_thread = self.create_thread(thread_history)
        new_thread_messages = self.client.beta.threads.messages.list(self.expert_thread.id)
        log.info('Nr of messages in new expert thread: ' + str(len(new_thread_messages.data)))
        # set flag variable to in  
        input("Press Enter to continue...")

    def run_parsing(self, assistant, thread, additional_messages=None, additional_instructions=None):
        log.info('Running parsing')
        attempt = 1
        while attempt<MAX_NR_OF_RETRIES:
            try:
                with self.client.beta.threads.runs.stream(
                    #response_format=model,
                    thread_id=thread.id,
                    assistant_id=assistant.id,
                    additional_messages=additional_messages,
                    additional_instructions= additional_instructions,
                    event_handler=EventHandler.EventHandler()
                ) as stream:
                    stream.until_done()
                # Let's make sure it finished correctly
                runs = self.client.beta.threads.runs.list(thread.id)
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
                log.error('OpenAI streaming failed for file {} with message {}'.format(self.dossier_title, additional_messages), exc_info=sys.exc_info())
                 # Let's kill that run
                runs = self.client.beta.threads.runs.list(thread.id)
                # Seems the most recent one is nr 0
                run = runs.data[0]
                q=0
                for r in runs:
                    q=q+1
                    log.info('Run ' +str(q) + ' status is ' + r.status)
                if (run.status!='cancelled') & (run.status!='completed') & (run.status!='failed') & (run.status!='expired'):
                    run = self.client.beta.threads.runs.cancel(
                        thread_id=thread.id,
                        run_id=runs.data[0].id,
                        timeout=120
                        )
                    wait = 0
                    while (run.status!='cancelled') & (run.status!='completed') & (run.status!='failed') & (run.status!='expired') & (wait<60):
                        time.sleep(wait)
                        wait = wait + 6
                        run = self.client.beta.threads.runs.retrieve(
                            thread_id=thread.id,
                            run_id=runs.data[0].id
                            )
                        # TODO: See chunks, see https://community.openai.com/t/inspecting-file-search-chunks/996790/2 
                        log.info('waiting for run to cancel')
                    if (run.status!='cancelled') | (run.status!='completed') | (run.status!='failed') | (run.status!='expired'):
                        log.error('Unsuccesful in canceling failing parsing for file {} with message {}'.format(self.dossier_title, additional_messages)) #https://community.openai.com/t/assistant-api-cancelling-a-run-wait-until-expired-for-several-minutes/544100/4
                        # This is flimsy
                        attempt = MAX_NR_OF_RETRIES
                log.info('Trying again. Attempt: ' + str(attempt))

    def get_results(self, thread):
        res = None
        annotated_citations = None
        log.info('Retrieving the result')
        attempt = 1
        while attempt<MAX_NR_OF_RETRIES:
            try:
                # Get the last message from the thread
                message = self.client.beta.threads.messages.retrieve(
                    thread_id=thread.id,
                    message_id=self.client.beta.threads.messages.list(thread_id=thread.id, order="desc").data[0].id
                )
                message_text_object = message.content[0]
                message_text_content = message_text_object.text.value  

                runs = self.client.beta.threads.runs.list(thread.id)
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

                res = self.clean_json_string(res)

                log.info('\nSources')
                for annotation in annotated_citations:
                    log.info(f"Annotation {annotation['number']}:")
                    log.info(f"  File Name: {annotation['file_name']}")
                    log.info(f"  Character Positions: {annotation['start_index']} - {annotation['end_index']}")
                    log.info("")  # Add a blank line for readability            
            
            except Exception as e:
                attempt = attempt + 1
                time.sleep(attempt)
                log.error('Failed to get results from OpenAI parsing for file ().'.format(self.dossier_title), exc_info=sys.exc_info())
                log.info('Trying again. Attempt: ' + str(attempt))
            
            finally:
                return res, annotated_citations
        