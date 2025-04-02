import os
import ntpath
import re
from sandbox.EventHandler import EventHandler
import json
from openai import OpenAI
import openai
from logger_tt import getLogger
from sandbox.pydantic_models_2 import HTA_Document, HTA_Document_Preamble, Basic_PICOs, \
PICOs, Trials, References, HTA_Document_NT
import sandbox.document_splitting as ds
import time
import json
import sys
import tempfile
from pypdf import PdfReader
from google import genai
from google.genai import types
from google.genai import errors as GemeniError
import instructor
from tenacity import retry_if_exception_type
from google.api_core import retry
import secret.secrets as secrets
log = getLogger(__name__)

# import decsion file
# parse to 

DECISION_DICT = {'avslag-och-begransningar': 'rejected', 'avslag-och-uteslutningar': 'rejected','begransad': 'limited', 'generell': 'full', 'no decision': 'no decision'}

SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/gemini/'


CLIENT = genai.Client(api_key=secrets.GEMENI_API_KEY)
CLIENT_INSTRUCTOR  = instructor.from_genai(CLIENT, mode=instructor.Mode.GENAI_TOOLS)

client2 = OpenAI(
    api_key=secrets.open_ai_key,
    max_retries=4, 
    timeout=40.0)

#client = OpenAI(
#    api_key="GEMINI_API_KEY",
#    base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
#)

# from https://github.com/google-gemini/cookbook/issues/469
# Catch transient Gemini errors.
def is_retryable(e) -> bool:
    if retry.if_transient_error(e):
        # Good practice, but probably won't fire with the google-genai SDK
        log.error('Caught a TransientError', exc_info=1)
        return True
    elif (isinstance(e, genai.errors.ClientError)):# and e.code == 429):
        # Catch 429 quota exceeded errors
        log.error('Caught a ClientError', exc_info=1)
        return True
    elif (isinstance(e, genai.errors.ServerError)):#and e.code == 503
        # Catch 500, includsing 503 model overloaded errors
        log.error('Caught a ServerError', exc_info=1)
        return True
    else:
        return False

MAX_NR_OF_RETRIES = 3

MAX_WAIT_TIME_STORAGE = 10

#def quality_assurance():
    # Performs a number of checks to validate that the data has been extracted in good order
    # pydantic validate model?
    # or go through models using the validated parsing code but this time looking in full depth?

class Worker_dossier:

    def __init__(self, client=CLIENT, doc_type = 'dossier', gem_model = "gemini-2.0-flash", alternate = False, dh=None):
        log.info("Creating worker for dossier")
        self.doc_type = doc_type
        self.client = CLIENT_INSTRUCTOR # ignore arg
        self.model = gem_model
        self.alternate = alternate
        self.vector_store = None
        self.message_files = []
        self.dossier_nr = ''
        self.dh = dh
        self.quality = dict({'dossier': '', 'parseable': False, 'hta_document': False, 'hta_document_preamble': False, 
                             'basic_picos': False, 'picos': False, 'trials': False, 'references': False, 'hta_document_nt': False})
        
    def clean_up(self):
        if self.message_files:
            self.delete_files()

    def delete_file(self, f):
        response = self.client.files.delete(name=f.name)
        log.info('Deleted file ' + f.name + ': ' + str(response))

    def delete_files(self):
        for f in self.message_files:
            response = self.client.files.delete(name=f.name)
            log.info('Deleted file ' + f.name + ': ' + str(response))


    def start_chat(self, gemini_model = "gemini-2.0-flash",  history = None):
        system_instructions = """You are a professional health economist. 
            You will be presented a document in Swedish describing 
            the reasons for a reimbursement decision for a medicinal product. 
            You should use your tools to extract info. Be meticulous and 
            make sure you get all the requested information. 
            It is more important to get the details right than to deliver a result fast. 
            Sometimes the information needs to be summarized in order to fit the structured output. 
            Make sure to respect the maximum string lengths (max_length), inclusing white spaces, of the columns found in the Field declarations.
            Information that you cannot find should be returned as blank ('') in the corresponding column."""
        self.chat = self.client.chats.create(
            model=gemini_model, # gemini-2.0-pro-exp-02-05 gemini-2.0-flash
            #request_options={'retry':retry.Retry()},
            config=types.GenerateContentConfig(system_instruction=system_instructions)
        )
        

    def upload_file(self, file_path):
        new_file = self.client.files.upload(file=open(file_path, "rb"), 
                                            config = {
                                                'mime_type': 'application/pdf'
                                            })
        # Make sure it was succesfully uploaded
        log.info('File upload status: ' + new_file.state)
        if new_file.state=='ACTIVE':
            self.message_files.append(new_file)
            log.info('File added. Nr of message files: ' + str(len(self.message_files)))
        else:
            log.info('Not added. File upload status: ' + new_file.state)

    @retry.Retry(predicate=is_retryable, initial=10, timeout=130)
    def get_gemini_response(self, content, model):
       # attempt = 1
       # while attempt<MAX_NR_OF_RETRIES:
       #     log.info('Sending request ' + str(attempt) + ' to Gemini')
       #     try:
       log.info('Sending request ' + 'to Gemini')
       response = self.chat.send_message(
           message=content,
           config=types.GenerateContentConfig(
               response_mime_type = 'application/json',
               response_schema = model)
        )
       return response
       #     except (GemeniError.ServerError, GemeniError.APIError) as e:
       #         attempt = attempt + 1
       #         log.error('Gemini API Error. Backing off and retrying.', exc_info=sys.exc_info())
       #         time.sleep(2^attempt)  
       # return None  

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

    
    def validated_parsing(self, model, message):
        no_data = True
        attempts = 0
        while no_data and (attempts<MAX_NR_OF_RETRIES):
            attempts = attempts + 1
            log.info('Validated parsing, attempt ' + str(attempts))
            self.run_parsing(additional_messages=message)
            ass_results, annot = self.get_results()
            res_json_pre = self.json_parse(ass_results)
            if res_json_pre:
                for var in model.Meta.validation_set:
                    no_data = not bool(res_json_pre[var])
                    message = [{
                        "role": "user",
                        "content": "Redo. You seem to have missed to extract {}.".format(var)
                    }]
            else:
                no_data = True
        return ass_results, annot, no_data
    
   
    def parse2(self, res, message, model, additional_instructions = None):
        
        ass_results, annot, no_data = self.validated_parsing(model, message)

        if no_data:
            log.error('Parsing failed in parse2 for ' + model.__name__)
            return None
        
        res_json_pre = self.json_parse(ass_results)
        res_json_pre = json.dumps(res_json_pre)
        res_ = self.convert_to_pydantic(res_json_pre, model)
        
        res_json = res_.model_dump()
        log.info(res_json)
        if res:
            res.update(res_json) 
        else:
            res = res_json
        log.info('After adding')
        log.info(res)
        return res
    

    def parse_with_gemini(self, files, message, model):

        res = None
        
        content = [*files, message]
        res_ok = False
        retries = 0
        while (not res_ok) and (retries<MAX_NR_OF_RETRIES):
            res = self.get_gemini_response(
                content=content, 
                model=model)
            if res:
                res_ok = bool(res.text)
            retries = retries+1

        if not res_ok:
            log.error('Unable to parse message: ' + message)
            return None 
        
        self.quality[model.__name__.lower()] = res_ok
        return res

    def parse_decision_file(self, file_paths, decision):
        res = None
        file_nr = 0
        #while not res and (file_nr<(len(file_paths)-1)):
        #self.dossier_title = ntpath.basename(file_paths[file_nr]).split('.')[0]
        # -----------------------------------------------
        # The decision file
        # -----------------------------------------------
        # Add vector store and file
        self.upload_file(file_paths[file_nr])
        # Do the basic parsing
        res = None
        
        message = "Extract information according to the pydantic model you have been provided. The reimbursement decision was {}.".format(decision)
        res = self.parse_with_gemini(files=[self.message_files[0]], message=message, model=HTA_Document)
        
        log.info('Decision doc')
        log.info(res.text)
        # Get the team
        if not res:
            # remove file
            #self.delete_file(self.message_files[0])
            #self.message_files = []
            # remove vs
            #self.delete_vs()
            log.error('Unable to parse file ' + self.dossier_title)
            log.info('Trying with the next')
            file_nr = file_nr + 1
            
        if not res:
            return None 

        return res
    
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
        
        self.start_chat(gemini_model=self.model)
        try:
            file_paths = self.get_file_paths(file_dir)
            self.dossier_title = ntpath.basename(file_paths[0]).split('.')[0]
            if self.doc_type=='dossier':
                log.info('Parsing dossier ' + self.dossier_title)
                res = self.parse_decision_file(file_paths, decision)
                self.basis_document_position = 1
                HTA_decision = res.parsed
                if HTA_decision:
                    HTA_dump = HTA_decision.model_dump()
                else:
                    HTA_dump = json.loads(res.text)
                self.dossier_nr = HTA_dump['diarie_nr']
                self.quality['dossier'] = self.dossier_nr
                # Check if already in DB
                if self.dh.get_hta_with_diarie_nr(HTA_dump['diarie_nr'], self.doc_type):
                    log.info('Found in DB after initial parsing')
                    res = 'found'
                    return # exits to finally
                # assign icd code
                k=0
                icd_dict = {}
                for ind in HTA_dump['indications']:
                    ind_name = ind['indication']['indication']
                    log.info('Get ind: ' + ind_name)
                    icd10_code = self.get_response(self.get_ICD_code(ind_name))
                    # truncate to only one decimal
                    icd10_code = re.sub(r'(?<=)\.\d*','', icd10_code) # we only need the first part
                    log.info('Got ICD: ' + icd10_code)
                    HTA_dump['indications'][k]['indication']['icd10_code'] = icd10_code
                    k=k+1
                    icd_dict[ind_name] = icd10_code
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
                if self.doc_type=='dossier':
                    res_preamble = self.parse_with_gemini(
                        files = [self.message_files[self.basis_document_position]], 
                        message = "You have been presented the first pages, including a summary, of a HTA report. Extract the information according to the pydantic model.", 
                        model = HTA_Document_Preamble)
                    log.info('Preamble:')
                    log.info(res_preamble.text)
                    preamble_json = json.loads(res_preamble.text)
                    preamble_json['staff'] = preamble_json['staff'] + HTA_dump['staff']
                    del HTA_dump['staff']
                    HTA_preamble = res_preamble.parsed
                else:
                    res_NT = self.parse_with_gemini(
                        files = self.message_files[self.basis_document_position:],
                        message = "You have been presented a HTA report. Extract the information according to the pydantic model.", 
                        model = HTA_Document_NT) 
                    log.info('NT doc')
                    log.info(res_NT.text)
                    HTA_NT = res_NT.parsed
                    if HTA_NT:
                        HTA_dump = HTA_NT.model_dump()
                    else:
                        HTA_dump = json.loads(res_NT.text)
                    self.dossier_nr = HTA_dump['diarie_nr']
                    self.quality['dossier'] = self.dossier_nr
                
                # compare diarie
                #if self.doc_type=='dossier':
                #    if not res['diarie_nr']==self.dossier_nr:
                #        log.error('Basis document has a different diarie number')
                #        raise Exception('Differing diarie nr')
                
            else:
                log.info('Missing basis document for ' + self.dossier_title)

            # Get the rest...
            # -------------------------------------
            #if len(self.message_files)>1: # TODO Finer chunking for results chapter
            #    for f in file_paths[2:]:
            #        self.upload_file(f)
            #else:
            #    log.info('Lacking chapters for file {}'.format(self.dossier_title))

            references = self.parse_with_gemini(
                        files = self.message_files[self.basis_document_position:], 
                        message = "Extract the references from the files (if there are any). Specifically, look in the reference list (sv. Referenser).", 
                        model = References)
            log.info('References:')
            log.info(references.text)
            references_json = json.loads(references.text)
            
            # Let's extract the history of the chat so far
            self.curated_chat_history = self.chat._curated_history
            # For the picos and analysis, we will use the more elobarate, but rate-limited model
            if self.alternate:
                self.start_chat(gemini_model="gemini-2.0-pro-exp-02-05", history=self.curated_chat_history)
            # PICOs
            log.info('Get the picos')
            picos = self.parse_with_gemini(
                        files = self.message_files[self.basis_document_position:], 
                        message = "Extract the PICO:s that the company and the agency have considered. Make sure to include all and write only one PICO per row in the table. Each combination of Population (P), Intervention (I), Comparator (C) and Outcome (O)", 
                        model = Basic_PICOs)
            log.info('PICO:s')
            log.info(picos.text)

            log.info('Get the analyses')
            picos_analyses = self.parse_with_gemini(
                        files = [], 
                        message = "For each PICO that you identified, extract the associated analysis results and put into the augmented pydantic model.", 
                        model = PICOs) #PICOs_Basic_Analysis)
            log.info('PICO:s w. analysis')
            log.info(picos_analyses.text)
            pico_json = json.loads(picos_analyses.text)
            
            # already taken care of!
            # log.info('Get the trials')
            # trials = self.parse_with_gemini(
            #             files = [], 
            #             message = "Find all trials that are referenced in the documents and extract information according to the pydantic model.", 
            #             model = Trials) #PICOs_Basic_Analysis)
            # log.info('Trials')
            # log.info(trials.text)

            # log.info('Add trials to analysis')
            # trials = self.parse_with_gemini(
            #             files = [], 
            #             message = "Update the attached pydantic model with the trialsa you found.", 
            #             model = PICOs) #PICOs_Basic_Analysis)

            k=0
            for p in pico_json['picos']:
                ind_name = str(p['indication']['indication'])
                log.info('Get ind: ' + ind_name)
                icd10_code = self.get_response(self.get_ICD_code(ind_name))
                # truncate to only one decimal
                icd10_code = re.sub(r'(?<=)\.\d*','', icd10_code) # we only need the first part
                log.info('Got ICD: ' + icd10_code)
                pico_json['picos'][k]['indication']['icd10_code'] = icd10_code
                k=k+1

            # Put it all together
            log.info('Put it together')
            if self.doc_type=='dossier': 
                if has_basis:
                    preamble_json = {col: preamble_json[col] for col in list(preamble_json.keys())  if not re.match('diarie_nr|picos', col)}
                    res = dict(**HTA_dump,
                                        **preamble_json,
                                        **references_json,
                                        **pico_json)
                else:
                    res = dict(**HTA_dump,
                                        **references_json,
                                        **pico_json)
            else:
                res = dict(**HTA_dump,
                                    **references_json,
                                    **pico_json)
            #res_final = Dossier(**HTA_decision.model_dump(),
            #    **{
            #        'application_type': res_preamble.parsed.application_type,
            #        'latest_decision_date': res_preamble.parsed.latest_decision_date,
            #        'annual_turnover': res_preamble.parsed.annual_turnover,
            #        'three_part_deal': res_preamble.parsed.three_part_deal,
            #        'comparators': res_preamble.parsed.comparators,
            #        'experts': res_preamble.parsed.experts,
            #        'uncertainty_assessment_clinical': res_preamble.parsed.uncertainty_assessment_clinical,
            #        'uncertainty_assessment_he': res_preamble.parsed.uncertainty_assessment_he
            #        }
            #)
            #res_final.picos = {**picos.parsed.model_dump()}
            #res_final.references = references
            #res = self.parse_with_gemini(
            #            files = self.message_files[self.basis_document_position:], 
            #            message = "Now let's put all the information you extracted together in the attached pydantic model representing the dossier covering all documents. Specificallyu, make sure to map the trials correctly to each analysis", 
            #            model = Dossier) #PICOs_Basic_Analysis)

            # Update with correct title
            res['title'] = self.dossier_title
            # Add url
            res['url'] = url

            if self.doc_type=='dossier':
                if res['decision']!= decision:
                    log.error('Parsing wonky. Extracted faulty decision')
            
                res['decision']= decision

            if self.alternate:
                res['parsing_model'] =  self.model + "+gemini-2.0-pro-exp-02-05"
            else: 
                res['parsing_model'] =  self.model
            # Fix dates
            #if self.doc_type!='dossier':
            #    res_final.latest_decision_date = None
            #    reader = PdfReader(file_paths[0])
            #    if reader.metadata.creation_date:
            #        res['date'] = reader.metadata.creation_date.strftime('%Y%m%d')
            #    else:
            #        res['date'] = None

            if self.doc_type=='dossier':
                self.quality['parseable'] = self.quality['hta_document'] & self.quality['picos']
            else:
                self.quality['parseable'] = self.quality['hta_document_nt'] & self.quality['picos']
            
            res_str = json.dumps(res)
            log.info('Before pydantic extraction')
            log.info(res_str)
            #log.info(res_final.model_dump_json(indent=4))
            with open(SAVE_PATH + "before_pydant_" + re.sub('\/','-',self.dossier_nr) + ".json", "w") as outfile:
                outfile.write(res_str)
            
            #return ass_results, ass_results_pico_comp, ass_results_pico_agency, ass_results_trial_comp
            # log.info('Extracting document')
            # # TODO: Do we need to? All the parts have already been validated? cant we send it as a json?
            # results_doc = self.convert_to_pydantic(res, HTA_Document_Basis)

            # with open(SAVE_PATH + "after_pydant.json", "w") as outfile:
            #     outfile.write(json.dumps(results_doc.model_dump_json()))
        
        except Exception as e:
            log.error('Parsing failed', exc_info=sys.exc_info())
            res = None
        
        finally:
            self.clean_up()
            return res, self.quality, None # none since there is no vector store 
   
    def get_refs(self, res):
        log.info('Get the references')
        self.run_parsing(additional_messages=self.get_references())
        ass_results_refs, annot = self.get_results() 
        res_refs = self.convert_to_pydantic(ass_results_refs, References)
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

    def convert_to_pydantic(self, input, format):
        # converts json string input to a pydanti object using openai 
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
                self.vector_store = self.client.beta.vector_stores.create(
                    name=name,
                    chunking_strategy= CHUNKING_STRATEGY_DECISION,
                    file_ids=[self.message_files[0]['id']], # The decision file 
                    )
            else:
                self.vector_store = self.client.beta.vector_stores.create(
                    name=name)
            start_time = time.time()
            current_time = time.time()
            response = self.client.beta.vector_stores.retrieve(vector_store_id=self.vector_store.id)
            while ((not auto)|response.file_counts.completed<1) and (current_time-start_time)<MAX_WAIT_TIME_STORAGE:
                time.sleep(0.5)
                response = self.client.beta.vector_stores.retrieve(vector_store_id=self.vector_store.id)
                current_time = time.time()
                log.info('Waiting for storage')
            if (current_time-start_time)>MAX_WAIT_TIME_STORAGE:
                log.info('Store status: ' + response.status)
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
                        log.info('Adding file ' + f)
                        self.upload_file(f)
                        log.info('Succesfully added ' + f)
                    basis_file_found = True
        
        return basis_file_found

    def add_files_to_vs(self, files, chunking_strategy):
        nr_of_files = len(files)
        batch_add = self.client.beta.vector_stores.file_batches.create_and_poll(
                vector_store_id=self.vector_store.id,
                chunking_strategy= chunking_strategy,
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
                analysis (cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys)), 
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
                                gender (M/F/All. If missing assign to All),
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
                                gender (M/F/All),
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
                        analysis (cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys)), 
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
                        analysis (cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys)),
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
                "The analysis was conducted using cost-effectiveness criteria. Still, some QALY gains and/or ICER values could be found in your response. Please redo.",
            }
        ]

    def reask_about_costs(self):
        log.info('Costs were missing. Try again')
        return [
            {
            "role": "user",
            "content": 
                "The analysis was conducted using cost-minimization. Still, no total treatment cost values could be found in your response. Please redo.",
            }
        ]
    
    def create_thread(self, message=[]):
        log.info('Creating thread')
        self.thread = self.client.beta.threads.create(
            messages= message,
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
                    event_handler=EventHandler()
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
                log.error('OpenAI streaming failed for file {} with message {}'.format(self.dossier_title, additional_messages), exc_info=sys.exc_info())
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
                        # TODO: See chunks, see https://community.openai.com/t/inspecting-file-search-chunks/996790/2 
                        log.info('waiting for run to cancel')
                    if (run.status!='cancelled') | (run.status!='completed') | (run.status!='failed') | (run.status!='expired'):
                        log.error('Unsuccesful in canceling failing parsing for file {} with message {}'.format(self.dossier_title, additional_messages)) #https://community.openai.com/t/assistant-api-cancelling-a-run-wait-until-expired-for-several-minutes/544100/4
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
        