import os
import ntpath
import re
import openai
from sandbox.EventHandler import EventHandler
import json
from logger_tt import getLogger
from sandbox.pydantic_models_2 import HTA_Document
import sandbox.document_splitting as ds
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

SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/merger/'


client_instructor = instructor.from_openai(OpenAI(
    api_key=secrets.open_ai_key, 
    max_retries=4, 
    timeout=60.0))

log = getLogger(__name__)
# taske in both objects
# and the quality scores
# Go through each score

MAX_NR_OF_RETRIES = 3

MAX_WAIT_TIME_STORAGE = 10

class Worker_dossier:

    def __init__(self, client=client_instructor, doc_type = 'dossier', model = 'gpt-4o', dh=None):
        log.info("Creating merger")
        self.doc_type = doc_type
        self.client = client 
        self.model = model
        self.vector_store = None
        self.message_files = []
        self.dh = dh
        self.quality = dict({'dossier': '', 'parseable': False, 'hta_document': False, 'hta_document_preamble': False, 
                             'basic_picos': False, 'picos': False, 'trial': False, 'references': False})
        
    def create_thread(self, messages=[]):
        log.info('Creating thread')
        return self.client.beta.threads.create(
            messages= messages
        )
    

    def create_assistant(self, vs, model="gpt-4o"): #"gpt-4o-mini"
            log.info("Creating assistant")
            # OBS client2
            return self.client.beta.assistants.create(
                instructions="""You are a professional health economist. You will be presented two JSON files each containing extracted information, 
                using two different approaches, from the same dossier of documents related to a health technology assessment of a medicinal product. 
                The documents in this dossier have been added to your knowledge base at creation in a vector store. Refer to the provided files for knowledge. 
                Your job is to produce an extract from all these sources according 
                to the pydantic model, which has been specified as the expected structured output.""",
                model=model,
                response_model=HTA_Document,
                #reasoning_effort= 'high', # default is medium high not supported for mini
                temperature=0.2,
                tools= [
                    {"type": "file_search"}
                ],
                tool_resources={
                    "file_search": {
                    "vector_store_ids": [vs.id]
                    }
                }
                # Add output model
            )
    
    def merge(self, JSON_combo, JSON_gemini, vs):
        self.assistant = self.create_assistant(vs, self.model)
        # create messages
        messages=[
            {
            "role": "user",
            "content": "Wait until you have received both JSON files before presenting a result. The first JSON file: {}".format(JSON_combo),
            },
            {
            "role": "user",
            "content": "The second JSON file: {}".format(JSON_gemini)
            },
            {
            "role": "user",
            "content": "That's it. Please provide the response now."
            }
        ]
        thread = self.create_thread(messages)
        self.run_parsing(self.assistant, thread)
        res_final, annot = self.get_results()

        res = json.dumps(res_final)
        log.info('Before pydantic extraction')
        log.info(res)
        #log.info(res_final.model_dump_json(indent=4))
        with open(SAVE_PATH + "before_pydant_" + re.sub('\/','-', res['diarie_nr']) + ".json", "w") as outfile:
            outfile.write(res)

        return res, annot


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
                    event_handler=EventHandler()
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



