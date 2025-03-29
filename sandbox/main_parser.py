import time
from sqlalchemy import inspect
import worker
import worker_basis
import worker_dossier
import os
import sys
import json
import re
import pandas as pd
import fitz
from openai import OpenAI
from logger_tt import getLogger, setup_logging
from sqlalchemy_models360 import HTA_Document, HTA_Document_Basis, PICO, Analysis, Population, Outcome_Measure
from parse_decision_file import parse_decision_file
from data_cleaner import clean_up
import concurrent.futures
import queue
from threading import Thread, Event
from multiprocessing.pool import ThreadPool
import csv
from data_handler_new import DataHandlerProduction 

from secret import secrets

client = OpenAI(
    api_key=secrets.open_ai_key, 
    max_retries=4, 
    timeout=60.0)

setup_logging(use_multiprocessing=True, config_path='C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/log_config.json') # os.getcwd() + '/log_config.json')

log = getLogger(__name__)

dummy = False

unparseable = []

MAX_NR_OF_RETRIES = 5

DOCUMENT_TYPE = 'dossier' #'decision' # basis ntbasis
SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'

# test files:
# multiple picos: Kaftrio 27 januari 2022 0_avslag-och-uteslutningar
# complex comparator - best avaialble care, even though a comparative produict is mentoined: Cuprior 24 jan 2020 0_generell
# limitations: Dicloabak 04 sep 2019 0_begransad
# multipla picos inkl barn: Jylamvo 16 jun 2023 0_begransad

# Parse the decision documents
#file_dir_out = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/decisions2/'
file_dir = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/' + DOCUMENT_TYPE + '/'
#file_dir = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/decisions2/'
#file_dir = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/ntbasis_test/'
#file_dir = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/basis_test/'
SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'

df12 = pd.read_csv(SAVE_PATH + 'files_agg_new3.csv', sep=';') # since we cannot use fritz when threading
df12 = clean_up(df12)

def parser_manager(file_queue, data_queue, unproc_queue, pool_size = 20):
    log.info('Starting parser')
    # create thread pool
    with ThreadPool(pool_size) as pool:
        # use threads to generate items and put into the queue
        _ = [pool.apply_async(parser_work, args=(file_queue, data_queue, unproc_queue, )) for _ in range(pool_size)]
        # wait for all tasks to complete
        pool.close()
        pool.join()
    # put a signal to expect no further tasks
    data_queue.put(None)
    # report a message
    print('Parser manager done.')
 
def parser_work(q_files, q_data, unproc_queue):
    # run until there is no more work
    dh = DataHandlerProduction()
    while True:
        # retrieve one item from the queue
        log.info('Getting data')
        filename = q_files.get()
        # check for signal of no more work
        if not filename:
            log.info('No data to parse')
            # put back on the queue for other consumers
            q_data.put(None)
            # shutdown
            #parser_done.set()
            return
        
        try:
            log.info('Parsing: ' + filename)
            log.info('Files in queue: ' + str(q_files.qsize()))
            
            # Get diarie_nr, cannot use fritz when threading
            doc = fitz.open(filename) 
            diarie_nr_raw = re.findall(r'(\d*[\/]\d*)', doc.metadata['keywords'], flags=re.DOTALL)[0].strip() 
            diarie_nr = ''
            if diarie_nr_raw:
                diarie_nr = diarie_nr_raw[0].strip() 
            doc.close()
            #df_inform, reason = parse_decision_file(filename, None)
            #df12 = clean_up(df_inform)

            # Check if already in DB
            already_in_db = bool(dh.get_hta_with_diarie_nr_and_document_type(diarie_nr, DOCUMENT_TYPE))
                
            if not already_in_db:
                if DOCUMENT_TYPE=='decision':
                    parser = worker.Worker(client)
                else:
                    parser = worker_basis.Worker_basis(client)
                
                data = parser.parse_file_2(filename)
            
                if not data:
                    log.error('Unable to parse file: ' + filename, exc_info=sys.exc_info())
                    log.info('Adding file to unparseable queue')
                    unproc_queue.put(filename)
                else:
                    log.info("Extracted product: " + data.title)
            
                    data = clean_up_data(data, df12, filename) 
            
                    q_data.put(data)
                    #data_ready.set()
                    log.info('Data sent for insertion')
                    #time.sleep(10)
                    log.info('Successfully parsed file ' + filename)
            else:
                log.info('Data already in DB. Skipping file {}'.format(filename))
        except Exception as e:
            log.error('Unable to parse file: ' + filename, exc_info=sys.exc_info())
            log.info('Adding file to unparseable queue')
            unproc_queue.put(filename)
        finally:
            log.info('In finally')
            dh.close()
            q_files.task_done()
            log.info('Parsing task done issued')
 
# consumer manager
def insert_manager(queue_data, pool_size=20):
    log.info('Starting inserter')
    # create thread pool
    with ThreadPool(pool_size) as pool:
        # start consumer tasks
        _ = [pool.apply_async(insert_work, args=(queue_data,)) for _ in range(pool_size)]
        # wait for all tasks to complete
        pool.close()
        pool.join()
    print('Insert manager done.')
 
def insert_work(q_data):
    # run until there is no more work
    log.info('Entered inserter work')
    
    while True:
        # Wait for data to be ready
        #data_ready.wait()
        try: 
            log.info('Data is ready')
            # retrieve one item from the queue
            log.info('Data objects in queue:' + str(q_data.qsize()))
            data = q_data.get()
            insert_success = parse_and_insert(data)

        except Exception as e:
            log.error('Unable to insert data for file', exc_info=sys.exc_info())
        finally:
            q_data.task_done()

def create_file_queue():
    log.info('Creating file queue')
    q = queue.Queue()
    for root, dirs, files in os.walk(file_dir, topdown=False):
        for file in files[::-1]:
            q.put(file_dir + file)
    log.info('Added ' + str(q.qsize) + ' elements to the file queue')
    return q

def clean_up_data(data, df12, filename):
    d = data.dict()
    d2 = {k: d[k] for k in ('products', 'company', 'date')}
    df_data  = pd.DataFrame({'company':d2['company']['name'], 'drug_name': d2['products'][0]['name'], 'decision_date': d2['date'], 'annual_turnover':'','active substance':'', 'severity':''}, index=[0])
    # Clean up company and product
    df_data_clean = clean_up(df_data)
    data.company.name = df_data_clean['company'].iloc[0]
    data.products[0].name = df_data_clean['drug_name'].iloc[0]                         
    # compare and extract
    # diarie_nr, company, product
    if (not data.diarie_nr) or (data.diarie_nr[0:4]=='1234') or (data.diarie_nr[0:4]=='0000'):
        df_manual = df12[(df12['drug_name']==data.products[0].name) & (df12['document_type']=='decision')]  
        data.diarie_nr = df_manual['diarie_nr'].iloc[0]
    else:
        df_manual = df12[df12['diarie_nr']==data.diarie_nr]    
    if not data.company:
        data.company.name = df_manual['company'].iloc[0]
    if len(data.products)<1:
        data.products[0].name = df_manual['drug_name'].iloc[0] 
    # Decision 
    decision_sv = filename.split('/')[-1].split('_')[-1].split('.')[0]
    decision_dict = {'begransad': 'limited','generell': 'full', 'avslag-och-uteslutningar': 'rejected'}
    data.decision = decision_dict[decision_sv]
    
    return data

def parse_and_insert(data):
    data_dict = data#.model_dump()
    log.info("Extracted product: " + data['title'])
    #log.info("Extracted trials: " + str(len(data_dict['picos'][0]['analysis']['trials'])))
    #db_parent = HTA_Document(**data_dict,session=None)
    log.info('Received data:')
    log.info(data)#.model_dump())
    log.info('Trying to put into HTA_Document')
    
    if DOCUMENT_TYPE=='decision':
        db_parent = HTA_Document(**data_dict,session=None)
    else:
        db_parent = HTA_Document_Basis(**data_dict,session=None)
        db_parent.decision = 'no decision'
    
    if not db_parent:
        log.info('No data to insert')
        return False
        #if parser_done:
        #    return
        #data_ready.clear()
    else:
        db_parent.document_type = DOCUMENT_TYPE
        log.info('db_parent:')
        log.info(db_parent.__dict__)
    #try:
        log.info('Inserting ' + data['diarie_nr'])
        insert_data(db_parent)
    #except Exception as e:
    #    log.error('Unable to insert data for file ' + data.diarie_nr, exc_info=sys.exc_info())
    #finally:
    #    log.info('In finally for parse_and_insert')
        return True

def insert_data(db_parent):
    dh = DataHandlerProduction()
    dict_data = db_parent.__dict__
    # insert agency
    log.info('Insert agency')
    dh.insert_agency(db_parent.hta_agency.name)
    # insert company
    log.info('Insert company')
    insert_company(db_parent, dh)
    
    # insert hta_document
    idhta_doc = insert_hta_document(db_parent, dh=dh)
    insert_hta_indication(db_parent, dh, idhta_doc)
    # insert product
    log.info('Insert product')
    insert_hta_product(dict_data, dh, idhta_doc)
    
    # insert staff
    # Some odd old documents lack staff
    log.info('Nr of staff: ' + str(len(db_parent.staff)))
    log.info('Nr of experts: ' + str(len(db_parent.experts)))
    if len(db_parent.staff)>0: 
        log.info('Insert staff')
        insert_staff(dict_data, dh, idhta_doc)
    if len(db_parent.experts)>0: 
        log.info('Insert experts')
        insert_experts(dict_data, dh, idhta_doc)
    # insert references (if any)
    if DOCUMENT_TYPE!='decision':
        if len(db_parent.references)>0:
            insert_references(dict_data, dh, idhta_doc)
    # insert pico, analysis, trial, costs
    log.info('Insert picos')
    insert_picos_et_al(db_parent, dh, idhta_doc)
    log.info('Successfully inserted data from file ' + data.diarie_nr)
    dh.close()


def insert_company(db_parent, dh):
    log.info('Adding company: ' + db_parent.company.name)
    dh.insert_company(db_parent.company.name)

def insert_references(dict_data, dh, id_hta):
    log.info('Adding references')
    ref_list = []
    for ref in dict_data['references']:
        ref_list.append({'authors': ref.reference.authors, 'title': ref.reference.title, 'journal': ref.reference.journal, 
                         'vol': ref.reference.vol, 'pages': ref.reference.pages, 'month': ref.reference.month, 'year': ref.reference.year, 'url': ref.reference.url})
    df_ref = pd.DataFrame(ref_list)
    dh.insert_references(df_ref)
    df_ref['idhta_document'] = id_hta
    dh.insert_hta_references(df_ref)

def insert_hta_document(db_parent, dh=None):
    # TODO: Change if HTA_basis
    if DOCUMENT_TYPE=='decision':
        df_hta = pd.DataFrame({col: getattr(db_parent, col) for col in inspect(HTA_Document).columns.keys() if not re.match('id', col)}, index=['diarie_nr','date','document_type'])#, index=['diarie_nr','date','document_type']
    else:
        df_hta = pd.DataFrame({col: getattr(db_parent, col) for col in inspect(HTA_Document_Basis).columns.keys() if not re.match('id', col)}, index=['diarie_nr','date','document_type'])#, index=['diarie_nr','date','document_type']
    if not df_hta['date'].iloc[0]:
        log.info('Missing date')
        df_hta['date'] = '2000-01-01' # TODO: Fix this
    df_hta['company'] = db_parent.company.name
    log.info('Nr of rows 1: ' + str(len(df_hta.index)))
    df_hta = pd.concat([df_hta, df_hta], ignore_index=True)
    df_hta = df_hta.set_index(['diarie_nr','date','document_type'])  
    df_hta = df_hta[:-1]
    df_hta.index.names = ['diarie_nr','date','document_type']

    log.info('index names')
    log.info(df_hta.index.names)
    log.info('Nr of rows 2: ' + str(len(df_hta.index)))
    df_hta = df_hta.drop_duplicates()
    log.info('Nr of rows 3: ' + str(len(df_hta.index)))
    id = dh.insert_hta(df_hta, db_parent.hta_agency.name)
    return id

def insert_hta_indication(db_parent, dh, idhta):
    rows_list = []
    for ind in db_parent.indications:
        rows_list.append({'idhta_document': idhta, 'ICD': ind.indication.icd_10, 'severity': ind.severity}) 
    df_hta_indications = pd.DataFrame(rows_list) 
    dh.insert_hta_indication(df_hta_indications)

def insert_hta_product(dict_data: dict, dh, idhta):
    rows_list = []
    for p in dict_data['products']:
        dh.insert_one_product(p.name)
        rows_list.append({'drug_name': p.name, 'idhta_document': idhta}) 
    df_hta_prod = pd.DataFrame(rows_list) 
    dh.insert_hta_has_product(df_hta_prod)

def insert_staff(dict_data, dh, idhta):
    rows_list = []
    rows_list_2 = []
    for s in dict_data['staff']:
        s.staff.name
        rows_list.append({'name': s.staff.name, 'title': s.staff.profession})
        rows_list_2.append({'role': s.role,'dissent': s.dissent, 'name': s.staff.name, 'idhta_document': idhta})
    df_staff = pd.DataFrame(rows_list)
    df_hta_staff = pd.DataFrame(rows_list_2)
    dh.insert_reviewers(df_staff, dict_data['hta_agency'].name)
    dh.insert_hta_has_reviewer(df_hta_staff)

def insert_experts(dict_data, dh, id_hta):
    rows_list = []
    rows_list_2 = []
    for exp in dict_data['experts']:
        rows_list.append({'first_name': exp.expert.first_name, 'last_name': exp.expert.last_name, 'position': exp.expert.position})
    df_experts = pd.DataFrame(rows_list)
    dh.insert_experts(df_experts)
    df_experts['idhta_document'] = id_hta
    dh.insert_hta_has_experts(df_experts)

def upsert(table, df, row, idpico, dh):
    last_id = 0
    
    if table=='pico':
        log.info('Trying to insert pico')
        result = dh.insert_picos(df)
    elif table=='analysis':
        log.info('Trying to insert analysis')
        result = dh.insert_analysis(df)
    elif table=='outcome_measure':
        log.info('Trying to insert outcome measure')
        result = dh.insert_outcome_measure(df)
    else:
        log.info('Trying to insert population')
        result = dh.insert_population(df)
    
    if result:
        last_id = result.lastrowid
        log.info('Found something. Lastrowid: ' + str(last_id))
            #print(str(result.lastrowid))
        if last_id==0:
            log.info('Already here. See if we can find it')
            if table=='pico':
                last_id = dh.get_current_row(PICO, {col.name: getattr(row, col.name) for col in row.__table__.columns})
            elif table=='analysis':
                last_id = dh.get_current_row(Analysis, {'idpico': idpico})
            elif table=='outcome_measure':
                last_id = dh.get_current_row(Outcome_Measure, {'name': df['name'][0]})
            else:
                last_id = dh.get_current_row(Population, {col.name: getattr(row, col.name) for col in row.__table__.columns})
    else:
        log.info(table + ' insert failed')
    return last_id

def insert_picos_et_al(db_parent, dh, idhta):
    # population, pico, analysis, costs, trials
    k=0
    for p in db_parent.picos:
        # Add the demographics
        df_demo = pd.DataFrame({col.name: getattr(p.population.demographics, col.name) for col in p.population.demographics.__table__.columns if not re.match('id', col.name)}, index=[0])
        id_demo = dh.insert_demographics(df_demo)
        # Add the population
        df_pop = pd.DataFrame({col.name: getattr(p.population, col.name) for col in p.population.__table__.columns if not re.match('id|demo', col.name)}, index=[0])
        df_pop['id_demographics'] = id_demo
        idpop = upsert('population', df_pop, p.population, 0, dh)
        
        df_pico = pd.DataFrame({col.name: getattr(p, col.name) for col in p.__table__.columns if not re.match('id', col.name)}, index=[0])
        df_pico['idhta_document'] = idhta
        df_pico['idpopulation'] = idpop
        df_pico['product'] = db_parent.products[0].name
        
        last_id = upsert('pico', df_pico, p, 0, dh)
        
        df_analysis = pd.DataFrame({col.name: getattr(p.analysis, col.name) for col in p.analysis.__table__.columns if not re.match('id', col.name)}, index=[0])
        log.info('last pico id: ' + str(last_id))
        if last_id:
            #print('Got a result')
            df_analysis['idpico'] = last_id
            last_id = upsert('analysis', df_analysis, p, last_id, dh)
            if last_id and len(p.analysis.trials)>0: # The decison documents do not contain any trial info
                log.info('Adding trials: ' + str(len(p.analysis.trials)))
                if len(p.analysis.trials)>0:
                    for t in p.analysis.trials:
                        log.info('more than 0')
                        df_trial = pd.DataFrame({col.name: getattr(t, col.name) for col in t.__table__.columns if not re.match('id', col.name)}, index=[0])
                        df_trial['idanalysis'] = last_id
                        log.info('df_trial:')
                        log.info(df_trial)
                        id_trial = dh.insert_trial(df_trial)
                        for r in t.outcome_values:
                            df_outcome_measure = pd.DataFrame({col.name: getattr(r.outcome_measure, col.name) for col in r.outcome_measure.__table__.columns if not re.match('id', col.name)}, index=[0])
                            id_om = upsert('outcome_measure', df_outcome_measure, p, last_id, dh)
                            df_results = pd.DataFrame({col.name: getattr(r, col.name) for col in r.__table__.columns if not re.match('id', col.name)}, index=[0])
                            df_results['idtrial'] = id_trial
                            df_results['idoutcome_measure'] = id_om
                            dh.to_sql_update(df_results, 'outcome_value')
                            

                if len(p.analysis.costs)>0:
                    for c in p.analysis.costs:
                        df_costs = pd.DataFrame({col.name: getattr(c, col.name) for col in c.__table__.columns if not re.match('id', col.name)}, index=[0])
                        df_costs['idanalysis'] = last_id
                        dh.insert_costs(df_costs)
        else:
            log.error('Could not insert pico')
            log.info(df_pico.to_string())

def dump_queue(q):
    q.put(None)
    q_list = list(iter(q.get(timeout=0.00001), None))
    with open('./unprocessable.csv', 'w') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        wr.writerow(q_list)

if __name__ == "__main__":

    multi_threading = False
    
    if multi_threading:
        #data_ready = Event()
        #parser_done = Event()
        pool_size=6#12
        pool_size_inserter=4#4
        file_queue = create_file_queue()
        data_queue = queue.Queue()
        unparseable_queue = queue.Queue()
        log.info('Creating pool of workers')
        parser = Thread(target=parser_manager, args=(file_queue, data_queue, unparseable_queue, pool_size,))
        parser.start()
        inserter = Thread(target=insert_manager, args=(data_queue, pool_size_inserter,))
        inserter.start()
        # wait for the producer to finish
        parser.join()
        # wait for the consumer to finish
        inserter.join()
        file_queue.join()
        data_queue.join()
        dump_queue(unparseable_queue)
        #with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        #    executor.map
        #    executor.submit(parser_work, file_queue, data_queue)
        #    executor.submit(insert_work, data_queue)
        log.info('Parsing finished!')
    else:
        dh = DataHandlerProduction()

        k=0
        for root, dirs, files in os.walk(file_dir, topdown=False):
            for file in files[::-1]:
                k = k+1
                log.info('Processing file ' + str(k))
                if dummy:
                    with open(SAVE_PATH + "before_pydant.json") as f:
                        data = json.load(f)
                    #data = dummy_data
                    # Serializing json
                    #with open(file_dir_out + "sample_" + str(k)+ ".json", "w") as outfile:
                    #    json.dump(data, outfile)
                else:
                    log.info(os.path.join(root, file))
                    if DOCUMENT_TYPE=='decision':
                        parser = worker.Worker(client)
                        data = parser.parse_file_2(file_dir + file)
                    elif DOCUMENT_TYPE=='dossier':
                        log.info('Parsing dossier: ' + file_dir)
                        parser = worker_dossier.Worker_dossier(client, doc_type=DOCUMENT_TYPE)
                        data = parser.parse_file_2(file_dir)
                    else:
                        parser = worker_basis.Worker_basis(client, doc_type=DOCUMENT_TYPE)
                        data = parser.parse_file_2(file_dir + file)

                if not data:
                    continue
                try:
                    parse_and_insert(data)
                except Exception as e:
                    log.error('Unable to insert data for file ' + data['diarie_nr'], exc_info=sys.exc_info())
                # data_dict = data.model_dump()
                # log.info("Extracted product: " + data.title)
                # #log.info("Extracted trials: " + str(len(data_dict['picos'][0]['analysis']['trials'])))
                # #db_parent = HTA_Document(**data_dict,session=None)
                
                # if DOCUMENT_TYPE=='decision':
                #     db_parent = HTA_Document(**data_dict,session=None)

                # else:
                #     db_parent = HTA_Document_Basis(**data_dict,session=None)
                #     db_parent.decision = 'no decision'
                # db_parent.document_type = DOCUMENT_TYPE
                # #else:
                # #    db_parent.latest_decision_date = None
                # log.info('db_parent:')
                # log.info(db_parent.__dict__)
                # #log.info("Extracted trials after: " + str(len(db_parent.picos[0].analysis.trials)))
                # insert_data(db_parent)

            for name in dirs:
                log.info(os.path.join(root, name))