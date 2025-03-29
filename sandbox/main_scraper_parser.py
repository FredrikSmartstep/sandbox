
from datetime import datetime
from sqlalchemy import inspect
#import worker_dossier as wd
import worker_dossier_gemeni as wd_gem
import worker_dossier_combo as wd
import worker_dossier_merger as wd_mer
import sys
import json
import re
import pandas as pd
from openai import OpenAI
from logger_tt import getLogger, setup_logging
from sqlalchemy_models360 import HTA_Document, HTA_Document_Basis, PICO, Analysis, Population, Outcome_Measure
import sqlalchemy_models360_basis as m
from parse_decision_file import parse_decision_file
import scraping_tools as st
from data_cleaner import clean_up
import queue
from threading import Thread, Event
from multiprocessing.pool import ThreadPool
import csv
from data_handler_new import DataHandlerProduction, upser
import tempfile
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session

from secret import secrets

client = OpenAI(
    api_key=secrets.open_ai_key, 
    max_retries=4, 
    timeout=60.0)

setup_logging(use_multiprocessing=True, config_path='C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/log_config.json') # os.getcwd() + '/log_config.json')

log = getLogger(__name__)

# ----- settings ----------
RUN_BASICS = False
DOCUMENT_TYPE = 'nt-basis'#'dossier' #'decision' # basis 

DUMMY = False
MULTI_THREADING = False

pool_size=3 #12 trying with 4 to see if that is enough to get a stable request rate at about 5
pool_size_inserter=3#4
#--------------------------

unparseable = []

MAX_NR_OF_RETRIES = 5


file_dir = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/' + DOCUMENT_TYPE + '/'

SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'

df12 = pd.read_csv(SAVE_PATH + 'files_agg_new3.csv', sep=';') # since we cannot use fritz when threading
df12 = clean_up(df12)


def parser_manager(links_queue, data_queue, unproc_queue, pool_size = 20):
    log.info('Starting parser')
    # create thread pool
    with ThreadPool(pool_size) as pool:
        # use threads to generate items and put into the queue
        _ = [pool.apply_async(parser_work, args=(links_queue, data_queue, unproc_queue, )) for _ in range(pool_size)]
        # wait for all tasks to complete
        pool.close()
        pool.join()
    # put a signal to expect no further tasks
    data_queue.put(None)
    # report a message
    print('Parser manager done.')
 
def parser_work(q_links, q_data, unproc_queue):
    log.info('Entered parser work')
    # run until there is no more work
    dh = DataHandlerProduction()
    while True:
        # retrieve one item from the queue
        log.info('Getting link')
        link_data = q_links.get()
        # check for signal of no more work
        if not link_data:
            log.info('No link data')
            # put back on the queue for other consumers
            q_data.put(None)
            # shutdown
            #parser_done.set()
            return
        try: 
            log.info('Dossiers in queue: ' + str(q_links.qsize()))

            data = run_sequential_parsing(link_data, 'first_run')

            if data:
                q_data.put(data)
                #data_ready.set()
                log.info('Data sent for insertion')
                #time.sleep(10)
                log.info('Successfully parsed file ' + link_data['product'])

        except Exception as e:
            log.error('Unable to parse file: ' + link_data['product'], exc_info=sys.exc_info())
            log.info('Adding file to unparseable queue')
            unproc_queue.put(link_data)
        finally:
            log.info('In finally')
            dh.close()
            q_links.task_done()
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
            insert_success = convert_and_insert(data)

        except Exception as e:
            log.error('Unable to insert data for file', exc_info=sys.exc_info())
        finally:
            q_data.task_done()


def clean_up_data(data, df12):
    #d = data.dict()
    d2 = {k: data[k] for k in ('products', 'company', 'date')}
    df_data  = pd.DataFrame({'company':d2['company']['name'], 'drug_name': d2['products'][0]['name'], 'decision_date': d2['date'], 'annual_turnover':'','active substance':'', 'severity':''}, index=[0])
    # Clean up company and product
    df_data_clean = clean_up(df_data)
    data['company']['name'] = df_data_clean['company'].iloc[0]
    data['products'][0]['name'] = df_data_clean['drug_name'].iloc[0]                         
    # compare and extract
    # diarie_nr, company, product
    if (not data['diarie_nr']) or (data['diarie_nr'][0:4]=='1234') or (data['diarie_nr'][0:4]=='0000'):
        df_manual = df12[(df12['drug_name']==data['products'][0]['name']) & (df12['document_type']=='decision')]  
        data['diarie_nr'] = df_manual['diarie_nr'].iloc[0]
    else:
        df_manual = df12[df12['diarie_nr']==data['diarie_nr']]    
    if not data['company']:
        data['company']['name'] = df_manual['company'].iloc[0]
    if len(data['products'])<1:
        data['products'][0]['name'] = df_manual['drug_name'].iloc[0] 
    # Decision 
    #decision_sv = filename.split('/')[-1].split('_')[-1].split('.')[0]
    #decision_dict = {'begransad': 'limited','generell': 'full', 'avslag-och-uteslutningar': 'rejected'}
    #data.decision = decision_dict[decision_sv]
    
    return data

def check_if_in_db(tmpdirname, prod_name):
    from pypdf import PdfReader
    import re, os
    dh = DataHandlerProduction()
    for root, dirs, files in os.walk(tmpdirname, topdown=False):
        k=0
        for file in files[::-1]:
            k = k+1
            log.info('Trying to get dossier nr for: ' + prod_name + ' file nr ' + str(k))
            file_path = os.path.join(tmpdirname, file)
            reader = PdfReader(file_path)
            meta = reader.metadata
            diarie_match = re.search('\d{1,4}[\/\,] ?20[0-2]\d', str(meta.keywords)) # at least one had a comma instead of a slash. also white space after maz occur '\d{1,4}[\/\,] ?20[0-2]\d'
            log.info('Extracted metadata for '+ prod_name + ' file nr ' + str(k))
            if diarie_match:
                if dh.get_hta_with_diarie_nr_and_document_type(diarie_match[0], DOCUMENT_TYPE):
                    log.info('Found in db')
                    return True
    log.info('Not found in db')
    return False

def get_and_parse(link_data, parsing_class='combo'):
    if parsing_class=='combo':
        parser = wd.Worker_dossier(client, doc_type=DOCUMENT_TYPE, dh=DataHandlerProduction())
    elif parsing_class=='merger': # should not come here
        log.error('Merger not expected here')
        pass
    else:
        # "gemini-2.0-flash-thinking-exp-01-21" json mode not enabled
        parser = wd_gem.Worker_dossier(client, doc_type=DOCUMENT_TYPE, gem_model="gemini-2.0-flash", dh=DataHandlerProduction(), alternate=True)
    
    with tempfile.TemporaryDirectory() as tmpdirname:
        log.info('Getting the files')
        quality = None
        vs = None
        # download files to temp dir
        if st.get_files(link_data, temp_dir=tmpdirname):
            # Check if already in DB
            if not check_if_in_db(tmpdirname, link_data['product']):
                log.info('Parsing dossier: ' + link_data['product'] + ' with ' + parsing_class)
                data, quality, vs = parser.parse_file_2(tmpdirname, link_data['link'], link_data['decision'])
                if not data:
                    log.info('Parsing failed, returning empty')
                    return None, quality, vs
                elif data=='found':
                    return 'found', quality, vs
                elif not quality['parseable']:
                    log.info('Parsing ran into issues.')
                    return data, quality, vs
                else:
                    log.info("Extracted product: " + data['title'])
                    data = clean_up_data(data, df12) 
                    return data, quality, vs
            else:
                log.info('Dossier already in database.')
                return 'found', quality, vs
        else:
            log.info('No file found')
        return None, quality, vs


def convert_and_insert(data):
    data_dict = data#.model_dump()
    log.info("Extracted product: " + data['title'])
    log.info('Received data:')
    log.info(data)#.model_dump())
    log.info('Trying to put into HTA_Document')
    
    db_parent = HTA_Document_Basis(**data_dict,session=None)
    
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
    #    log.error('Unable to insert data for file ' + data['diarie_nr'], exc_info=sys.exc_info())
    #finally:
    #    log.info('In finally for convert_and_insert')
        return True

def insert_data(db_parent):
    insert_score = {'hta': False, 'indication': False, 'staff': False, 'expert': False, 'references': False, 'pico': False, 'analysis': False, 'trial': False}
    dh = DataHandlerProduction()
    dict_data = db_parent.__dict__ # why changed? {(col, getattr(db_parent, col)) for col in db_parent.__table__.columns.keys()}#
    log.info('dict data')
    log.info(dict_data)
    # insert agency
    #log.info('Insert agency')
    #dh.insert_agency(db_parent.hta_agency.name)
    # insert company
    #log.info('Insert company')
    #insert_company(db_parent, dh)
    
    # insert hta_document
    idhta_doc = insert_hta_document(db_parent, dh=dh)
    if idhta_doc>-1:
        log.info('Insert to HTA successsful: ' + str(idhta_doc))
        insert_score['hta'] = True
    id_hta_ind = insert_hta_indication(db_parent, dh, idhta_doc)
    if id_hta_ind>-1:
        insert_score['indication'] = True
    # insert product
    #log.info('Insert product')
    #insert_hta_product(dict_data, dh, idhta_doc)
    
    # insert staff
    # Some odd old documents lack staff
    log.info('Nr of staff: ' + str(len(db_parent.staff)))
    log.info('Nr of experts: ' + str(len(db_parent.experts)))
    if len(db_parent.staff)>0: 
        log.info('Insert staff')
        idstaff = insert_staff(dict_data, dh, idhta_doc)
        if idstaff>-1:
            insert_score['staff'] = True
    if len(db_parent.experts)>0: 
        log.info('Insert experts')
        idexp = insert_experts(dict_data, dh, idhta_doc)
        if idexp>-1:
            insert_score['expert'] = True
    # insert references (if any)
    if DOCUMENT_TYPE!='decision':
        if len(db_parent.references)>0:
            idref = insert_references(dict_data, dh, idhta_doc)
            if idref>-1:
                insert_score['references'] = True
    # insert pico, analysis, trial, costs
    log.info('Insert picos')
    insert_score = insert_picos_et_al(db_parent, dh, idhta_doc, insert_score)
    # TODO add score card for insert
    if insert_score['hta'] & insert_score['pico'] & insert_score['analysis']:
        log.info('Successfully inserted data from file ' + dict_data['diarie_nr'])
    else:
        log.error('Insert failed for file ' + dict_data['diarie_nr'] + 'with scorecard:')
        log.error(insert_score)
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
    df_ref = pd.DataFrame(ref_list).dropna(axis=1) # To get rid of null
    dh.insert_references(df_ref)
    df_ref['idhta_document'] = id_hta
    return dh.insert_hta_references(df_ref)

def insert_hta_document(db_parent, dh=None):
    # get rid of time of latest
    if db_parent.latest_decision_date:
        log.info('latest found')
        if re.match('\d{4}-\d{2}-\d{2}',db_parent.latest_decision_date):
            db_parent.latest_decision_date = re.match('\d{4}-\d{2}-\d{2}',db_parent.latest_decision_date)[0]
    log.info('latest to insert: ' + str(db_parent.latest_decision_date))
    with Session(dh.engine) as session:

        log.info('Add company')
        comp = m.Company(name=db_parent.company.name)
        comp = session.merge(comp)

        log.info('Add agency')
        hta_ag = m.HTA_Agency.clone(db_parent.hta_agency)
        ag = session.merge(hta_ag)

        log.info("Add document")
        hta_doc = m.HTA_Document.clone(db_parent)
        
        hta_doc = session.merge(hta_doc)
        hta_doc.company = comp
        hta_doc.hta_agency = ag

        session.commit()

    with Session(dh.engine) as session: # another session is neeeded since the idhta isn't updated during the meerge but stays None

        log.info("Add products")
        new_prods = []
        for p in db_parent.products:
            prod = m.Product.clone(p)
            prod = session.merge(prod)
            new_prods.append(prod)

        hta_doc_2 = m.HTA_Document.clone(db_parent)
        
        hta_doc_2 = session.merge(hta_doc_2)
        hta_doc_2.products = new_prods

        session.commit()

        id = hta_doc_2.id
    
    return id

#def insert_picos(b_parent, dh, idhta):
#    with Session(dh.engine) as session:
        

def insert_hta_indication(db_parent, dh, idhta):
    rows_list = []
    for ind in db_parent.indications:
        rows_list.append({'idhta_document': idhta, 'icd10_code': ind.indication.icd10_code, 'severity': ind.severity}) 
    df_hta_indications = pd.DataFrame(rows_list) 
    return dh.insert_hta_indication(df_hta_indications)

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
    return dh.insert_hta_has_reviewer(df_hta_staff)

def insert_experts(dict_data, dh, id_hta):
    rows_list = []
    for exp in dict_data['experts']:
        rows_list.append({'first_name': exp.expert.first_name, 'last_name': exp.expert.last_name, 'position': exp.expert.position})
    df_experts = pd.DataFrame(rows_list)
    dh.insert_experts(df_experts)
    df_experts['idhta_document'] = id_hta
    return dh.insert_hta_has_experts(df_experts)

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

def insert_picos_et_al(db_parent, dh, idhta, scorecard):
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
        df_pico['icd10_code'] = p.indication.icd10_code
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
                        trial = m.Trial(**df_trial.T.iloc[:,0].to_dict())
                        with Session(dh.engine) as session:
                            session.add(trial)
                            session.merge(trial)
                            session.commit()
                            id_trial = trial.id
                        #id_trial = dh.insert_trial(df_trial)
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
    return scorecard

def dump_queue(q):
    q.put(None)
    q_list = list(iter(q.get(timeout=0.00001), None))
    with open('./unprocessable2.csv', 'w') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        wr.writerow(q_list)


def run_sequential_parsing(link_data, parsing_class, combo_data = None, vs_combo = None):
    # 1. Run using combo (passing 'first_run' as parsing_class arg)
    # 2. If failed, run a second time using 'combo' as parsing_class arg
    # 3. If failed, run gemini and send also the combo_data and the dossier vector store  
    # 4. After the gemni run has finished, immediately send to the merger along with both data results and the dossier vs

    data, quality, vs = get_and_parse(link_data, parsing_class=parsing_class)
    
    if data=='found':
        return
    
    #quality['parseable'] = False
    
    log.info('Adding parsing quality scores to file')
    pd.DataFrame(quality, index=[0]).to_csv('parsing_quality_single_run_' + parsing_class + '.csv', mode='a', index=False, header=False)

    #if parsing_class=='gemini': #
    #        # Send off to merger TODO
    #        merger = wd_mer.Worker_dossier()
    #        data, quality = merger.merge(combo_data, data, vs_combo)

    if not data or not quality['parseable']:                
        log.error('Unable to parse dossier: ' + link_data['product'] + 'with ' + parsing_class, exc_info=sys.exc_info())

        if parsing_class=='first_run': # second run. Has proven to work and yield another xx%
            run_sequential_parsing(link_data, parsing_class='gemini')
        elif parsing_class=='gemini': # 
            # Send to gemini
            run_sequential_parsing(link_data, parsing_class='combo')#, combo_data=data, vs_combo=vs)
        else: # merger
            log.error('Parsing unsuccessful for dossier: ' + link_data['product'])
            return False
    return data           


def create_links_queue():
    if DOCUMENT_TYPE=='dossier':
        FILENAME = './links.json'
    else:
        FILENAME = './nt_links.json'

    if RUN_BASICS:
        if DOCUMENT_TYPE=='dossier':
            links = st.get_pharma_reimbursement_links()
        else:
            links = st.get_nt_assessment_links()

        with open(FILENAME, 'w') as myfile:
            wr = myfile.write(json.dumps(links))

    else: 
        with open(FILENAME, 'r') as f:
            links = json.load(f)
    # start with a small set
    links.reverse()
    #links = links[350:]
    q = queue.Queue()
    [q.put(li) for li in links]

    return q

        
if __name__ == "__main__":

    links_queue = create_links_queue() 
    log.info('Queue length: ' + str(links_queue.qsize()))

    if MULTI_THREADING:
        #data_ready = Event()
        #parser_done = Event()
        data_queue = queue.Queue()
        unparseable_queue = queue.Queue()
        log.info('Creating pool of workers')
        parser = Thread(target=parser_manager, args=(links_queue, data_queue, unparseable_queue, pool_size,))
        parser.start()
        inserter = Thread(target=insert_manager, args=(data_queue, pool_size_inserter,))
        inserter.start()
        # wait for the producer to finish
        parser.join()
        # wait for the consumer to finish
        inserter.join()
        links_queue.join()
        data_queue.join()
        dump_queue(unparseable_queue)
        #with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        #    executor.map
        #    executor.submit(parser_work, file_queue, data_queue)
        #    executor.submit(insert_work, data_queue)
        log.info('Parsing finished!')
    else:
        k=0
        for link_data in iter(links_queue.get, None):
            k = k+1
            log.info('Processing dossier ' + str(k))
            if DUMMY:
                with open(SAVE_PATH + "before_pydant.json") as f:
                    data = json.load(f)
            else:
                data = run_sequential_parsing(link_data, 'first_run') #'first_run' combo
                if data:
                    log.info('Data sent for insertion')
                    log.info('Successfully parsed file ' + link_data['product'])
                    try:
                        convert_and_insert(data)
                    except Exception as e:
                        log.error('Unable to insert data for file ' + data['diarie_nr'], exc_info=sys.exc_info())
            #     data, quality = get_and_parse(link_data)
            # if not data or not quality['parseable']:                
            #     log.error('Unable to parse dossier: ' + link_data['product'] + 'with combo', exc_info=sys.exc_info())
            #     log.info('Adding parsing quality scores to file')
            #     pd.DataFrame(quality, index=[0]).to_csv('parsing_quality_single_run.csv', mode='a', index=False, header=False)
            #     # Send to gemini
            #     data, quality = get_and_parse(link_data, parsing_class='gemini')
            #     pd.DataFrame(quality, index=[0]).to_csv('parsing_quality_single_run_gemini.csv', mode='a', index=False, header=False)
            #     # Send off to merger

            # elif data=='found':
            #     continue
            # else:    
            #     try:
            #         convert_and_insert(data)
            #     except Exception as e:
            #         log.error('Unable to insert data for file ' + data['diarie_nr'], exc_info=sys.exc_info())
