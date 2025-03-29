import json
import os
from os.path import join, dirname, abspath 
import sqlalchemy as sqlal
from sqlalchemy import exc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_model4 import Base, HTADocument, Company, HTAAgency, Form, Indication, Trial, \
    NTCouncilRecommendation, Staff, Price, Product, Analysis, ProductCompanyAssociation, HTADocumentIndicationAssociation
from logger_tt import getLogger
import numpy as np
from secret import secrets
log = getLogger(__name__)

SSL_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/ssl/DigiCertGlobalRootCA.crt.pem'

logic_dict = {"no": False, "yes": True, "NA": None, "": None}

def clean_str_for_float(string):
        string = string.replace(",",".").replace(" ","")
        if string=="":
            return None
        else:
            return string
        
class DataHandler():
    """
    Class for connecting to production database
    """

    def __init__(self, engine=None):
        if engine:
            log.info('Connecting using provided engine')
            self.engine = engine
        else: 
            mysql_config = 'mysql_config.yml'

            SQLALCHEMY_DATABASE_URI = None#os.getenv('SQLALCHEMY_DATABASE_URI') or None

            #print(SQLALCHEMY_DATABASE_URI)
            
            if SQLALCHEMY_DATABASE_URI: # Connect using env_var if it's initialized.
                temp = SQLALCHEMY_DATABASE_URI
                connect_text = "MYSQL CONNECTION using env key: SQLALCHEMY_DATABASE_URI"
            else:
                with open(join(dirname(abspath(__file__)), mysql_config), 'r') as ymlfile:
                    #cfg = yaml.safe_load(ymlfile)
                    #cfg = yaml.load(ymlfile)
                    host = 'smartstepnordics-mysql8.mysql.database.azure.com'#cfg['mysql']['host']
                    port = '3306'#cfg['mysql']['port']
                    user = 'smartstepnordics_adm'#cfg['mysql']['user']
                    pw = secrets.mysql_pwd
                    self.dbschema = 'documents_db'#cfg['mysql']['db']

                    temp = r'mysql+pymysql://' + user + ':' + pw + '@' + host + ':' + str(port) + '/' + self.dbschema + "?charset=utf8mb4"
                    connect_text = "### MYSQL CONNECTION user=%s host=%s schema=%s" % (user, host, self.dbschema)

            ssl_args = {'ssl': { 'ca':SSL_PATH}}
            conn = temp
            # pool_pre_ping are used to make sure we have a connection before doing a query
            self.engine = sqlal.create_engine(conn, connect_args=ssl_args)
        # See https://docs.sqlalchemy.org/en/14/core/pooling.html#pooling-multiprocessing
        #self.engine.dispose() 
        self.connection = self.engine.connect()
        self.Session = sessionmaker(bind=self.engine)
        self._add_process_guards()
        log.info(connect_text)
    
    def _add_process_guards(self):
        """Add multiprocessing guards.
        Forces a connection to be reconnected if it is detected
        as having been shared to a sub-process.
        """

        @sqlal.event.listens_for(self.engine, "connect")
        def connect(dbapi_connection, connection_record):
            connection_record.info['pid'] = os.getpid()

        @sqlal.event.listens_for(self.engine, "checkout")
        def checkout(dbapi_connection, connection_record, connection_proxy):
            pid = os.getpid()
            if connection_record.info['pid'] != pid:
                log.exception('Database connection clash')
                connection_record.connection = connection_proxy.connection = None
                raise exc.DisconnectionError(
                    "Connection record belongs to pid %s, "
                    "attempting to check out in pid %s" %
                    (connection_record.info['pid'], pid)
                )
    
    def close(self):
        log.info('Closing DB connection')
        self.connection.close()
        self.engine.dispose()
        log.info('DB engine dispose executed')

    def insert_HTA(self, data): 
        log.info("Creating a new database session")   
        session = self.Session()
        session.autoflush = False
        # Create new entries
        agency = self.add_agency(session, 'TLV')
        company = self.add_company(session, data['applicant'])
        product = self.add_product(session, data, company)

        for form_data in data['form']:
            self.add_form(session, form_data, product, company)

        hta_doc = self.add_HTA_document(session, data, company, agency)

        for ind in data['indication']:
            indication = self.add_indication(session, ind) 
            indication_here = False
            for indication_ass in hta_doc.indications:
                if indication==indication_ass.indication:
                    indication_here = True
            if not indication_here:
                hta_doc.indications.append(HTADocumentIndicationAssociation(document=hta_doc, indication=indication))

        for analysis_data in data['HE_results']:
            analysis = self.add_analysis(session, analysis_data, hta_doc)

            for trial_data in analysis_data['trials_company']:
                analysis.trials.append(self.add_trial(session, trial_data))
            hta_doc.analyses.append(analysis)

        for person_data in data['decision_makers']:
            self.add_person(session, person_data, agency)
            #, hta_doc, 'board member'))
        
        self.add_person(session, data['presenter_to_the_board'], agency)
            #, hta_doc, 'presenter to board'))

        session.add(hta_doc)
        # Commit the session
        session.commit()

        # Close the session
        session.close()
    
    @staticmethod
    def add_analysis(session, analysis_data, hta_doc):
        log.info('add analysis')
        try:
            analysis = session.query(Analysis).filter_by(
                HTA_document = hta_doc,
                cohort = analysis_data['population']).first()
        except:
            analysis = None
        if not analysis:
            analysis = Analysis(
                HTA_document = hta_doc,
                cohort = analysis_data['population'],
                intervention = analysis_data['intervention'],
                comparators_company = analysis_data['comparators_company'],
                comparators_agency = analysis_data['comparators_TLV'],
                comparator_reason_company = analysis_data['comparator_reason_company'],
                comparator_reason_agency = analysis_data['comparator_reason_TLV'],
                QALY_gain_company = analysis_data['QALY_gain_company'],
                QALY_gain_agency_lower = analysis_data['QALY_gain_TLV_lower'],
                QALY_gain_agency_higher = analysis_data['QALY_gain_TLV_higher'],
                QALY_total_cost_company = analysis_data['QALY_total_cost_company'],
                QALY_total_cost_agency_lower = analysis_data['QALY_total_cost_TLV_lower'],
                QALY_total_cost_agency_higher = analysis_data['QALY_total_cost_TLV_higher'],
                comparison_method = analysis_data['comparison_method'],
                indirect_method = analysis_data['indirect_method']
            )
            session.add(analysis)
        return analysis
    
    @staticmethod
    def add_form(session, form_data, product, company):
        try:
            form = session.query(Form).filter_by(form=form_data['form'], strength=form_data['strength'], product=product).first()
        except:
            form = None
        if not form:
            form = Form(form=form_data['form'], strength=form_data['strength'])
            form.product = product
            price = Price(AIP=float(clean_str_for_float(form_data['AIP'])), AUP=float(clean_str_for_float(form_data['AUP'])))
            price.company = company
            form.prices.append(price)
        session.add(form)


    @staticmethod
    def add_person(session, person_data, agency):
        log.info('add person')
        person = session.query(Staff).filter_by(name=person_data['name']).first()
        if not person:
            person = Staff(name=person_data['name'], title=person_data['profession'], agency=agency)
            session.add(person)
            #session.commit()
        return person

    @staticmethod
    def add_product(session, data, company):
        log.info('add product')
        product = session.query(Product).filter_by(name=data['product_name']).first()
        if not product:
            product = Product(name=data['product_name'])
            product.prod_companies.append(ProductCompanyAssociation(company=company, role="agent"))

        session.add(product)
        
        return product

    @staticmethod
    def add_company(session, name):
        log.info('add company')
        company = session.query(Company).filter_by(name=name).first()
        if not company:
            company = Company(name=name)
            session.add(company)
            #session.commit()
        return company

    @staticmethod  
    def add_agency(session, name):
        log.info('add agency')
        agency = session.query(HTAAgency).filter_by(name=name).first()
        if not agency:
            agency = HTAAgency(name=name)
            session.add(agency)
            #session.commit()
        return agency

    @staticmethod
    def add_HTA_document(session, data, company, agency):
        log.info('add HTA document')
        hta_document = session.query(HTADocument).filter_by(title=data['title'],
                                                            diarie_nr=data['diarie_number'],
                                                            date=data['date']).first()
        if not hta_document:
            hta_document = HTADocument(
                title=data['title'],
                diarie_nr=data['diarie_number'],
                date=data['date'],
                decision=data['decision'],
                currency=data['currency'],
                document_type = data['document_type'],
                requested_complement=logic_dict[data['requested_complement']],
                requested_information=data['requested_information'],
                requested_complement_submitted=logic_dict[data['requested_complement_submitted']],
                #summary=data['efficacy_summary'],
                company=company,
                agency=agency
            )
        #session.add(hta_document)
        #session.commit()
        return hta_document

    @staticmethod    
    def add_indication(session, indication_data):
        log.info('add indication')
        indication = session.query(Indication).filter_by(who_full_desc=indication_data['indication']).first()
        if not indication:
            indication = Indication(who_full_desc=indication_data['indication'])
            session.add(indication)
            #session.commit()
        return indication
    
    @staticmethod
    def add_trial(session, trial_data):
        log.info('add trial')
        try:
            trial = session.query(Trial).filter_by(
                title_of_paper = trial_data['title_of_paper'],
                primary_outcome_variable = trial_data['primary_outcome_variable']).first()
        except:
            trial = None
        if not trial:
            trial = Trial(
                title = trial_data['title'],
                nr_of_patients = trial_data['number_of_patients'] if isinstance(trial_data['number_of_patients'], int) else None,
                nr_of_controls =  trial_data['number_of_controls'] if isinstance(trial_data['number_of_controls'], int) else None,
                duration = trial_data['duration'],
                phase = trial_data['phase'],
                meta_analysis = logic_dict[trial_data['meta-analysis']],
                randomized = logic_dict[trial_data['randomized']],
                controlled = logic_dict[trial_data['controlled']],
                blinded = trial_data['blinded'],
                primary_outcome = trial_data['primary_outcome_variable'],
                results = trial_data['results'],
                safety = trial_data['safety'],
            )
        session.add(trial)
        return trial
    