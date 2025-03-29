import json
import pandas as pd
import re
import os
from os.path import join, dirname, abspath 
import sqlalchemy as sqlal
from sqlalchemy import exc
from sqlalchemy.inspection import inspect

from sqlmodel import Field, Session, SQLModel, create_engine, UniqueConstraint, inspect#, insert
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
#from db_model4 import Base, HTADocument, Company, HTAAgency, Form, Indication, Trial, \
#    NTCouncilRecommendation, Staff, Price, Product, Analysis, ProductCompanyAssociation, HTADocumentIndicationAssociation
from json_model2 import HTA_Document, HTA_Agency, Company, Product, Product_Company, PICO
from logger_tt import getLogger
import numpy as np
from sqlalchemy import func, MetaData, Table
from sqlalchemy.dialects.mysql import insert
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
            self.engine = create_engine(conn, connect_args=ssl_args)
            #self.async_engine = create_async_engine(conn, connect_args=ssl_args, echo=True, future=True)
        # See https://docs.sqlalchemy.org/en/14/core/pooling.html#pooling-multiprocessing
        #self.engine.dispose() 
        #self.connection = self.engine.connect()
        self.meta = MetaData()
        self.meta.reflect(bind=self.engine)
        
        #self.session = Session(self.engine)
        #self._add_process_guards() Suddenly Getting strange lack of pid error
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

    def new_session(self):
        log.info("Creating a new database session")
        self.session = Session(self.engine)

    def insert_HTA(self, hta_doc, data_agency, data_company, data_products, data_picos): 
              
        self.session.autoflush = False #Isit this that fucks up the add?
        #self.session.autobegin = False # the executes should not start a transaction. We want to control this ourselves by commit
        #with self.session.begin():
        log.info('Adding agency')
        #hta_doc.agency.name = re.sub(r'(Tand.*)|(.*TLV.*)', 'TLV', hta_doc.agency.name)
        #data_agency.name = re.sub(r'(Tand.*)|(.*TLV.*)', 'TLV', data_agency.name)
        #hta_doc.agency.name = hta_doc.agency.name.replace(r'(Tand.*)|(.*TLV.*)', 'TLV', regex=True, flags=re.I)
        log.info(data_agency.model_dump())
        self.upsert_with_merge(self.session, HTA_Agency, data_agency) 
        #agency = self.upsert_with_merge(self.session, HTA_Agency, hta_doc.agency) 
        log.info('Added agency:')
        log.info(data_agency.model_dump())
        log.info('Adding company')
        log.info(data_company.model_dump())
        company = self.upsert_with_merge(self.session, Company, data_company)
        # for p in data_picos:
        #         log.info(p.model_dump())
        #         self.upsert_with_merge(self.session, PICO, p)
        if False:
            
            log.info('Added company:')
            log.info(company.model_dump())
            log.info('Adding products')
            prods = []
            prod_comp_list = []
            for p in hta_doc.products:
                log.info(p.model_dump())
                prods.append(self.upsert_with_merge(self.session, Product, p))
                prod_comp_list.append(Product_Company(company=company, product=p, role='owner'))
            log.info('Adding prod comp role')
            self.session.add_all(prod_comp_list)
        #hta_doc.company.products = prods
        log.info('Adding agency to hta:')
        log.info(data_agency.model_dump())
        #hta_doc.agency = data_agency
        hta_doc.idhta_agency = data_agency.idhta_agency # this shouldn't be necessary. Why not inferred from the collection?
        log.info('Adding company to hta')
        log.info(data_company.model_dump())
        #hta_doc.company = company no need if id is added
        hta_doc.idcompany = data_company.idcompany # this shouldn't be necessary. Why not inferred from the collection?
        #hta_doc.products = prods
        #hta_doc.picos = data_picos doesn't work
        
        log.info('Adding products')
        for p in data_products:
            self.upsert_with_merge(session=self.session, model=Product, data=p)
            prod_comp = Product_Company(idcompany=data_company.idcompany, idproduct=p.idproduct, role='owner')
            self.upsert_with_merge(session=self.session, model=Product_Company, data=prod_comp)

        log.info('Adding doc')
        self.upsert_with_merge(session=self.session, model=HTA_Document, data=hta_doc)
        
        log.info('Adding picos')
        for p in data_picos:
            p.idhta_document = hta_doc.idhta_document
            self.upsert_with_merge(session=self.session, model=PICO, data=p)
        #self.upsert(self.session,HTA_Agency, hta_doc.agency)
        # Create new entries
        #with Session(self.engine) as session:
       #async with AsyncSession(self.async_engine) as session:
            #session.add(data.agency) #self.add_agency(session, data.agency)
            #for prod in data.products:
            #    session.add(prod)
        # company = self.add_company(session, data['applicant'])
        # product = self.add_product(session, data, company)

        # for form_data in data['form']:
        #     self.add_form(session, form_data, product, company)

        # hta_doc = self.add_HTA_document(session, data, company, agency)

        # for ind in data['indication']:
        #     indication = self.add_indication(session, ind) 
        #     indication_here = False
        #     for indication_ass in hta_doc.indications:
        #         if indication==indication_ass.indication:
        #             indication_here = True
        #     if not indication_here:
        #         hta_doc.indications.append(HTADocumentIndicationAssociation(document=hta_doc, indication=indication))

        # for analysis_data in data['HE_results']:
        #     analysis = self.add_analysis(session, analysis_data, hta_doc)

        #     for trial_data in analysis_data['trials_company']:
        #         analysis.trials.append(self.add_trial(session, trial_data))
        #     hta_doc.analyses.append(analysis)

        # for person_data in data['decision_makers']:
        #     self.add_person(session, person_data, agency)
        #     #, hta_doc, 'board member'))
        
        # self.add_person(session, data['presenter_to_the_board'], agency)
        #     #, hta_doc, 'presenter to board'))

            #session.add(data)
            #await HTA_Document.upsert(data, session)
        # Commit the session
        #self.session.commit()

        # Close the session
        #self.session.close()

        # order
        # hta_agency
        # company
        # products
        # hta_document
        # picos
        # analysis
        # trial
        # staff
        # indication


    def close_session(self):
        # Commit the session
        self.session.commit()

        # Close the session
        self.session.close()

    @staticmethod
    def sqlmodel_to_df(obj: SQLModel) -> pd.DataFrame:
        """Convert a SQLModel objects into a pandas DataFrame."""
        return pd.DataFrame.from_records(obj.model_dump())

    def upsert_with_merge(self, session, model, data, exclude=set()):
        # First try to find the object in the db using the unique column(s)
        # If found, we want to update the attributes
        # for any children collections, we would like to do the same, i.e., first check if exists and then update or create
        # thus we should use a recursive pattern. However, if we create objects in a bottom-up approach this wont be necessary
        # If not found, we want to add this new row

        result = None
        # The unique columns used to find existing rows in the db 
        if len(inspect(model).class_._upsert_index_elements.default)>0:
            col = list(inspect(model).class_._upsert_index_elements.default)[0]
            log.info('adding, update if common ' + col)
            result = session.query(model).filter_by(**{col: data.__getattr__(col)}).first()
        exclude.add(model.__tablename__)
        # iterate over all foreign keys
        #fks = Table('hta_document',self.meta).foreign_keys
        fks = [x for x in self.meta.tables[model.__tablename__].foreign_keys]# if x[0].split('.') not in exclude]
        
        for k in fks:#inspect(model).relationships.items():
            # get model name, and dont run for already queed
            log.info(str(k.target_fullname))
            #if k[0] not in exclude:
            #    upsert_with_merge()

        
        if result is None:
            #result = data
            log.info('Not found in DB. New!')

            session.add(data)
            #session.commit()
            #session.refresh(result)
        else:
            log.info('Found in db')
            # Let sqlalchemy understand that they are the same instance
             # generalized way of: data.idhta_agency = result.idhta_agency
            pk_name = inspect(model).primary_key[0].name
            setattr(data, pk_name, result.__getattr__(pk_name))
            # Let's bring in the database object but update with attributes from 'data' in session
            session.merge(data) 
            # When running this for the HTA document and the subsequent idhta_document cannot be None:
            # At this point,
            # the variable data contains pico that in turn has a value for the idhta_document
            # Session.dirty contains the hta_document to be updated.
            # session.new, however, contains a set of PICO with idhta_document set to None???!!!  
            # Why are they not updated with the idhta_docment number?

        # To handle duplicate keys
        #insert_dict = data.model_dump()
        #inserted = insert(model).values(**insert_dict)
        #upserted = inserted.on_duplicate_key_update(
        #    **{k: inserted.inserted[k]
        #                        for k, v in insert_dict.items()})
        #res = session.execute(upserted)

        table = model.__table__
            
        session.execute(table
                 .insert()
                 .prefix_with('IGNORE')
                 .values(data.model_dump()))
        session.commit() 
        #session.refresh(data)

    @staticmethod
    def upsert_work(session,model, data):
        """model can be a db.Model or a table(), insert_dict should contain a primary or unique key."""
        insert_dict = data.model_dump()
        inserted = insert(model).values(**insert_dict)
        upserted = inserted.on_duplicate_key_update(
             **{k: inserted.inserted[k]
                                for k, v in insert_dict.items()})
        res = session.execute(upserted)
        # id=func.LAST_INSERT_ID(model.id),
        return res.lastrowid

    @staticmethod
    def upsert(session, model, rows):
        table = model.__table__
        query = insert(model).values(rows.dict())   # each record is a dict
        update_dict = {x.name: x for x in query.inserted}  # specify columns for update, u can filter some of it
        stmt = query.on_duplicate_key_update(**update_dict) # here's the modification: u should expand the columns dict
        # stmt = insert(table)
        # primary_keys = [key.name for key in inspect(table).primary_key]
        # update_dict = {c.name: c for c in stmt.excluded if not c.primary_key}

        # if not update_dict:
        #     raise ValueError("insert_or_update resulted in an empty update_dict")

        # stmt = stmt.on_conflict_do_update(index_elements=primary_keys,
        #                                 set_=update_dict)

        # seen = set()
        # foreign_keys = {col.name: list(col.foreign_keys)[0].column for col in table.columns if col.foreign_keys}
        # unique_constraints = [c for c in table.constraints if isinstance(c, UniqueConstraint)]
        # def handle_foreignkeys_constraints(row):
        #     for c_name, c_value in foreign_keys.items():
        #         foreign_obj = row.pop(c_value.table.name, None)
        #         row[c_name] = getattr(foreign_obj, c_value.name) if foreign_obj else None

        #     for const in unique_constraints:
        #         unique = tuple([const,] + [row[col.name] for col in const.columns])
        #         if unique in seen:
        #             return None
        #         seen.add(unique)

        #     return row

        # rows = list(filter(None, (handle_foreignkeys_constraints(row) for row in rows)))
        session.execute(stmt, rows)

    @staticmethod  
    def simple_upsert(session, model, data, col = 'name'):
        log.info('adding, update if common ' + col)
        result = session.query(model).filter_by(**{col: data.__getattr__(col)}).first()
        if result is None:
            result = data
            log.info('Not found in DB. New!')
            session.add(result)
        else:
            log.info('Found in db')
            if col=='title':
                log.info('idhta fore: ' + str(result.idhta_document))
            # sync the data
            for key, value in data.model_dump(exclude=[col], exclude_none=True).items():
                log.info('Overwriting ' + key + " with " + str(value))
                setattr(result, key, value)
            if col=='title':
                log.info('idhta: ' + str(result.idhta_document))
            # persist the data to the database
    #        insert_stmt = insert(model.__table__).values(result.dict())
    #        update_dict = {x.name: x for x in insert_stmt.inserted}
    #        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(data={**update_dict}, status='U')
    #        session.execute(on_duplicate_key_stmt)   

            insert_dict = result.model_dump()
            inserted = insert(model).values(**insert_dict)
            upserted = inserted.on_duplicate_key_update(
                 **{k: inserted.inserted[k]
                                    for k, v in insert_dict.items()}) # idhta_agency=func.LAST_INSERT_ID(model.idhta_agency),
            session.execute(upserted)             
        #session.execute(model.__table__
        #        .insert()
        #        .prefix_with('IGNORE')
        #        .values(result.dict()))

        #session.add(result)
        #session.commit() not with begin
        #session.refresh(result)

        return result

    @staticmethod  
    def simple_upsert_ag(session, model, data, col = 'name'):
        log.info('adding, update if common ' + col)
        result = session.query(model).filter_by(**{col: data.__getattr__(col)}).first()
        if result is None:
            result = data
            log.info('Not found in DB. New!')
            session.add(result)
        else:
            log.info('Found in db')
            if col=='title':
                log.info('idhta fore: ' + str(result.idhta_document))
            # sync the data
            for key, value in data.model_dump(exclude=[col], exclude_none=True).items():
                log.info('Overwriting ' + key + " with " + str(value))
                setattr(result, key, value)
            if col=='title':
                log.info('idhta: ' + str(result.idhta_document))
            # persist the data to the database
    #        insert_stmt = insert(model.__table__).values(result.dict())
    #        update_dict = {x.name: x for x in insert_stmt.inserted}
    #        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(data={**update_dict}, status='U')
    #        session.execute(on_duplicate_key_stmt)   

            insert_dict = result.model_dump()
            inserted = insert(model).values(**insert_dict)
            upserted = inserted.on_duplicate_key_update(
                 **{k: inserted.inserted[k]
                                    for k, v in insert_dict.items()}) # idhta_agency=func.LAST_INSERT_ID(model.idhta_agency),
            session.execute(upserted)             
        #session.execute(model.__table__
        #        .insert()
        #        .prefix_with('IGNORE')
        #        .values(result.dict()))

        #session.add(result)
        session.commit()
        session.refresh(result)

        return result
    
    @staticmethod  
    def simple_upsert_prod(session, model, data, col = 'name'):
        log.info('adding, update if common ' + col)
        result = session.query(model).filter_by(**{col: data.__getattr__(col)}).first()
        if result is None:
            result = data
            log.info('Not found in DB. New!')
        else:
            log.info('Found in db')
            if col=='title':
                log.info('idhta fore: ' + str(result.idhta_document))
        # sync the data
        for key, value in data.model_dump(exclude=[col], exclude_none=True).items():
            log.info('Overwriting ' + key + " with " + str(value))
            setattr(result, key, value)
        if col=='title':
            log.info('idhta: ' + str(result.idhta_document))
        # persist the data to the database
#        insert_stmt = insert(model.__table__).values(result.dict())
#        update_dict = {x.name: x for x in insert_stmt.inserted}
#        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(data={**update_dict}, status='U')
#        session.execute(on_duplicate_key_stmt)   

        insert_dict = result.model_dump()
        inserted = insert(model).values(**insert_dict)
        upserted = inserted.on_duplicate_key_update(
             idproduct=func.LAST_INSERT_ID(model.idproduct), **{k: inserted.inserted[k]
                                for k, v in insert_dict.items()})
        session.execute(upserted)             
        #session.execute(model.__table__
        #        .insert()
        #        .prefix_with('IGNORE')
        #        .values(result.dict()))

        #session.add(result)
        session.commit()
        session.refresh(result)

        return result
    
    @staticmethod  
    def simple_upsert_comp(session, model, data, col = 'name'):
        log.info('adding, update if common ' + col)
        result = session.query(model).filter_by(**{col: data.__getattr__(col)}).first()
        if result is None:
            result = data
            log.info('Not found in DB. New!')
        else:
            log.info('Found in db')
            if col=='title':
                log.info('idhta fore: ' + str(result.idhta_document))
        # sync the data
        for key, value in data.model_dump(exclude=[col], exclude_none=True).items():
            log.info('Overwriting ' + key + " with " + str(value))
            setattr(result, key, value)
        if col=='title':
            log.info('idhta: ' + str(result.idhta_document))
        # persist the data to the database
#        insert_stmt = insert(model.__table__).values(result.dict())
#        update_dict = {x.name: x for x in insert_stmt.inserted}
#        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(data={**update_dict}, status='U')
#        session.execute(on_duplicate_key_stmt)   

        insert_dict = result.model_dump()
        inserted = insert(model).values(**insert_dict)
        upserted = inserted.on_duplicate_key_update(
             idcompany=func.LAST_INSERT_ID(model.idcompany), **{k: inserted.inserted[k]
                                for k, v in insert_dict.items()})
        session.execute(upserted)             
        #session.execute(model.__table__
        #        .insert()
        #        .prefix_with('IGNORE')
        #        .values(result.dict()))

        #session.add(result)
        session.commit()
        session.refresh(result)

        return result
    
    @staticmethod  
    def simple_upsert_hta(session, model, data, col = 'name'):
        log.info('adding, update if common ' + col)
        result = session.query(model).filter_by(**{col: data.__getattr__(col)}).first()
        if result is None:
            result = data
            log.info('Not found in DB. New!')
            session.add(result)
        else:
            log.info('Found in db')
            if col=='title':
                log.info('idhta fore: ' + str(result.idhta_document))
            # sync the data
            for key, value in data.model_dump(exclude=[col], exclude_none=True).items():
                log.info('Overwriting ' + key + " with " + str(value))
                setattr(result, key, value)
            if col=='title':
                log.info('idhta: ' + str(result.idhta_document))
        # persist the data to the database
#        insert_stmt = insert(model.__table__).values(result.dict())
#        update_dict = {x.name: x for x in insert_stmt.inserted}
#        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(data={**update_dict}, status='U')
#        session.execute(on_duplicate_key_stmt)   

            insert_dict = result.model_dump()
            inserted = insert(model).values(**insert_dict)
            upserted = inserted.on_duplicate_key_update(
                **{k: inserted.inserted[k]
                                    for k, v in insert_dict.items()}) # idhta_document=func.LAST_INSERT_ID(model.idhta_document),
            session.execute(upserted)             
        #session.execute(model.__table__
        #        .insert()
        #        .prefix_with('IGNORE')
        #        .values(result.dict()))

        #session.add(result)
        session.commit()
        session.refresh(result)

        return result


    @staticmethod  
    def add_agency(session, agency):
        log.info('add agency')
        result = session.query(HTA_Agency).filter_by(name=agency.name).first()
        if result is None:
            result = agency
        else:
            log.info('Found agency in db')
        # sync the data
        for key, value in agency.model_dump(exclude_none=True).items():
            setattr(result, key, value)

        # persist the data to the database
        session.add(result)
        #session.commit()
        #session.refresh(result)

        return result


    @staticmethod  
    def add_agency(session, agency):
        log.info('add agency')
        result = session.query(HTA_Agency).filter_by(name=agency.name).first()
        if result is None:
            result = agency
        else:
            log.info('Found agency in db')
        # sync the data
        for key, value in agency.model_dump(exclude_none=True).items():
            setattr(result, key, value)

        # persist the data to the database
        session.add(result)
        session.commit()
        session.refresh(result)

        return result
    
    @staticmethod
    def add_company(session, company):
        log.info('add company')
        db_company = session.query(Company).filter_by(name=company.name).first()
        if db_company:
            log.info('Found company in db')
            company = db_company#Company(name=name)
        session.add(company)
            #session.commit()
        return company
    
    @staticmethod
    def add_product(session, product):
        log.info('add product')
        db_product = session.query(Product).filter_by(name=product.name).first()
        if db_product:
            log.info('Found product in db')
            product = db_product
            #product.prod_companies.append(ProductCompanyAssociation(company=company, role="agent"))
        session.add(product)
        
        return product

"""     @staticmethod
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
    """ 