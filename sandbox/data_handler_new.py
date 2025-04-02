
from os.path import join, dirname, abspath
import os, re
from sys import exc_info
import pandas as pd
from pangres import upsert
import sqlalchemy as sqlal
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool
import sqlalchemy.exc as sqlal_exc
from sqlalchemy.sql import text
from sqlalchemy import UniqueConstraint, event, inspect
from sqlalchemy import exc
from sqlalchemy.dialects.mysql import insert
from sandbox.sqlalchemy_models360 import Demographics
from sqlalchemy.orm import scoped_session, sessionmaker

from datetime import timedelta, date, datetime
import numpy as np
#import yaml
import time
import logging
log = logging.getLogger(__name__)
import secret.secrets as secrets

SSL_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/ssl/DigiCertGlobalRootCA.crt.pem'

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
import sqlalchemy.sql.expression as expr

class Upsert(Insert): pass

@compiles(Upsert, "mysql")
def compile_upsert(insert_stmt, compiler, **kwargs):
    print(insert_stmt)
    print('kwargs:')
    print(kwargs)
    #if insert_stmt._has_multi_parameters:
    #    keys = insert_stmt.parameters[0].keys()
    #else:
    keys = insert_stmt.parameters.keys()
    pk = insert_stmt.table.primary_key
    auto = None
    if (len(pk.columns) == 1 and
            isinstance(pk.columns.values()[0].type, sqlal.Integer) and
            pk.columns.values()[0].autoincrement):
        auto = pk.columns.keys()[0]
        if auto in keys:
            keys.remove(auto)
    insert = compiler.visit_insert(insert_stmt, **kwargs)
    ondup = 'ON DUPLICATE KEY UPDATE'
    updates = ', '.join(
        '%s = VALUES(%s)' % (c.name, c.name)
        for c in insert_stmt.table.columns
        if c.name in keys
    )
    if auto is not None:
        last_id = '%s = LAST_INSERT_ID(%s)' % (auto, auto)
        if updates:
            updates = ', '.join((last_id, updates))
        else:
            updates = last_id
    upsert = ' '.join((insert, ondup, updates))
    return upsert

def upser(session, model, rows):
    # Handles both fk and unique keys for upserts
    table = model.__table__
    stmt = insert(table)
    primary_keys = [key.name for key in inspect(table).primary_key]
    update_dict = {c.name: c for c in inspect(table).columns if not c.primary_key}

    if not update_dict:
        raise ValueError("insert_or_update resulted in an empty update_dict")

    stmt = stmt.on_duplicate_key_update(update_dict)

    seen = set()
    foreign_keys = {col.name: list(col.foreign_keys)[0].column for col in table.columns if col.foreign_keys}
    unique_constraints = [c for c in table.constraints if isinstance(c, UniqueConstraint)]
    def handle_foreignkeys_constraints(row):
        for c_name, c_value in foreign_keys.items():
            foreign_obj = row.pop(c_value.table.name, None)
            row[c_name] = getattr(foreign_obj, c_value.name) if foreign_obj else None

        for const in unique_constraints:
            unique = tuple([const,] + [row[col.name] for col in const.columns])
            if unique in seen:
                return None
            seen.add(unique)

        return row
    
    rows = list(filter(None, (handle_foreignkeys_constraints(row) for row in rows)))
    res = session.execute(stmt, rows)
    return res


@compiles(Insert)
def _use_replace_not_insert_ignore(insert, compiler, **kw): #_prefix_insert_with_ignore
#    s = compiler.visit_insert(insert, **kw)
#    s = s.replace("INSERT INTO", "REPLACE INTO")
#    return s

    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kw)

class DataHandlerProduction:
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

            ###print(SQLALCHEMY_DATABASE_URI)
            
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
                    self.dbschema = 'documents_db_5'#cfg['mysql']['db']

                    temp = r'mysql+pymysql://' + user + ':' + pw + '@' + host + ':' + str(port) + '/' + self.dbschema + "?charset=utf8mb4"
                    connect_text = "### MYSQL CONNECTION user=%s host=%s schema=%s" % (user, host, self.dbschema)

            ssl_args = {'ssl': { 'ca':SSL_PATH}}
            conn = temp
            # pool_pre_ping are used to make sure we have a connection before doing a query
            self.engine = sqlal.create_engine(conn, connect_args=ssl_args, echo=True)
        # See https://docs.sqlalchemy.org/en/14/core/pooling.html#pooling-multiprocessing
        #self.engine.dispose() 
        self.connection = self.engine.connect()
        self.session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

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

    def insert_agency(self, name):
        query = text('INSERT IGNORE INTO hta_agency (name) VALUES (:p1)')
        result = self.execute(query, {'p1': name})
        return result
    
    def insert_one_product(self, name):
        query = text('INSERT IGNORE INTO product (name) VALUES (:p1)')
        result = self.execute(query, {'p1': name})
        return result
    
    def insert_company(self, name):
        query = text('INSERT IGNORE INTO company (name) VALUES (:p1)')
        result = self.execute(query, {'p1': name})
        return result

    def insert_companies(self, df):
        #df = df.set_index(['id'])
        self.to_sql_update(df, 'company')

    def insert_indications(self, df):
        #df = df.set_index(['icd10_code'])
        self.to_sql_update(df, 'indication')

    def insert_trial(self, df):
        return self.to_sql_update(df, 'trial').lastrowid
    
    def insert_outcome_measure(self, df):
        return self.to_sql_update(df, 'outcome_measure')
    
    def insert_references(self, df):
        return self.to_sql_update(df, 'reference')
        
    def insert_costs(self, df):
        #df = df.set_index(['idanalysis','assessor','product'])
        res = self.to_sql_update(df, 'costs')
        if res:
            return res.lastrowid
        else:
            return -1

    def insert_analysis(self, df):
        #df = df.set_index(['idpico'])
        result = self.to_sql_update(df, 'analysis')
        return result 

    def insert_picos(self, df):
        log.info('inside insert pico')
        log.info(df.columns)
        log.info('Nr of rows ' + str(len(df.index)))
        log.info('ICD: ' + df['icd10_code'])
        
        #df_hta = pd.read_sql_table('hta_document', self.engine)
        #df_hta = df_hta[['id', 'diarie_nr']]
        #df_hta = df_hta.rename(columns={"id": "idhta_document"})
        
        df_indication = pd.read_sql_table('indication', self.engine)
        df_indication = df_indication.rename(columns={"id": "idindication"})
        df_indication = df_indication[['idindication', 'icd10_code']]
        
        df_product = pd.read_sql_table('product', self.engine)
        df_product = df_product.rename(columns={"name": "product", "id":"idproduct"})

        #df = df.merge(df_hta, on='diarie_nr', how='inner') # left?
        log.info('Nr of rows, 1 merge ' + str(len(df.index)))
        df = df.merge(df_indication, on='icd10_code', how='inner')
        log.info('Nr of rows, 2 merge ' + str(len(df.index)))
        df = df.merge(df_product, on='product', how='inner')
        log.info('Nr of rows, 3 merge ' + str(len(df.index)))
        df = df.drop_duplicates()
        #print('PICO')
        #print(df.columns)
        df = df.drop(columns=['product','icd10_code'])
        #df = df.set_index(['population','intervention','comparator_company','comparator_agency'])
        result = self.to_sql_update(df, 'pico')    

        return result    
    
    def insert_population(self, df):
        log.info('inside insert pop')
        log.info(df.columns)
        log.info('Nr of rows ' + str(len(df.index)))

       # df_indication = pd.read_sql_table('indication', self.engine)
       # df_indication = df_indication.rename(columns={"id": "idindication"})
       # df_indication = df_indication[['idindication', 'icd10_code']]

       # log.info('Nr of rows, 1 merge ' + str(len(df.index)))
       # df = df.merge(df_indication, on='icd10_code', how='inner')
       # df = df.drop_duplicates()
        #df = df.drop(columns=['icd10_code'])
        
        result = self.to_sql_update(df, 'population')    

        return result
        
        
    def insert_demographics(self, df):
        result = self.to_sql_update(df, 'demographics') 
        last_id = result.lastrowid
        if last_id==0:
            last_id = self.get_current_row(Demographics, df.squeeze().to_dict())
        return last_id

    def insert_active_drug(self, df):
        #df = df.set_index(['atc'])
        self.to_sql_update(df, 'active_drug')

    def insert_EMA(self, df):
        df['date_of_refusal'] = pd.to_datetime(df['date_of_refusal'], dayfirst=True, errors='ignore')
        df['marketing_authorisation_date'] = pd.to_datetime(df['marketing_authorisation_date'], dayfirst=True, errors='ignore')
        df['date_of_opinion'] = pd.to_datetime(df['date_of_opinion'], dayfirst=True, errors='ignore')
        df['decision_date'] = pd.to_datetime(df['decision_date'], dayfirst=True, errors='ignore')
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product",'id':'idproduct'})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        #df = df.set_index(['product_number'])
        self.to_sql_update(df, 'ema_status')

    def insert_nt_council_rec(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product",'id':'idproduct'})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        #df = df.set_index(['date', 'idproduct'])
        self.to_sql_update(df, 'NT_council_recommendation')

    def insert_nt_council_deal(self, df):
        df_companies = pd.read_sql_table('company',self.engine)#self.get_df('company')
        df_companies = df_companies.rename(columns={"name": "company", 'id':'idcompany'})
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product",'id':'idproduct'})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_companies, on='company', how='left')
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['company','product'])
        #df = df.set_index(['date', 'idproduct'])
        self.to_sql_update(df, 'nt_council_deal')

    def insert_nt_council_follow_up(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product",'id':'idproduct'})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        #df = df.set_index(['date', 'idproduct'])
        self.to_sql_update(df, 'nt_council_follow_up')

    def insert_form(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product",'id':'idproduct'})
        df_product['product'] = df_product['product'].str.lower()
        df['product'] = df['product'].str.lower()
        df = df.merge(df_product, on='product', how='left')
        df = df.drop(columns=['product'])
        df = df.drop_duplicates()
        #df = df.set_index(['NPL_id'])
        self.to_sql_update(df, 'form')

    def insert_price(self, df):
        df_companies = pd.read_sql_table('company',self.engine)
        df_companies['name'] = df_companies['name'].str.lower()
        df['name'] = df['name'].str.lower()
        df['ATC'] = df['ATC'].str.strip()
        df = df.merge(df_companies, on='name', how='left')
        #df_form = pd.read_sql_table('form',self.engine)
        #df_form = df_form[['idform','NPL_id']]
        ##print(df.dtypes)
        ##print(df_form.dtypes)
        #df['NPL_id'] = df['NPL_id'].astype('int64')
        df = df.replace({r'\,'}, {'.'}, regex=True) 

        #df_form['NPL_id'] = df_form['NPL_id'].astype('int64')
        #df = df.merge(df_form, on='NPL_id', how='left') 
        df = df.drop(columns=['name', 'product'])
        #df = df.dropna() # TODO. REMOVE. ADD missing forms separately
        #return df
        #df = df.set_index(['idcompany', 'NPL_id'])
        self.to_sql_update(df, 'price')

    def insert_product(self, df):
        #df = df.set_index(['name'])
        self.to_sql_update(df, 'product')

    def insert_regulatory_status(self, df):
        trans = dict({r'Ja$': 1, r'Nej$': 0})
        df = df.replace(regex=trans)
        df = df.drop_duplicates()
        df_product = pd.read_sql_table('product', self.engine)
        df = df.merge(df_product, on='name', how='inner') # OBS inner
        df = df.drop(columns=['name'])
        #df = df.set_index(['idproduct','country'])
        self.to_sql_update(df, 'regulatory_status')


    def insert_company_has_product(self, df):
        df_companies = pd.read_sql_table('company',self.engine)#self.get_df('company')
        df_companies = df_companies.rename(columns={"name": "company", "id": "idcompany"})
        df = df.merge(df_companies, on='company', how='left')
        
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})
        df = df.merge(df_product, on='product', how='left')
        
        df = df[['idcompany', 'idproduct','role']]
        self.to_sql_update(df, 'product_company')

    def insert_product_has_active_drug(self, df):
        df_active_drugs = pd.read_sql_table('active_drug',self.engine)
        df_active_drugs = df_active_drugs.rename(columns={"name": "active substance",'id':'idactive_drug'})  
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product",'id':'idproduct'})
        df = df.merge(df_active_drugs, on='ATC', how='left')
        df = df.merge(df_product, on='product', how='left')
        df = df[['idproduct','idactive_drug']]
        self.to_sql_update(df, 'product_has_active_drug')

    def insert_product_has_ATC(self, df):
        df = df.drop_duplicates()
        df_active_drugs = pd.read_sql_table('active_drug',self.engine)
        df_active_drugs = df_active_drugs.rename(columns={"name": "active substance"})  
        df_product = pd.read_sql_table('product',self.engine)
        
        df = df.merge(df_active_drugs, on='ATC', how='inner')
        #print(df.columns)
        #print(df_product.columns)
        df = df.merge(df_product, on='name', how='inner')
        #df = df[['idproduct','idactive_drug']]
        self.to_sql_update(df, 'product_has_active_drug')

    def insert_product_has_indication(self, df, source):
        df_indication = pd.read_sql_table('indication',self.engine)
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "drug_name",'id':'idproduct'})
        df = df.merge(df_indication, on='icd10_code', how='inner') # missing, should be left
        df = df.merge(df_product, on='drug_name', how='left')
        # #print(df.columns)
        df = df[['idproduct','idindication']]
        df['source'] = source
        df = df.drop_duplicates()
        #df = df.set_index(['idproduct','idindication','source'])
        res = self.to_sql_update(df, 'product_has_indication')
        if res:
            return res.lastrowid
        else:
            return -1

    def insert_reviewers(self, df, agency_name):
        idagency = self.get_idagency_with_name(agency_name)
        df['idHTA_agency'] = idagency
        df = df.rename(columns={"title":"profession"}) 
        #df = df.set_index(['profession','name'])
        res = self.to_sql_update(df, 'staff')
        #for row in res.inserted_primary_key_rows:
        #    log.inof('Added reviewer id')
        #    log.info(str(row[0]))
        if res:
            return res.lastrowid
        else:
            return -1

    def insert_experts(self, df):
        #df = df.set_index(['profession','name'])
        res = self.to_sql_update(df, 'expert')
        if res:
            return res.lastrowid
        else:
            return -1

    def insert_hta(self, df, agency):
        log.info('Inside db_handler.insert_hta')
        log.info('From db_handler 1. Index names')
        log.info(df.index.names)
        log.info('values before')
        log.info(df)
        idagency = self.get_idagency_with_name(agency)
        df['idHTA_agency'] = idagency
        log.info('From db_handler 2. Index names')
        log.info(df.index.names)
        df_companies = pd.read_sql_table('company',self.engine)
        df_companies = df_companies.rename(columns={"name": "company", "id": "idcompany"})
        #print(df.columns)
        df = df.reset_index()
        df_companies['company'] = df_companies['company'].str.lower()
        df['company'] = df['company'].str.lower()
        df = df.merge(df_companies, on='company', how='left')
        #df = df.set_index(['diarie_nr','date','document_type'])  
        df = df.drop(columns={'company'})
        #log.info('From db_handler 3. Index names')
        #log.info(df.index.names)
        #log.info('values')
        #log.info(df)
        #log.info('company')
        #log.info(df.idcompany)
        res = self.to_sql_update(df,'hta_document')
        if res:
            return res.lastrowid
        else:
            return -1
    
    def insert_hta_indication(self, df):
        log.info('Inside db_handler.insert_hta_indication')
        log.info('Valeus')
        log.info(df)
        #df_hta = pd.read_sql_table('hta_document',self.engine)
        #df_hta = df_hta.rename(columns={"id":"idhta_document"})
        df_indication = pd.read_sql_table('indication',self.engine)
        df_indication = df_indication.rename(columns={ "id":"idindication"})
        #df = df.merge(df_hta, on='diarie_nr', how='inner') # left?
        df2 = df.merge(df_indication, on='icd10_code', how='inner')
        df2 = df2.drop_duplicates()
        if df2.empty:
            log.error('Indication {} missing'.format(df['icd10_code'][0]))
        #print(df.columns)
        df2 = df2[['idindication','idhta_document','severity']]
        #df = df.set_index(['idindication','idhta_document','severity'])
        res = self.to_sql_update(df2, 'hta_document_indication')
        if res:
            return res.lastrowid
        else:
            return -1


    def insert_hta_has_product(self, df):
        #df_hta = pd.read_sql_table('hta_document',self.engine)
        #df_hta = df_hta.rename(columns={"id": "idhta_document"})
        
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "drug_name", "id": "idproduct"})
        
        #df = df.merge(df_hta, on='diarie_nr', how='inner') # left?
        df = df.merge(df_product, on='drug_name', how='inner')
        df = df.drop_duplicates()
        
        #print(df.columns)
        df = df[['idproduct','idhta_document']]
        #df = df.set_index(['idproduct','idhta_document'])
        res = self.to_sql_update(df, 'hta_document_product')
        if res:
            return res.lastrowid
        else:
            return -1

    def insert_hta_has_reviewer(self, df):
        #df_hta = pd.read_sql_table('hta_document',self.engine)
        #df_hta = df_hta.rename(columns={"id":"idhta_document"})
        df_reviewer = pd.read_sql_table('staff',self.engine)
        df_reviewer = df_reviewer.rename(columns={"id": "idstaff"})
        
        #df = df.merge(df_hta, on='diarie_nr', how='left')
        df = df.merge(df_reviewer, on='name', how='left')

        #print(df.columns)
        df = df[['idstaff','idhta_document','role','dissent']].dropna().drop_duplicates().reindex() #shouldn\t have to
        #df = df.set_index(['idstaff','idhta_document','role'])
        res = self.to_sql_update(df, 'hta_document_staff')
        if res:
            return res.lastrowid
        else:
            return -1


    def insert_hta_has_experts(self, df):
        df_expert = pd.read_sql_table('expert',self.engine)
        df_expert = df_expert.rename(columns={"id": "idexpert"})

        df = df.merge(df_expert, on=['first_name', 'last_name', 'position'])
        log.info('After merge of experts')
        log.info(df)
        #print(df.columns)
        df = df[['idexpert','idhta_document']]
        #df = df.set_index(['idstaff','idhta_document','role'])
        res = self.to_sql_update(df, 'hta_document_has_expert')
        if res:
            return res.lastrowid
        else:
            return -1

    def insert_hta_references(self, df):
        df_ref = pd.read_sql_table('reference',self.engine)
        df_ref = df_ref.rename(columns={"id": "idreference"})
        df_ref = df_ref[['idreference', 'title']]
        df = df.merge(df_ref, on=['title'])
        log.info('After merge of refs')
        log.info(df)
        #print(df.columns)
        df = df[['idreference','idhta_document']]
        #df = df.set_index(['idstaff','idhta_document','role'])
        res = self.to_sql_update(df, 'hta_document_reference')
        if res:
            return res.lastrowid
        else:
            return -1


    def get_hta_with_diarie_nr_and_document_type(self, diarie_nr, doc_type):
        query = text('SELECT id from hta_document WHERE diarie_nr=:p1 AND document_type=:p2')
        result = self.query_database(query, par={'p1': diarie_nr, 'p2': doc_type})
        result = result.fetchone()
        if result is not None:
            return result[0]
        else:
            return None
        
    def get_hta_with_diarie_nr(self, diarie_nr):
        query = text('SELECT id from hta_document WHERE diarie_nr=:p1')
        result = self.query_database(query, par={'p1': diarie_nr})
        result = result.fetchone()
        if result is not None:
            return result[0]
        else:
            return None

    def get_idagency_with_name(self, name):
        query = text("select id from hta_agency where name = :p1")
        result = self.engine.connect().execute(query, {'p1': name})
        result = result.fetchone()
        if result is not None:
            idingredient = result[0]
            return idingredient
        else:
            return None

    def get_df(self, table_name):
        query = text("""SELECT * FROM %s""" %(table_name))
        
        df = self.read_sql_query(query)
        return df
    
    def get_current_row(self, model, data):
        unique_keys = inspect(model).class_._upsert_index_elements
        query = 'SELECT id FROM %s WHERE ' %(model.__tablename__)
        query = query + (' and ').join([x + '= :' + x  for i,x in enumerate(unique_keys)])
        query = text(query)
        result = self.engine.connect().execute(query, {x:data[x] for x in unique_keys}) #dict(zip(list(unique_keys), data)))

        return result.fetchone()[0]
        
   ### START GENERIC METHODS --------------------------------------------------------------------
    @staticmethod
    def encode_float64(val):
        #Static method to handle SQL encoding of numpy.float64
        if val is None or np.isnan(val):
            val = None
        else:
            val = ('%.15g' % val)
        return val

    @staticmethod
    def encode_int64(val):
        #Static method to handle SQL encoding of numpy.int64
        if val is None or np.isnan(val):
            val = None
        else:
            val = int(val)
        return val

    def get_count_of_table(self, table_name):
        query = text('select count(*) from %s' % table_name)
        result = self.engine.execute(query)
        count = result.fetchone()[0]
        return count

    def get_first_available_id(self, table_name, col_name):
        query = text("select MAX(%s) from %s" % (col_name, table_name))
        result = self.engine.execute(query)
        max = result.fetchone()[0]
        if max is None:
            return 1
        else:
            return max + 1

    def convert_sql_result_to_df(self, result):
        df = pd.DataFrame(result.fetchall())
        df.columns = result.keys()
        return df

    def execute(self, query, par={}):
        with self.engine.connect() as connection:
            with connection.begin():
                result = connection.execute(query, par)
        return result
        
    def to_sql(self, df, table_name):
        try:
            upsert(
                con=self.engine,
                df=df,
                table_name=table_name,
                if_row_exists='update',
                dtype=None,  # same logic as the parameter in pandas.to_sql
                chunksize=1000,
                create_table=False  # create a new table if it does not exist
            )
            #df.to_sql(table_name, self.engine, if_exists='append', index=False)
        except BaseException as e:
            log.exception('insert to table ' + table_name + ' failed', exc_info=True)

    def query_database(self, query, retries=0, par={}):
        while True:
            try:
                result = self.execute(query, par)
                return result
            except Exception as e: #sqlal.exc.InternalError as e:               
                if retries > 0:
                    time.sleep(2)
                    retries = retries - 1
                    self.engine.connect()
                else:
                    log.exception(e)
                    raise e
   
    def to_sql_w_result(self, df, table):
        """
        Applies the SQL insert into 'table' on duplicate key update __ on a Dataframe.
        Does not generate any warning or errors if duplicated row is found. Instead it will just
        apply a update that does nothing.
        - This method will prevent duplicated records ONLY if a normal sql insert gives ERROR. One
        way to make sure that insert gives error is to add a composite key to the db.table, so that
        we cannot insert duplicated records.
        """
        result = None
        try:
            joined_columns = ', '.join(df.columns)
            MAX_BULK_ROWS = 1000  # Value is limit by MAX_ALLOWED_PACKET in the mysql server
            counter = 0
            temp_value = ''

            for indx, row in df.iterrows():
                counter += 1
                values = row.values
                astring = []
                for val in values:
                    if isinstance(val, str):
                        astring.append('\'%s\'' % val)
                    elif isinstance(val, datetime):
                        astring.append('\'%s\'' % val)
                    elif (val is None) or np.isnan(val):
                        astring.append('null')
                    else:
                        astring.append(str(val))

                df_values = ','.join(astring)
                temp_value += '(' + df_values + '),'
                if (counter % MAX_BULK_ROWS == 0 or counter == len(df)):
                    query = 'insert into %s (%s) values %s ON DUPLICATE KEY UPDATE %s=%s' % (
                    table, joined_columns, temp_value[:-1], df.columns[0], df.columns[0]) + ' id=LAST_INSERT_ID(id)'
                    query = text(query)
                    result = self.query_database(query, retries=5)
                    temp_value = ''
                    
        except BaseException as e:
            log.exception('insert to table ' + table + ' failed', exc_info=True)
        return result
    
    @staticmethod
    def convert_entry(val):
        bool_to_tiny_int = {False: '0', True: '1'}
        if isinstance(val, str):
            val = val.replace("\'","\\'") # For some reason single quota is fucked
            return ('\'%s\'' % val)
        elif isinstance(val, datetime):
            return ('\'%s\'' % val)
        elif isinstance(val, bool):
            return (bool_to_tiny_int[val])
        elif (val is None) or np.isnan(val):
            return ('null')
        else:
            return (str(val))
    
    def to_sql_update(self, df, table):
        """
        Applies the SQL insert into 'table' on duplicate key update on a Dataframe.
        """
        
        result = None
        try:
            # get the unique keys
            query = 'SHOW INDEX FROM ' + table
            df_index = self.read_sql_query(query)
            unique = set(df_index.loc[df_index['Key_name'].str.contains('^unique')]['Column_name'])
            unique.add('id') 
            #query = 'SHOW COLUMNS FROM ' + table
            #df_columns = self.read_sql_query(query)
            # Let's get rid of null values (may resolve 'a value is required for bind parameter 1430')
            df = df.replace(to_replace='', value=np.nan).dropna(axis=1, how='all') # TODO FIX THISSSSSS!!!!!! also we dont want to drop all rows!!
            null_columns = [c for c in df.columns if pd.isnull(c)]
            #df = df.dropna(axis=1, how='all')
            all_columns = set(df.columns) #set(df_columns['Field'])
            non_unique = all_columns.difference(unique)
            non_unique = list(non_unique.difference(null_columns))
            joined_columns = ', '.join(df.columns)
            MAX_BULK_ROWS = 1000  # Value is limit by MAX_ALLOWED_PACKET in the mysql server
            counter = 0
            temp_value = ''

            for indx, row in df.iterrows():
                update_str = ','.join([idx + '=' + self.convert_entry(row[idx]) for idx in non_unique])
                counter += 1
                values = row.values
                astring = []
                for val in values:
                    astring.append(self.convert_entry(val))

                df_values = ','.join(astring)
                temp_value += '(' + df_values + '),'
                #if (counter % MAX_BULK_ROWS == 0 or counter == len(df)):
                # Update. if duplicate key then update the non-unique columns if there are
                if len(non_unique)>0:  
                    query = 'insert into %s (%s) values %s ON DUPLICATE KEY UPDATE %s' % (table, joined_columns, temp_value[:-1], update_str)
                else:
                    query = 'INSERT IGNORE INTO %s (%s) values %s' % (table, joined_columns, temp_value[:-1]) 
                query = text(query)
                #log.info('qurty')
                #log.info(query)
                result = self.query_database(query, retries=5) 
                temp_value = ''
                    
        except BaseException as e:
            log.exception('insert to table ' + table + ' failed', exc_info=True)
        
        return result


    def to_sql_insert_ignore(self, df, table):
        """
        Applies the SQL insert into 'table' on duplicate key update __ on a Dataframe.
        Does not generate any warning or errors if duplicated row is found. Instead it will just
        apply a update that does nothing.
        - This method will prevent duplicated records ONLY if a normal sql insert gives ERROR. One
        way to make sure that insert gives error is to add a composite key to the db.table, so that
        we cannot insert duplicated records.
        """
        try:
            joined_columns = ', '.join(df.columns)
            MAX_BULK_ROWS = 1000  # Value is limit by MAX_ALLOWED_PACKET in the mysql server
            counter = 0
            temp_value = ''

            for indx, row in df.iterrows():
                counter += 1
                values = row.values
                astring = []
                for val in values:
                    if isinstance(val, str):
                        astring.append('\'%s\'' % val)
                    elif isinstance(val, pd.datetime):
                        astring.append('\'%s\'' % val)
                    elif (val is None) or np.isnan(val):
                        astring.append('null')
                    else:
                        astring.append(str(val))

                df_values = ','.join(astring)
                temp_value += '(' + df_values + '),'
                if (counter % MAX_BULK_ROWS == 0 or counter == len(df)): # ON DUPLICATE KEY UPDATE %s=%s # , df.columns[0], df.columns[0]
                    query = 'insert into %s (%s) values %s ' % (
                    table, joined_columns, temp_value[:-1])
                    query = text(query)
                    self.query_database(query, retries=5)
                    temp_value = ''
        except BaseException as e:
            log.exception('insert to table ' + table + ' failed', exc_info=True)

    def to_sql_with_cond(self, df, table, cond=''):

        joined_columns = ', '.join(df.columns)
        MAX_BULK_ROWS = 5000  # Value is limit by MAX_ALLOWED_PACKET in the mysql server
        counter = 0
        temp_value = ''

        for indx, row in df.iterrows():
            counter += 1
            astring = [str(v) for v in row.values]
            df_values = ','.join(astring)
            temp_value += '(' + df_values + '),'
            if (counter % MAX_BULK_ROWS == 0 or counter == len(df)):
                query = 'insert into %s (%s) values %s %s ' % (
                table, joined_columns, temp_value[:-1], cond)
                query = text(query)
                self.query_database(query, retries=2)
                temp_value = ''


    def read_sql_query(self, query):
        """Read SQL query into a DataFrame. This function is used instead of dataframe.read_sql()
        since this will make testing easier."""

        result = self.engine.connect().execute(text(query))
        if query.split(' ')[0].upper() not in ['DELETE']:
            labels = result.keys()
            row = result.fetchall()
            dataframe = pd.DataFrame.from_records(row, columns=labels)
            none_cols = dataframe.isna().all()
            dataframe.loc[:, none_cols] = dataframe.loc[:, none_cols].fillna(np.nan)
            return dataframe 