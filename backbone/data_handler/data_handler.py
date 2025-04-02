
from os.path import join, dirname, abspath
import os
from sys import exc_info
import pandas as pd
import sqlalchemy as sqlal
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool
import sqlalchemy.exc as sqlal_exc
from sqlalchemy.sql import text
from sqlalchemy import event
from sqlalchemy import exc
from datetime import timedelta, date, datetime
import numpy as np
#import yaml
import time
from logger_tt import getLogger, setup_logging
from secret import secrets

log = getLogger(__name__)

SSL_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/ssl/DigiCertGlobalRootCA.crt.pem'

SCHEMA = 'stage'#documents_db_5'

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
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

            #print(SQLALCHEMY_DATABASE_URI)
            
            if SQLALCHEMY_DATABASE_URI: # Connect using env_var if it's initialized.
                temp = SQLALCHEMY_DATABASE_URI
                connect_text = "MYSQL CONNECTION using env key: SQLALCHEMY_DATABASE_URI"
            else:
                host = 'smartstepnordics-mysql8.mysql.database.azure.com'#cfg['mysql']['host']
                port = '3306'#cfg['mysql']['port']
                user = 'smartstepnordics_adm'#cfg['mysql']['user']
                pw = secrets.mysql_pwd#cfg['mysql']['password']
                self.dbschema = SCHEMA#cfg['mysql']['db']

                temp = r'mysql+pymysql://' + user + ':' + pw + '@' + host + ':' + str(port) + '/' + self.dbschema + "?charset=utf8mb4"
                connect_text = "### MYSQL CONNECTION user=%s host=%s schema=%s" % (user, host, self.dbschema)

            ssl_args = {'ssl': { 'ca':SSL_PATH}}
            conn = temp
            # pool_pre_ping are used to make sure we have a connection before doing a query
            self.engine = sqlal.create_engine(conn, connect_args=ssl_args, echo=True)
        # See https://docs.sqlalchemy.org/en/14/core/pooling.html#pooling-multiprocessing
        #self.engine.dispose() 
        self.connection = self.engine.connect()
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
        query = text('INSERT INTO hta_agency (name) VALUES (:p1)')
        result = self.execute(query, {'p1': name})
        return result

    def insert_companies(self, df):
        
        self.to_sql(df, 'company')

    def insert_indications(self, df):
        self.to_sql(df, 'indication')
    
    def insert_active_drug(self, df):
        self.to_sql(df, 'active_drug')

    def insert_EMA(self, df):
        df['date_of_refusal'] = pd.to_datetime(df['date_of_refusal'], dayfirst=True, errors='ignore')
        df['marketing_authorisation_date'] = pd.to_datetime(df['marketing_authorisation_date'], dayfirst=True, errors='ignore')
        df['date_of_opinion'] = pd.to_datetime(df['date_of_opinion'], dayfirst=True, errors='ignore')
        df['decision_date'] = pd.to_datetime(df['decision_date'], dayfirst=True, errors='ignore')
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        self.to_sql(df, 'ema_status')

    def insert_nt_council_rec(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        self.to_sql(df, 'nt_council_recommendation')

    def insert_nt_council_no_ass(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='left') # OBS inner
        df = df.drop(columns=['product'])
        self.to_sql(df, 'nt_council_no_assessment')

    def insert_nt_council_deal(self, df):
        df_companies = pd.read_sql_table('company',self.engine)#self.get_df('company')
        df_companies = df_companies.rename(columns={"name": "company", "id": "idproduct"})
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idcompany"})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_companies, on='company', how='left')
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['company','product'])
        self.to_sql(df, 'nt_council_deal')

    def insert_nt_council_follow_up(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        self.to_sql(df, 'nt_council_follow_up')

    def insert_form(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})
        df_product['product'] = df_product['product'].str.lower()
        df['product'] = df['product'].str.lower()
        df = df.merge(df_product, on='product', how='left')
        df = df.drop(columns=['product'])
        df = df.drop_duplicates()
        self.to_sql(df, 'form')

    def insert_price(self, df):
        df_companies = pd.read_sql_table('company',self.engine)
        df_companies = df_companies.rename(columns={"id": "idcompany"})
        df_companies['name'] = df_companies['name'].str.lower()
        df['name'] = df['name'].str.lower()
        df['ATC'] = df['ATC'].str.strip()
        df = df.merge(df_companies, on='name', how='left')
        #df_form = pd.read_sql_table('form',self.engine)
        #df_form = df_form[['idform','NPL_id']]
        #print(df.dtypes)
        #print(df_form.dtypes)
        #df['NPL_id'] = df['NPL_id'].astype('int64')
        df = df.replace({r'\,'}, {'.'}, regex=True) 

        #df_form['NPL_id'] = df_form['NPL_id'].astype('int64')
        #df = df.merge(df_form, on='NPL_id', how='left') 
        df = df.drop(columns=['name', 'product'])
        #df = df.dropna() # TODO. REMOVE. ADD missing forms separately
        #return df
        self.to_sql(df, 'price')

    def insert_product(self, df):

        self.to_sql(df, 'product')

    def insert_atmp_status(self, df):
        trans = dict({r'Yes$': 1, r'No$': 0})
        df = df.replace(regex=trans)
        df_new_products = df.loc[:, ['product']].drop_duplicates()
        df_new_products = df_new_products.rename(columns={'product':'name'})
        self.insert_product(df_new_products)
        df['authorisation_date'] = pd.to_datetime(df['authorisation_date'])
        df['withdrawal_date'] = pd.to_datetime(df['withdrawal_date'])
        df_product = pd.read_sql_table('product', self.engine)

        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})
        df = df.merge(df_product, on='product', how='inner') # TODO shouild be left
        df = df.drop(columns='product')
        
        #self.to_sql(df, 'ema_atmp_status')
        df.to_sql('ema_atmp_status', self.engine, if_exists='append', index=False)

    def insert_regulatory_status(self, df):
        trans = dict({r'Ja$': 1, r'Nej$': 0})
        df = df.replace(regex=trans)
        df = df.drop_duplicates()
        df_product = pd.read_sql_table('product', self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        self.to_sql(df, 'regulatory_status')


    def insert_company_has_product(self, df):
        df_companies = pd.read_sql_table('company',self.engine)#self.get_df('company')
        df_companies = df_companies.rename(columns={"name": "company", "id": "idcompany"})
        df = df.merge(df_companies, on='company', how='left')
        
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})
        df = df.merge(df_product, on='product', how='left')
        
        df = df[['idcompany', 'idproduct','role']]
        self.to_sql(df, 'product_company')

    def insert_product_has_active_drug(self, df):
        log.info('INNE!!!!!')
        df_active_drugs = pd.read_sql_table('active_drug',self.engine)
        df_active_drugs = df_active_drugs.rename(columns={"name": "active substance",'id':'idactive_drug'})  
        print('nr of rows in active: ' + str(len(df_active_drugs.index)))
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product", "id": "idproduct"})
        print('nr of rows in products: ' + str(len(df_product.index)))
        df = df.merge(df_active_drugs, on='ATC', how='left')
        df = df.merge(df_product, on='product', how='left')
        df = df[['idproduct','idactive_drug']]
        print('nr of rows in combo: ' + str(len(df.index)))
        self.to_sql(df, 'product_has_active_drug')

    def insert_product_has_ATC(self, df):
        df = df.drop_duplicates()
        df_active_drugs = pd.read_sql_table('active_drug', self.engine)
        df_active_drugs = df_active_drugs.rename(columns={"name": "active substance", 'id':'idactive_drug'})  
        df_product = pd.read_sql_table('product', self.engine)
        df_product = df_product.rename(columns={"id": "idproduct"})
        df = df.merge(df_active_drugs, on='ATC', how='inner')
        print(df.columns)
        print(df_product.columns)
        df = df.merge(df_product, on='name', how='inner')
        df = df[['idproduct','idactive_drug']]
        self.to_sql(df, 'product_has_active_drug')

    def insert_product_has_indication(self, df, source):
        log.info('Length of ind prod, init: ' + str(len(df.index)))
        log.info('Inserting product to indication')
        df_indication = pd.read_sql_table('indication',self.engine)
        df_indication = df_indication.rename(columns={'id':'idindication'}) 
        log.info('Length of ind: ' + str(len(df_indication.index)))
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "drug_name", "id": "idproduct"})
        log.info('Length of prod: ' + str(len(df_product.index)))
        log.info(df.columns)
        log.info(df_indication.columns)
        df = df.merge(df_indication, left_on='ICD', right_on='icd10_code', how='inner') # missing, should be left
        log.info('Length of ind prod, first: ' + str(len(df.index)))
        df = df.merge(df_product, on='drug_name', how='left')
        log.info('Length of ind prod, second: ' + str(len(df.index)))
        # print(df.columns)
        df = df[['idproduct','idindication']]
        df['source'] = source
        df = df.drop_duplicates()
        self.to_sql(df, 'product_has_indication')

    def insert_reviewers(self, df, agency_name):
        idagency = self.get_idagency_with_name(agency_name)
        df['idHTA_agency'] = idagency
        self.to_sql(df, 'staff')


    def insert_hta(self, df, agency):

        idagency = self.get_idagency_with_name(agency)
        df['idHTA_agency'] = idagency
        df_companies = pd.read_sql_table('company',self.engine)
        df_companies = df_companies.rename(columns={"name": "company", "id": "idproduct"})
        df = df.merge(df_companies, on='company', how='left')
        df = df.drop(columns={'company'})
        self.to_sql(df,'HTA_document')
    
    def insert_hta_has_indication(self, df):
        df_hta = pd.read_sql_table('hta_document',self.engine)
        df_indication = pd.read_sql_table('indication',self.engine)
        df_indication = df_indication.rename(columns={"icd10_code": "ICD", 'id':'idindication'})
        df = df.merge(df_hta, on='diarie_nr', how='inner') # left?
        df = df.merge(df_indication, on='ICD', how='inner')
        df = df.drop_duplicates()
        print(df.columns)
        df = df[['idindication','idHTA_document']]
        self.to_sql(df, 'hta_document_indication')


    def insert_hta_has_product(self, df):
        df_hta = pd.read_sql_table('hta_document',self.engine)
        df_hta = df_hta.rename(columns={"id": 'idHTA_document'})
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "drug_name", "id": "idproduct"})
        df = df.merge(df_hta, on='diarie_nr', how='inner') # left?
        df = df.merge(df_product, on='drug_name', how='inner')
        df = df.drop_duplicates()
        print(df.columns)
        df = df[['idproduct','idHTA_document']]
        self.to_sql(df, 'hta_document_product')

    def insert_hta_has_reviewer(self, df):
        df_hta = pd.read_sql_table('hta_document',self.engine)
        df_hta = df_hta.rename(columns={"id": 'idHTA_document'})
        df_reviewer = pd.read_sql_table('staff',self.engine)
        df_reviewer = df_reviewer.rename(columns={"id": 'idstaff'})
        df = df.merge(df_hta, on='diarie_nr', how='left')
        df = df.merge(df_reviewer, on='name', how='left')
        print(df.columns)
        df = df[['idstaff','idHTA_document']].dropna().drop_duplicates().reindex() #shouldn\t have to
        self.to_sql(df, 'hta_document_staff')

    def get_idagency_with_name(self, name):
        query = text("select idHTA_agency from hta_agency where name = :p1")
        result = self.engine.connect().execute(query, {'p1': name})
        result = result.fetchone()
        if result is not None:
            idingredient = result[0]
            return idingredient
        else:
            return None
        
    def get_products_no_man(self):
        df = pd.read_sql_table('product_company',self.engine)
        df_companies = pd.read_sql_table('company',self.engine)#self.get_df('company')
        df_companies = df_companies.rename(columns={"name": "company","id":"idcompany"})
        df = df.merge(df_companies, on='idcompany', how='left')
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product","id":"idproduct"})
        df = df.merge(df_product, on='idproduct', how='left')
        df = df[['company', 'product','role']]
        products_with_man = set(df.loc[df['role']=='manufacturer', 'product'].drop_duplicates())
        unique_products = set(df_product['product'].drop_duplicates())
        drugs_no_manufacturer = unique_products.difference(products_with_man)
        return drugs_no_manufacturer


    def get_df(self, table_name):
        query = """SELECT * FROM %s""" %(table_name)
        
        df = self.read_sql_query(query)
        return df

    
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
        query = 'select count(*) from %s' % table_name
        result = self.engine.execute(query)
        count = result.fetchone()[0]
        return count

    def get_first_available_id(self, table_name, col_name):
        query = "select MAX(%s) from %s" % (col_name, table_name)
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

    def execute(self, query, par):
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(query, par)

    def to_sql(self, df, table_name):
        try:
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
        except BaseException as e:
            log.exception('insert to table ' + table_name + ' failed', exc_info=True)
            

    def query_database(self, query, retries=0):
        while True:
            try:
                self.engine.execute(query)
                break
            except sqlal.exc.InternalError as e:               
                if retries > 0:
                    time.sleep(2)
                    retries = retries - 1
                    self.engine.connect()
                else:
                    raise e
   
