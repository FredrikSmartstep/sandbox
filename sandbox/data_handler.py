
from os.path import join, dirname, abspath
import os
import re
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
import logging
log = logging.getLogger(__name__)
from secret import secrets
SSL_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/ssl/DigiCertGlobalRootCA.crt.pem'

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
                with open(join(dirname(abspath(__file__)), mysql_config), 'r') as ymlfile:
                    #cfg = yaml.safe_load(ymlfile)
                    #cfg = yaml.load(ymlfile)
                    host = 'smartstepnordics-mysql8.mysql.database.azure.com'#cfg['mysql']['host']
                    port = '3306'#cfg['mysql']['port']
                    user = 'smartstepnordics_adm'#cfg['mysql']['user']
                    pw = secrets.mysql_pwd
                    self.dbschema = 'stage'#cfg['mysql']['db'] documents_db

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

    def insert_trial(self, df):
        self.to_sql(df, 'trial')

    def insert_analysis(self, df):
        result = self.to_sql_w_result(df, 'analysis')
        return result 

    def insert_picos(self, df):
        df_hta = pd.read_sql_table('hta_document', self.engine)
        df_hta = df_hta[['id', 'diarie_nr']]
        df_hta = df_hta.rename(columns={"id": "idhta_document"})
        
        df_indication = pd.read_sql_table('indication', self.engine)
        df_indication = df_indication.rename(columns={"icd10_code": "ICD"})
        
        df_product = pd.read_sql_table('product', self.engine)
        df_product = df_product.rename(columns={"name": "product", "id":"idproduct"})

        df = df.merge(df_hta, on='diarie_nr', how='inner') # left?
        # TODO
        #df = df.merge(df_indication, on='ICD', how='inner')
        df = df.merge(df_product, on='product', how='inner')
        
        df = df.drop_duplicates()
        print(df.columns)
        df = df.drop(columns=['diarie_nr','product'])

        result = self.to_sql_w_result(df, 'pico')    

        return result    
        
    
    def insert_active_drug(self, df):
        self.to_sql(df, 'active_drug')

    def insert_EMA(self, df):
        df['date_of_refusal'] = pd.to_datetime(df['date_of_refusal'], dayfirst=True, errors='ignore')
        df['marketing_authorisation_date'] = pd.to_datetime(df['marketing_authorisation_date'], dayfirst=True, errors='ignore')
        df['date_of_opinion'] = pd.to_datetime(df['date_of_opinion'], dayfirst=True, errors='ignore')
        df['decision_date'] = pd.to_datetime(df['decision_date'], dayfirst=True, errors='ignore')
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product"})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        self.to_sql(df, 'ema_status')

    def insert_nt_council_rec(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product"})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        self.to_sql(df, 'NT_council_recommendation')

    def insert_nt_council_deal(self, df):
        df_companies = pd.read_sql_table('company',self.engine)#self.get_df('company')
        df_companies = df_companies.rename(columns={"name": "company"})
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product"})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_companies, on='company', how='left')
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['company','product'])
        self.to_sql(df, 'nt_council_deal')

    def insert_nt_council_follow_up(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product"})#.drop(columns={'ATC','idcompany','idactive_drug'})
        df = df.merge(df_product, on='product', how='inner') # OBS inner
        df = df.drop(columns=['product'])
        self.to_sql(df, 'nt_council_follow_up')

    def insert_form(self, df):
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product","id":"idproduct"})
        df_product['product'] = df_product['product'].str.lower()
        df['product'] = df['product'].str.lower()
        df = df.merge(df_product, on='product', how='left')
        df = df.drop(columns=['product'])
        df = df.drop_duplicates()
        self.to_sql(df, 'form')

    def insert_price(self, df):
        df_companies = pd.read_sql_table('company',self.engine)
        df_companies['name'] = df_companies['name'].str.lower()
        df_companies = df_companies.rename(columns={"id":"idcompany"})
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

    def insert_regulatory_status(self, df):
        trans = dict({r'Ja$': 1, r'Nej$': 0})
        df = df.replace(regex=trans)
        df = df.drop_duplicates()
        df_product = pd.read_sql_table('product', self.engine)
        df = df.merge(df_product, on='name', how='inner') # OBS inner
        df = df.drop(columns=['name'])
        self.to_sql(df, 'regulatory_status')


    def insert_company_has_product(self, df):
        df_companies = pd.read_sql_table('company',self.engine)#self.get_df('company')
        df_companies = df_companies.rename(columns={"name": "company",'id':'idcompany'})
        df = df.merge(df_companies, on='company', how='left')
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product",'id':'idproduct'})
        df = df.merge(df_product, on='product', how='left')
        df = df[['idcompany', 'idproduct','role']]
        self.to_sql(df, 'product_company')

    def insert_product_has_active_drug(self, df):
        df_active_drugs = pd.read_sql_table('active_drug',self.engine)
        df_active_drugs = df_active_drugs.rename(columns={"name": "active substance"})  
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "product"})
        df = df.merge(df_active_drugs, on='ATC', how='left')
        df = df.merge(df_product, on='product', how='left')
        df = df[['idproduct','idactive_drug']]
        self.to_sql(df, 'product_has_active_drug')

    def insert_product_has_ATC(self, df):
        df = df.drop_duplicates()
        df_active_drugs = pd.read_sql_table('active_drug',self.engine)
        df_active_drugs = df_active_drugs.rename(columns={"name": "active substance"})  
        df_product = pd.read_sql_table('product',self.engine)
        
        df = df.merge(df_active_drugs, on='ATC', how='inner')
        print(df.columns)
        print(df_product.columns)
        df = df.merge(df_product, on='name', how='inner')
        df = df[['idproduct','idactive_drug']]
        self.to_sql(df, 'product_has_active_drug')

    def insert_product_has_indication(self, df, source):
        df_indication = pd.read_sql_table('indication',self.engine)
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "drug_name"})
        df = df.merge(df_indication, left_on='ICD', right_on='icd10_code', how='inner') # missing, should be left
        df = df.merge(df_product, on='drug_name', how='left')
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
        print('Nr of rows ' + str(len(df.index)))
        idagency = self.get_idagency_with_name(agency)
        df['idHTA_agency'] = idagency
        df_companies = pd.read_sql_table('company',self.engine)
        df_companies = df_companies.rename(columns={"name": "company", 'id': 'idcompany'})
        df = df.merge(df_companies, on='company', how='left')
        print('Nr of rows, 1 merge ' + str(len(df.index)))
        df = df.drop(columns={'company'})
        self.to_sql(df,'hta_document')
    
    def insert_hta_has_indication(self, df):
        df['ICD'] =  df['ICD'].apply(lambda row: re.sub(r'(?<=\.\d)\d','', row))
    
        df_hta = pd.read_sql_table('hta_document',self.engine)
        df_indication = pd.read_sql_table('indication',self.engine)
        df_indication = df_indication.rename(columns={"icd10_code": "ICD"})
        df = df.merge(df_hta, on='diarie_nr', how='inner') # left?
        print('Nr of rows, 2 merge ' + str(len(df.index)))
        df = df.merge(df_indication, on='ICD', how='inner')
        print('Nr of rows, 3 merge ' + str(len(df.index)))
        df = df.drop_duplicates()
        print('Nr of rows, 4 merge ' + str(len(df.index)))
        print(df.columns)
        df = df[['idindication','idHTA_document']]
        self.to_sql(df, 'hta_document_indication')


    def insert_hta_has_product(self, df):
        df_hta = pd.read_sql_table('hta_document',self.engine)
        df_product = pd.read_sql_table('product',self.engine)
        df_product = df_product.rename(columns={"name": "drug_name"})
        df = df.merge(df_hta, on='diarie_nr', how='inner') # left?
        df = df.merge(df_product, on='drug_name', how='inner')
        df = df.drop_duplicates()
        print(df.columns)
        df = df[['idproduct','idHTA_document']]
        self.to_sql(df, 'hta_document_product')

    def insert_hta_has_reviewer(self, df):
        df_hta = pd.read_sql_table('hta_document',self.engine)
        df_reviewer = pd.read_sql_table('staff',self.engine)
        df = df.merge(df_hta, on='diarie_nr', how='left')
        df = df.merge(df_reviewer, on='name', how='left')
        print(df.columns)
        df = df[['idstaff','idHTA_document']].dropna().drop_duplicates().reindex() #shouldn\t have to
        self.to_sql(df, 'hta_document_staff')

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
        query = """SELECT * FROM %s""" %(table_name)
        
        df = self.read_sql_query(query)
        return df

    def get_patient(self, idpatient):
        query = "SELECT * FROM patient WHERE idpatient = '%s'" %(idpatient)
        df = self.read_sql_query(query)
        return df
       
    def get_patients_from_clinic(self, idclinic):
        result = self.engine.execute('SELECT idpatient FROM clinic_listing WHERE idclinic = %s' %idclinic).fetchall()
        li = [str(e[0]) for e in result]
        df = pd.DataFrame()
        if len(li):
            patient_list = (',').join(li)
            query = text("""SELECT * FROM patient WHERE idpatient IN (%s)""" %(patient_list))
            df = pd.read_sql_query(query,self.connection)
        return df
    
    def insert_patient_to_specific_clinic(self, last_name, first_name, social_security_nr, date_of_register, idclinic):
            # check if there already exists a patient with this name at this clinic
            # get list of patients for this clinic
            result = self.engine.execute('SELECT idpatient FROM clinic_listing WHERE idclinic = %s' %idclinic).fetchall()
            li = [str(e[0]) for e in result]
            df = pd.DataFrame()
            if len(li):
                patient_list = (',').join(li)
                query = text("""SELECT * FROM patient WHERE last_name = "%s"
                        AND first_name = "%s" AND idpatient IN (%s)""" %(last_name, first_name, patient_list))
                df = pd.read_sql_query(query,self.connection)
            # if match found
            if len(df):
                log.info('Matching names')
                # Check if social_security number matches
                patient = df[df.social_security_nr==social_security_nr].idpatient
                if (not patient.empty) & (social_security_nr is not None):
                    idpatient = patient.iloc[0]
                    log.info("Adding patient. Patient already exists. Idpatient: " + str(idpatient))
                    return idpatient
                elif social_security_nr==None:
                    log.info("Adding patient. Patient with this name already exists at this clinic. Aborting.")
                    return None # matching name but since no soc sec nr was provided it is impossible to say whether there is a true match 
            # Check if patient exists for another clinic
            else: 
                if social_security_nr is not None:
                    query = text("SELECT * FROM patient WHERE social_security_nr= :p1")
                    result = self.engine.execute(query, {'p1':social_security_nr})
                    if result.rowcount > 0:
                        idpatient = result.fetchone()[0]
                        log.info("Adding patient. Patient already exists at another clinic. idpatient: " + str(idpatient))
                        self.insert_patient_to_clinical_listing(idpatient, idclinic)
                        return idpatient
                else:
                    log.info('Not adding patient without social security number')
                    return None
            # Let's add the patient
            result = self.insert_patient(last_name, first_name, social_security_nr, date_of_register)
            idpatient = result.lastrowid
            # Add to clinic
            self.insert_patient_to_clinical_listing(idpatient, idclinic)
            return idpatient  

    def insert_patient(self, last_name, first_name, social_security_nr, date_of_register):
            query = text('INSERT INTO patient (last_name, first_name, social_security_nr, date_of_register) VALUES (:p1, :p2, :p3, :p4)')
            boo = self.engine.execute(query, ({'p1':last_name, 'p2': first_name, 'p3': social_security_nr, 'p4': date_of_register}))
            log.info('SQL INSERT PATIENT, LAST_NAME=%s, FIRST_NAME=%s, idpatient=%s' % (last_name, first_name, boo.lastrowid))
            return boo
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
                result = self.engine.execute(query)
                return result
            except sqlal.exc.InternalError as e:               
                if retries > 0:
                    time.sleep(2)
                    retries = retries - 1
                    self.engine.connect()
                else:
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
                    elif isinstance(val, pd.datetime):
                        astring.append('\'%s\'' % val)
                    elif (val is None) or np.isnan(val):
                        astring.append('null')
                    else:
                        astring.append(str(val))

                df_values = ','.join(astring)
                temp_value += '(' + df_values + '),'
                if (counter % MAX_BULK_ROWS == 0 or counter == len(df)):
                    query = 'insert into %s (%s) values %s ON DUPLICATE KEY UPDATE %s=%s' % (
                    table, joined_columns, temp_value[:-1], df.columns[0], df.columns[0])
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
                if (counter % MAX_BULK_ROWS == 0 or counter == len(df)):
                    query = 'insert into %s (%s) values %s ON DUPLICATE KEY UPDATE %s=%s' % (
                    table, joined_columns, temp_value[:-1], df.columns[0], df.columns[0])
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
                self.query_database(query, retries=2)
                temp_value = ''


    def read_sql_query(self, query):
        """Read SQL query into a DataFrame. This function is used instead of dataframe.read_sql()
        since this will make testing easier."""

        result = self.engine.connect().execute(query)
        if query.split(' ')[0].upper() not in ['DELETE']:
            labels = result.keys()
            row = result.fetchall()
            dataframe = pd.DataFrame.from_records(row, columns=labels)
            none_cols = dataframe.isna().all()
            dataframe.loc[:, none_cols] = dataframe.loc[:, none_cols].fillna(np.nan)
            return dataframe 