import json
import os
from os.path import join, dirname, abspath 
import sqlalchemy as sqlal
from sqlalchemy import exc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from logger_tt import getLogger

from db_model2 import Base, HTAAgency, Personal
from secret import secrets

log = getLogger(__name__)

SSL_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/ssl/DigiCertGlobalRootCA.crt.pem'

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
        agency = self.insert_agency(session, 'TLV')
        
        # persons = []
        # for person_data in data['decision_makers']:
        #     persons.append(self.insert_person(person_data, hta_doc, 'board member'))
        
        # for person_data in data['presenter_to_the_board']:
        #     persons.append(self.insert_person(person_data, hta_doc, 'presenter to board'))

        # Commit the session
        session.commit()

        # Close the session
        session.close()
    
    
    
    @staticmethod
    def insert_person(session, person_data):
        log.info('Insert person')
        person = session.query(Personal).filter_by(name=person_data['name']).first()
        if not person:
            person = Personal(name=person_data['name'], title=person_data['profession'])
            #session.add(person)
            #session.commit()
        return person

    

    @staticmethod  
    def insert_agency(session, name):
        log.info('Insert agency')
        agency = session.query(HTAAgency).filter_by(name=name).first()
        if not agency:
            agency = HTAAgency(name=name)
            #session.add(agency)
            #session.commit()
        return agency

    