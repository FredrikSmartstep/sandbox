import pandas as pd
from os.path import join, dirname, abspath 

from sqlmodel import Session, SQLModel, create_engine, inspect, and_#, insert
from json_model2 import HTA_Document, HTA_Agency, Company, Product, Product_Company, PICO, Staff, HTA_Document_Staff
from logger_tt import getLogger
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
            self.engine = create_engine(conn, connect_args=ssl_args)
        
        log.info(connect_text)

    def create_db_and_tables(self):
        SQLModel.metadata.create_all(self.engine)
       
    def close(self):
        log.info('Closing DB connection')
        self.connection.close()
        self.engine.dispose()
        log.info('DB engine dispose executed')

    def new_session(self):
        log.info("Creating a new database session")
        self.create_db_and_tables()
        #self.session = Session(self.engine)

    def insert_HTA(self, hta_doc, data_agency, data_company, data_products, data_picos, data_dm): 

        with Session(self.engine) as session:   
            session.autoflush = False   # flush occur during query 
            log.info('Adding agency')
            log.info(data_agency.model_dump())
            data_agency = self.try_to_find_and_update(session, HTA_Agency, data_agency) #self.upsert_with_merge(session, HTA_Agency, data_agency) 

            log.info('Adding company')
            log.info(data_company.model_dump())
            data_company = self.try_to_find_and_update(session, Company, data_company) #self.upsert_with_merge(session, Company, data_company)

            log.info('Adding products')
            prod_comp = []
            for k in range(0,len(data_products)):
                data_products[k]  = self.try_to_find_and_update(session=session, model=Product, data=data_products[k])
                prod_comp.append(Product_Company(idcompany=data_company.id, idproduct=data_products[k].id, role='owner'))
                prod_comp[k] = self.try_to_find_and_update(session=session, model=Product_Company, data=prod_comp[k])

            log.info('Check ig hta aleady in db')
            hta_doc = self.try_to_find_and_update(session, HTA_Document, hta_doc)

            log.info('Adding agency to hta')
            log.info(data_agency.model_dump())
            hta_doc.agency = data_agency

            log.info('Adding company to hta')
            log.info(data_company.model_dump())
            hta_doc.company = data_company
            #
            log.info('Adding doc')
            log.info(hta_doc.model_dump())
            session.add(hta_doc)
            #self.upsert_with_merge(session=session, model=HTA_Document, data=hta_doc) Didn't work. Got none on ids for prpduct and hta_agency??!!
            
            log.info('Adding picos')
            for k in range(0,len(data_picos)):
                data_picos[k].idhta_document = hta_doc.id
                data_picos[k] = self.try_to_find_and_update(session=session, model=PICO, data=data_picos[k])

            log.info('Adding dm')
            for s in data_dm:
                st = Staff(title=s.profession, name=s.name, idHTA_agency=data_agency.id)
                st = self.try_to_find_and_update(session=session, model=Staff, data=st)
                #hta_doc.staff.append(st)
                hta_staff = HTA_Document_Staff(idHTA_document = hta_doc.id, idstaff=st.idstaff, role=s.role)
                hta_staff = self.try_to_find_and_update(session=session, model=HTA_Document_Staff, data=hta_staff)

            session.commit() 

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

        result = None
        # The unique columns used to find existing rows in the db 
        if len(inspect(model).class_._upsert_index_elements.default)>0:
            col = list(inspect(model).class_._upsert_index_elements.default)[0]
            log.info('adding, update if common ' + col)
            result = session.query(model).filter_by(**{col: data.__getattr__(col)}).first()

        if result is None:
            #result = data
            log.info('Not found in DB. New!')
            session.add(data)
        else:
            log.info('Found in db')
            # Let sqlalchemy understand that they are the same instance
            pk_name = inspect(model).primary_key[0].name
            setattr(data, pk_name, result.__getattr__(pk_name))
            # Let's bring in the database object but update with attributes from 'data' in session
            session.merge(data) 

        #session.commit()
        session.flush()
        #session.refresh(data)

        return data
    
    def try_to_find_and_update(self, session, model, data):
        # This is a hack since for some reason merge does not work
        result = None
        # The unique columns used to find existing rows in the db 
        if len(inspect(model).class_._upsert_index_elements.default)>0:
            cols = list(inspect(model).class_._upsert_index_elements.default)
            vals=list(map(lambda x: data.__getattr__(x),cols))
            filters = dict(zip(cols,vals))
            log.info('adding, update if common ')# + str(and_(**{col: data.__getattr__(col)})))
            #result = session.query(model).filter_by(and_(**{col: data.__getattr__(col)})).first()
            query = session.query(model)
            for attr,value in filters.items():
                query = query.filter( getattr(model,attr)==value )
            # now we can run the query
            result = query.first()
            #result = session.query(model).filter(and_(*(field == value for field, value in filters.items()))).first()
        if result is None:
            result = data
            log.info('Not found in DB. New!')
        else:
            log.info('Found in db')
            # Now, let's update all attributes that differ
            for attr in inspect(model).mapper.column_attrs: 
                if getattr(data, attr.key):      
                    if getattr(data, attr.key)!=(getattr(result, attr.key)):
                        log.info('Updating ' + attr.key + ' with ' + str(getattr(data,attr.key)))
                        setattr(result, attr.key, getattr(data,attr.key))
        return result

