from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Float, Table, case, inspect
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import insert
#from pydantic import BaseModel

def is_pydantic(obj: object):
    """Checks whether an object is pydantic."""
    return type(obj).__class__.__name__ == "ModelMetaclass"

Base = declarative_base()

from functools import wraps
from typing import Union

from sqlalchemy.orm import MANYTOMANY, MANYTOONE, ONETOMANY

# not used
def handle_one_to_many_list(relation_cls, all_elements: list[dict]):
    instances = []
    for elem in all_elements:
        #for k, v in elem.items():
            #print('Element item, ' + str(k) + ':' + str(v))
        if elem.get("id"):
            instance = relation_cls.get_ref( elem.get("id"))
        else:
            instance = relation_cls(**elem)
        instances.append(instance)

    return instances

    elems_to_create = []
    updated_elems = []
    new_elems = []

    for elem in all_elements: # elem is e.g. a pico RECORD
        for k, v in elem.items() :
            print('Element item, ' + str(k) + ':' + str(v))
        elem_id = elem.get("id", None)

        existing_elem = relation_cls.get_ref(match_value=elem_id)

        if existing_elem is None:

            elems_to_create.append(elem)

        else:
            for key, value in elem.items():
                setattr(existing_elem, key, value)

            updated_elems.append(existing_elem)

    #new_elems = [relation_cls(**el) for el in elems_to_create]
    #new_elems.append(updated_elems)
    return new_elems


def auto_init(exclude: Union[set, list] = None):  # sourcery no-metrics
    """Wraps the `__init__` method of a class to automatically set the common
    attributes.

    Args:
        exclude (Union[set, list], optional): [description]. Defaults to None.
    """

    exclude = exclude or set()
    exclude.add("id")

    def decorator(init):
        @wraps(init)
        def wrapper(self, *args, **kwargs):  # sourcery no-metrics
            """
            Custom initializer that allows nested children initialization.
            Only keys that are present as instance's class attributes are allowed.
            These could be, for example, any mapped columns or relationships.

            Code inspired from GitHub.
            Ref: https://github.com/tiangolo/fastapi/issues/2194
            """
            cls = self.__class__
            model_columns = self.__mapper__.columns
            relationships = self.__mapper__.relationships
            
            #print('columns')
            #for c in model_columns:
            #    print(c)
            
            #print('relationships:')
            #for r in relationships:
            #    print(r)
            
            session = kwargs.get("session", None)
            #print('Starting walktrough')
            for key, val in kwargs.items():
                #print(key)
                #if isinstance(val, dict):
                #    for k, v in val.items() :
                #        print(str(k) + ':' + str(v))
                #else:
                #    print('Value:' + str(val))
                if key in exclude:
                    continue

                if not hasattr(cls, key):
                    continue
                    # raise TypeError(f"Invalid keyword argument: {key}")

                if key in model_columns:
                    #print('column')
                    setattr(self, key, val)
                    continue

                if key in relationships:
                    #print('realtionship: ' + key)
                    relation_dir = relationships[key].direction.name
                    relation_cls = relationships[key].mapper.entity
                    use_list = relationships[key].uselist

                    if relation_dir == ONETOMANY.name and use_list:
                        #print('one-to-many')
                        instances = []
                        for elem in val:
                            #for k, v in elem.items():
                            #    print('Element item, ' + str(k) + ':' + str(v))
                            instance = relation_cls.get_ref(elem, session=session)
                            if not instance:
                                 instance = relation_cls(**elem)
                            #if elem.get("id"):
                            #    instance = relation_cls.get_ref(elem, session=session)
                                #instance = relation_cls.get_ref(elem.get("id"), session=session)
                            #else:
                            #    instance = relation_cls(**elem)
                            instances.append(instance)
                        #instances = handle_one_to_many_list(relation_cls, val)
                        setattr(self, key, instances)

                    if relation_dir == ONETOMANY.name and not use_list:
                        #print('ome-to-one')
                        instance = relation_cls(**val)
                        setattr(self, key, instance)

                    elif relation_dir == MANYTOONE.name and not use_list:
                        #print('many-to-ome')
                        #if isinstance(val, dict):
                        #    if val.get("id"):
                        #        instance = relation_cls.get_ref(val, session=session)
                                #instance = relation_cls.get_ref(match_value=val.get("id"), session=session)
                        #    else:
                        #        instance = relation_cls(**val)
                                #raise ValueError(f"Expected 'id' to be provided for {key}")

                        #if isinstance(val, (str, int)):
                        #    instance = relation_cls.get_ref(val, session=session)
                            #instance = relation_cls.get_ref(match_value=val, session=session)
                        instance = relation_cls.get_ref(val, session=session)
                        if not instance:
                            instance = relation_cls(**val)
                        setattr(self, key, instance)

                    elif relation_dir == MANYTOMANY.name:
                        #print('many-to-many')
                        if not isinstance(val, list):
                            raise ValueError(f"Expected many to many input to be of type list for {key}")

                        if len(val) > 0 and isinstance(val[0], dict):
                            instances = []
                            for elem in val:
                                #if elem.get("id"):
                                #    instance = relation_cls.get_ref(elem, session=session)
                                    #instance = relation_cls.get_ref( elem.get("id"), session=session)
                                #else:
                                #    instance = relation_cls(**elem)
                                instance = relation_cls.get_ref(elem, session=session)
                                if not instance:
                                    instance = relation_cls(**elem)
                                instances.append(instance)
                            setattr(self, key, instances)

            return init(self, *args, **kwargs)

        return wrapper

    return decorator

# General upsert utility for models
class BaseClass(Base):
    __abstract__ = True  # mark as abstract to avoid table creation

    # @classmethod
    # def upsert(cls, item, session: Session):
    #     """upserts a single item"""
    #     stmt = cls._get_upsert_statement(item)
    #     session.execute(stmt)
    #     session.commit()

    # @classmethod
    # def _get_upsert_statement(cls, item) -> Insert:
    #     """Returns an UPSERT statement for a single item."""
    #     if not hasattr(cls, '_upsert_index_elements'):
    #         raise ValueError("No upsert index elements specified for the model.")
        
    #     to_insert = item.__dict__.copy()
    #     to_insert["created_on"] = func.now()
    #     to_update = {k: v for k, v in to_insert.items() if k not in cls._upsert_index_elements}
    #     stmt = insert(cls).values(to_insert)
    #    return stmt.on_duplicate_key_update(**to_update)
    @auto_init()
    def __init__(self, **_):
        pass

    @classmethod
    def get_ref(cls, data, session = None):
        if session:
            with session:
                # TODO: Fix thissss!!!!
                # The unique columns used to find existing rows in the db 
                if len(inspect(cls).class_._upsert_index_elements)>0:
                    col = list(inspect(cls).class_._upsert_index_elements)[0]
                    #log.info('adding, update if common ' + col)
                    #result = session.query(cls).filter_by(**{col: data.__getattr__(col)}).first()
                    result = session.query(cls).filter_by(**{col: data[col]}).first()
                else:
                    print('Missing upsert indices')
                    return None
                #eff_ref = getattr(cls, match_attr)
            return result #session.query(cls).filter(eff_ref == match_value).one_or_none()
        return None
    
    @classmethod
    def upsert(cls, data, session = None):
        result = None
        if session:
            with session:
                to_update = {k: v for k, v in data.items()}
                insert_stmt = insert(cls).values(**to_update)
                on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(data=insert_stmt.inserted.data, status="U")
                result = session.execute(on_duplicate_key_stmt)   
        return result #session.query(cls).filter(eff_ref == match_value).one_or_none()
        
    
    # @classmethod
    # def from_dto(cls, dto:BaseModel):
    #     obj = cls()
    #     properties = dict(dto)
    #     for key, value in properties.items():
    #         try:       
    #             if is_pydantic(value):
    #                 if not key.startswith('_'):
    #                     value = getattr(cls, key.lower()).property.mapper.class_.from_dto(value)
    #                     setattr(obj, key.lower(), value)
    #         except AttributeError as e:
    #             raise AttributeError(e)
    #     return obj

class Company(BaseClass):
    __tablename__ = 'company'
    id = Column(Integer, autoincrement=True)
    name = Column(String, nullable=False, primary_key=True)

    _upsert_index_elements = {"name"}

    # relationships
    hta_documents = relationship("HTA_Document", back_populates="company", cascade="save-update")

hta_document_product = Table(
    'hta_document_product', Base.metadata,
    Column('idhta_document', Integer, ForeignKey('hta_document.id')),
    Column('idproduct', Integer, ForeignKey('product.id'))
)

class Product(BaseClass):
    __tablename__ = 'product'
    id = Column(Integer, autoincrement=True)
    name = Column(String, nullable=False, primary_key=True)

    _upsert_index_elements = {"name"}
    # relationships
    hta_documents = relationship("HTA_Document", secondary=hta_document_product, back_populates="products", cascade="save-update")

class Staff(BaseClass):
    __tablename__ = 'staff'
    id = Column(Integer, autoincrement=True)
    profession = Column(String, nullable=False, primary_key=True)
    name = Column(String, nullable=False, primary_key=True)

    _upsert_index_elements = {"name"}
    # relationships
    idhta_agency = Column(Integer, ForeignKey('hta_agency.id'))
    hta_agency = relationship("HTA_Agency", back_populates="staff", cascade="save-update")
    staff_hta_documents = relationship("HTA_Document_Staff", back_populates="staff", cascade="save-update")


class HTA_Document_Staff(BaseClass):
    __tablename__ = 'hta_document_staff'
    idhta_document = Column(Integer, ForeignKey("hta_document.id"), primary_key=True)
    idstaff = Column(Integer, ForeignKey("staff.id"), primary_key=True)
    role = Column(String, primary_key=True)
    dissent = Column(String)

    _upsert_index_elements = {}#'idhta_document', 'idstaff', 'role'

    hta_document = relationship("HTA_Document", back_populates="staff", cascade="save-update")
    staff = relationship("Staff", back_populates="staff_hta_documents", cascade="save-update")


class Expert(BaseClass):
    __tablename__ = 'expert'
    id = Column(Integer, autoincrement=True)
    first_name = Column(String, primary_key=True)
    last_name = Column(String, primary_key=True)
    position = Column(String, primary_key=True)
    hta_documents = relationship("HTA_Document_Has_Expert", back_populates="expert", cascade="save-update")


class HTA_Document_Has_Expert(BaseClass):
    __tablename__ = 'hta_document_has_expert'
    idhta_document = Column(Integer, ForeignKey("hta_document.id"), primary_key=True)
    idexpert = Column(Integer, ForeignKey("expert.id"), primary_key=True)

    _upsert_index_elements = {}

    hta_document = relationship("HTA_Document", back_populates="experts", cascade="save-update")
    expert = relationship("Expert", back_populates="hta_documents", cascade="save-update")


class HTA_Agency(BaseClass):
    __tablename__ = 'hta_agency'
    id = Column(Integer, autoincrement=True)
    name = Column(String, nullable=False, primary_key=True)

    _upsert_index_elements = {"name"}
    # relationships
    staff = relationship("Staff", back_populates="hta_agency", cascade="save-update")

    hta_documents = relationship("HTA_Document", back_populates="hta_agency", cascade="save-update")

class Costs(BaseClass):
    __tablename__ = 'costs'
    id = Column(Integer, autoincrement=True)

    assessor = Column(String, primary_key=True)
    product = Column(String, primary_key=True)
    drug_cost = Column(String)
    other_costs = Column(String)
    total_treatment_cost = Column(String)
    cost_type = Column(String)

    _upsert_index_elements = {"assessor","product"}

    idanalysis = Column(Integer, ForeignKey('analysis.id'), primary_key=True)  
    analysis = relationship("Analysis", back_populates="costs", cascade="save-update")

class Outcome_Measure(BaseClass):
    __tablename__ = 'outcome_measure'
    id = Column(Integer, autoincrement=True)
    name = Column(String, nullable=False,  primary_key=True) # This is sick, but neede  in prder for sqlalchemy merge to work as it only operates on pks
    units = Column(String, nullable=False)

    _upsert_index_elements = {"name"}

    outcome_values = relationship("Outcome_Value", back_populates="outcome_measure", cascade="save-update")

class Outcome_Value(BaseClass):
    __tablename__ = 'outcome_value'
    id = Column(Integer, autoincrement=True)
    trial_arm = Column(String, nullable=False, primary_key=True)
    value = Column(String, nullable=False, primary_key=True)
    significance_level = Column(String, nullable=False)

    _upsert_index_elements = {"idoutcome_measure", "trial_arm", "value"}

    # Foreign keys and relationships    
    idoutcome_measure = Column(Integer, ForeignKey('outcome_measure.id'), primary_key=True)
    idtrial = Column(Integer, ForeignKey('trial.id'))  # Foreign key to link back to Trial
    outcome_measure = relationship("Outcome_Measure", back_populates="outcome_values", cascade="save-update")
    trial = relationship("Trial", back_populates="outcome_values", cascade="save-update")

class Trial(BaseClass):
    __tablename__ = 'trial'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    summary = Column(String)
    nr_of_patients = Column(Integer)
    nr_of_controls = Column(Integer)
    indication = Column(String)
    duration = Column(String)
    phase = Column(String)
    meta_analysis = Column(Boolean)
    randomized = Column(Boolean)
    controlled = Column(Boolean)
    type_of_control = Column(String)
    design = Column(String)
    objective = Column(String)
    reference =Column(String)
    url = Column(String)
    blinded = Column(String)
    primary_outcome_variable = Column(String)
    safety = Column(String)

    _upsert_index_elements = {"title", "indication"}

    # relationships
    outcome_values = relationship("Outcome_Value", back_populates="trial", cascade="save-update")

    idanalysis = Column(Integer, ForeignKey('analysis.id'))  # Foreign key linking Trial to Analysis
    analysis = relationship("Analysis", back_populates="trials", cascade="save-update")

class Analysis(BaseClass):
    __tablename__ = 'analysis'
    id = Column(Integer, autoincrement=True)
    
    analysis_type = Column(String)
    QALY_gain_company = Column(String)
    QALY_total_cost_company = Column(String)
    ICER_company = Column(String)

    QALY_gain_agency_lower = Column(String)
    QALY_gain_agency_higher = Column(String)
    QALY_total_cost_agency_lower = Column(String)
    QALY_total_cost_agency_higher = Column(String)
    ICER_agency_lower = Column(String)
    ICER_agency_higher = Column(String)
    
    comparison_method = Column(String)
    indirect_method = Column(String)
    efficacy_summary = Column(String)
    safety_summary = Column(String)
    decision_summary = Column(String)
    uncertainty_assessment_clinical = Column(String)
    uncertainty_assessment_he = Column(String)

    _upsert_index_elements = {"idpico"}
    # Relationships
    costs = relationship("Costs", back_populates="analysis", cascade="save-update")
    
    idpico = Column(Integer, ForeignKey('pico.id'), primary_key=True)
    pico = relationship("PICO", back_populates="analysis", cascade="save-update")

    trials = relationship("Trial", back_populates="analysis", cascade="save-update")

class PICO(BaseClass):
    __tablename__ = 'pico'
    id = Column(Integer, autoincrement=True)
    
    severity = Column(String)
    incidence = Column(String)
    prevalence = Column(String)
    pediatric = Column(Boolean)
    co_medication = Column(String)
    intervention = Column(String, primary_key=True)
    comparator_modus_company = Column(String)
    comparator_reason_company = Column(String)
    outcome_measure_company = Column(String)
    comparator_modus_agency = Column(String)
    comparator_reason_agency = Column(String)
    outcome_measure_agency = Column(String)

    _upsert_index_elements = {'population', 'intervention'}
    # Relationships
    idhta_document = Column('idHTA_document',Integer, ForeignKey('hta_document.id'))
    hta_document = relationship("HTA_Document", back_populates="picos", cascade="save-update")

    #comparator_company_id = Column(Integer, ForeignKey('product.id'))
    #comparator_agency_id = Column(Integer, ForeignKey('product.id'))
    comparator_company = Column(String, primary_key=True)#relationship("Product", foreign_keys=[comparator_company_id])
    comparator_agency = Column(String, primary_key=True)#relationship("Product", foreign_keys=[comparator_agency_id])

    analysis = relationship("Analysis", back_populates="pico", uselist=False, cascade="save-update") # one-to-one - this useliost=GFalse
    idpopulation = Column(Integer, ForeignKey('population.id'), primary_key=True)
    population = relationship("Population", back_populates="picos", cascade="save-update")
    idindication = Column(Integer, ForeignKey('indication.id'), primary_key=True)
    indication = relationship("Indication", back_populates="picos", cascade="save-update")

class Population(BaseClass):
    __tablename__ = 'population'
    id = Column(Integer, primary_key=True, autoincrement=True)
    description = Column(String)
    #ethnic_considerations = Column(String)
    genetic_factors = Column(String)
    family_history = Column(Boolean)
    sub_diagnosis = Column(String)
    disease_stage = Column(String)
    biomarker_status = Column(String)
    co_morbidities = Column(String)
    previous_treatment = Column(String)
    response_to_treatment = Column(String)
    lifestyle_factors = Column(String)
    psychosocial_factors = Column(String)
    special_conditions = Column(String)

    _upsert_index_elements = {'genetic_factors', 'family_history', 'sub_diagnosis', 'disease_stage', 
                              'biomarker_status', 'co_morbidities', 'previous_treatment', 'response_to_treatment',
                              'lifestyle_factors', 'psychosocial_factors', 'special_conditions'}

    picos = relationship("PICO", back_populates="population", cascade="save-update")
    id_demographics = Column(Integer, ForeignKey('demographics.id'))
    demographics = relationship("Demographics", back_populates="populations", cascade="save-update")


class Demographics(BaseClass):
    __tablename__ = 'demographics'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pediatric = Column(Boolean)
    adolescent = Column(Boolean)
    adult = Column(Boolean)
    elderly = Column(Boolean)
    gender = Column(String)

    _upsert_index_elements = {'pediatric','adolescent','adult','elderly','gender'}

    populations = relationship("Population", back_populates="demographics", cascade="save-update")


class Reference(BaseClass):
    __tablename__ = 'reference'
    id = Column(Integer, autoincrement=True)
    authors = Column(String)
    title = Column(String, primary_key=True)
    journal = Column(String)
    vol = Column(String)
    pages = Column(String)
    month = Column(String)
    year = Column(String)
    url = Column(String)

    ref_hta_documents = relationship("HTA_Document_Reference", back_populates="reference", cascade="save-update")

class HTA_Document_Reference(BaseClass):
    __tablename__ = 'hta_document_reference'
    idhta_document = Column(Integer, ForeignKey("hta_document.id"), primary_key=True)
    idreference = Column(Integer, ForeignKey("reference.id"), primary_key=True)

    hta_document = relationship("HTA_Document_Basis", back_populates="references", cascade="save-update")
    reference = relationship("Reference", back_populates="ref_hta_documents", cascade="save-update")

class HTA_Document_Indication(BaseClass):
    __tablename__ = 'hta_document_indication'
    idhta_document = Column(Integer, ForeignKey("hta_document.id"), primary_key=True)
    idindication = Column(Integer, ForeignKey("indication.id"), primary_key=True)
    severity = Column(String, primary_key=True)
    
    hta_document = relationship("HTA_Document", back_populates="indications", cascade="save-update")
    indication = relationship("Indication", back_populates="hta_documents", cascade="save-update")


class Indication(BaseClass):
    __tablename__ = 'indication'
    id = Column(Integer, autoincrement=True)
    icd10_code = Column(String, primary_key=True)

    hta_documents = relationship("HTA_Document_Indication", back_populates="indication", cascade="save-update")
    picos = relationship("PICO", back_populates="indication", cascade="save-update")


class HTA_Document(BaseClass):
    __tablename__ = 'hta_document'
    id = Column(Integer, autoincrement=True)
    title = Column(String, nullable=False)
    diarie_nr = Column(String, primary_key=True)
    date = Column(String, primary_key=True)
    decision = Column(String)
    limitations = Column(String)
    efficacy_summary = Column(String)
    safety_summary = Column(String)
    decision_summary = Column(String)
    currency = Column(String)
    analysis = Column(String)
    #annual_turnover = Column(String)
    document_type = Column(String, primary_key=True)
    requested_complement = Column(Boolean)
    requested_information = Column(String)
    requested_complement_submitted = Column(Boolean)
    #three_part_deal = Column(Boolean)
    previously_licensed_medicine = Column(Boolean)
    #latest_decision_date = Column(String)

    parsing_model = Column(String)

    _upsert_index_elements = {"diarie_nr", "date"}
    # Relationships
    idhta_agency = Column(Integer, ForeignKey('hta_agency.id'))
    hta_agency = relationship("HTA_Agency", back_populates="hta_documents", cascade="save-update")

    products = relationship("Product", secondary=hta_document_product, back_populates="hta_documents", cascade="save-update")

    idcompany = Column(Integer, ForeignKey('company.id'))
    company = relationship("Company", back_populates="hta_documents", cascade="save-update")

    picos = relationship("PICO", back_populates="hta_document", cascade="save-update")
    indications = relationship("HTA_Document_Indication", back_populates="hta_document", cascade="save-update")
    staff = relationship("HTA_Document_Staff", back_populates="hta_document", cascade="save-update")
    experts = relationship("HTA_Document_Has_Expert", back_populates="hta_document", cascade="save-update")

    __mapper_args__ = {
        "polymorphic_on": document_type,
        "polymorphic_identity": case(
                (document_type == "dossier", "dossier"),
                (document_type == "basis", "dossier"),
                else_="decision",
         )
    }

    
class HTA_Document_Basis(HTA_Document):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'hta_document'
    application_type = Column(String)
    annual_turnover = Column(String)
    three_part_deal = Column(Boolean)
    latest_decision_date = Column(String)
    url = Column(String)
    references = relationship("HTA_Document_Reference", back_populates="hta_document", cascade="save-update")

    __mapper_args__ = {
        "polymorphic_identity": "dossier",
    }