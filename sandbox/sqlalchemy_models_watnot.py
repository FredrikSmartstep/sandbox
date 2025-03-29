from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Float, Table, inspect
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel

def is_pydantic(obj: object):
    """Checks whether an object is pydantic."""
    return type(obj).__class__.__name__ == "ModelMetaclass"

Base = declarative_base()

from functools import wraps
from typing import Union

from sqlalchemy.orm import MANYTOMANY, MANYTOONE, ONETOMANY



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


class Company(BaseClass):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

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
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    _upsert_index_elements = {"name"}
    # relationships
    hta_documents = relationship("HTA_Document", secondary=hta_document_product, back_populates="products", cascade="save-update")

class Staff(BaseClass):
    __tablename__ = 'staff'
    id = Column(Integer, primary_key=True, autoincrement=True)
    profession = Column(String, nullable=False)
    name = Column(String, nullable=False)

    _upsert_index_elements = {"name"}
    # relationships
    idhta_agency = Column(Integer, ForeignKey('hta_agency.id'))
    hta_agency = relationship("HTA_Agency", back_populates="staff", cascade="save-update")
    staff_hta_documents = relationship("HTA_Document_Staff", back_populates="staff", cascade="save-update")


class HTA_Document_Staff(BaseClass):
    __tablename__ = 'hta_document_staff'
    idhta_document = Column(Integer, ForeignKey("hta_document.id"), primary_key=True)
    idstaff = Column(Integer, ForeignKey("staff.id"), primary_key=True)
    role = Column(String)
    dissent = Column(String)

    _upsert_index_elements = {}#'idhta_document', 'idstaff', 'role'

    hta_document = relationship("HTA_Document", back_populates="staff", cascade="save-update")
    staff = relationship("Staff", back_populates="staff_hta_documents", cascade="save-update")


class HTA_Agency(BaseClass):
    __tablename__ = 'hta_agency'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    _upsert_index_elements = {"name"}
    # relationships
    staff = relationship("Staff", back_populates="hta_agency", cascade="save-update")

    hta_documents = relationship("HTA_Document", back_populates="hta_agency", cascade="save-update")

class Costs(BaseClass):
    __tablename__ = 'costs'
    id = Column(Integer, primary_key=True, autoincrement=True)

    assessor = Column(String)
    product = Column(String)
    drug_cost = Column(String)
    other_costs = Column(String)
    total_treatment_cost = Column(String)

    _upsert_index_elements = {"assessor","product"}

    idanalysis = Column(Integer, ForeignKey('analysis.id'))  
    analysis = relationship("Analysis", back_populates="costs", cascade="save-update")

class Outcome_Measure(BaseClass):
    __tablename__ = 'outcome_measure'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    units = Column(String, nullable=False)

    _upsert_index_elements = {"name"}

class Outcome_Value(BaseClass):
    __tablename__ = 'outcome_value'
    id = Column(Integer, primary_key=True, autoincrement=True)
    trial_arm = Column(String, nullable=False)
    value = Column(String, nullable=False)
    significance_level = Column(String, nullable=False)

    _upsert_index_elements = {"idoutcome_measure", "trial_arm", "value"}

    # Foreign keys and relationships    
    idoutcome_measure = Column(Integer, ForeignKey('outcome_measure.id'))
    idtrial = Column(Integer, ForeignKey('trial.id'))  # Foreign key to link back to Trial
    outcome_measure = relationship("Outcome_Measure", cascade="save-update")
    trial = relationship("Trial", back_populates="results", cascade="save-update")

class Trial(BaseClass):
    __tablename__ = 'trial'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    number_of_patients = Column(Integer)
    number_of_controls = Column(Integer)
    indication = Column(String)
    duration = Column(String)
    phase = Column(String)
    meta_analysis = Column(Boolean)
    randomized = Column(Boolean)
    controlled = Column(Boolean)
    blinded = Column(String)
    primary_outcome_variable = Column(String)
    safety = Column(String)

    _upsert_index_elements = {"title", "indication"}

    # relationships
    results = relationship("Outcome_Value", back_populates="trial", cascade="save-update")
    idanalysis = Column(Integer, ForeignKey('analysis.id'))  # Foreign key linking Trial to Analysis
    analysis = relationship("Analysis", back_populates="trials", cascade="save-update")

class Analysis(BaseClass):
    __tablename__ = 'analysis'
    id = Column(Integer, primary_key=True, autoincrement=True)
    
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

    _upsert_index_elements = {}
    # Relationships
    costs = relationship("Costs", back_populates="analysis", cascade="save-update")
    
    idpico = Column(Integer, ForeignKey('pico.id'))
    pico = relationship("PICO", back_populates="analysis", cascade="save-update")

    trials = relationship("Trial", back_populates="analysis", cascade="save-update")

class PICO(BaseClass):
    __tablename__ = 'pico'
    id = Column(Integer, primary_key=True, autoincrement=True)
    indication = Column(String)
    icd_code = Column(String)
    severity = Column(String)
    population = Column(String)
    incidence = Column(String)
    prevalence = Column(String)
    co_medication = Column(String)
    intervention = Column(String)
    comparator_modus_company = Column(String)
    comparator_reason_company = Column(String)
    outcome_measure_company = Column(String)
    comparator_modus_agency = Column(String)
    comparator_reason_agency = Column(String)
    outcome_measure_agency = Column(String)

    _upsert_index_elements = {'population', 'intervention'}
    # Relationships
    idhta_document = Column(Integer, ForeignKey('hta_document.id'))
    hta_document = relationship("HTA_Document", back_populates="picos", cascade="save-update")

    #comparator_company_id = Column(Integer, ForeignKey('product.id'))
    #comparator_agency_id = Column(Integer, ForeignKey('product.id'))
    comparator_company = Column(String)#relationship("Product", foreign_keys=[comparator_company_id])
    comparator_agency = Column(String)#relationship("Product", foreign_keys=[comparator_agency_id])

    analysis = relationship("Analysis", back_populates="pico", uselist=False, cascade="save-update") # one-to-one - this useliost=GFalse

class HTA_Document(BaseClass):
    __tablename__ = 'hta_document'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    diarie_nr = Column(String)
    date = Column(String)
    decision = Column(String)
    limitations = Column(String)
    efficacy_summary = Column(String)
    safety_summary = Column(String)
    decision_summary = Column(String)
    currency = Column(String)
    requested_complement = Column(Boolean)
    requested_information = Column(String)
    requested_complement_submitted = Column(Boolean)
    previously_licensed_medicine = Column(Boolean)

    _upsert_index_elements = {"diarie_nr", "date"}
    # Relationships
    idhta_agency = Column(Integer, ForeignKey('hta_agency.id'))
    hta_agency = relationship("HTA_Agency", back_populates="hta_documents", cascade="save-update")

    products = relationship("Product", secondary=hta_document_product, back_populates="hta_documents", cascade="save-update")

    idcompany = Column(Integer, ForeignKey('company.id'))
    company = relationship("Company", back_populates="hta_documents", cascade="save-update")

    picos = relationship("PICO", back_populates="hta_document", cascade="save-update")

    staff = relationship("HTA_Document_Staff", back_populates="hta_document", cascade="save-update")
