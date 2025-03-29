from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    ForeignKey,
    func,
    insert
)
from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy.dialects.mysql import Insert
from pydantic import BaseModel

def is_pydantic(obj: object):
    """Checks whether an object is pydantic."""
    return type(obj).__class__.__name__ == "ModelMetaclass"

Base = declarative_base()

# General upsert utility for models
class BaseClass(Base):
    __abstract__ = True  # mark as abstract to avoid table creation

    @classmethod
    def upsert(cls, item, session: Session):
        """upserts a single item"""
        stmt = cls._get_upsert_statement(item)
        session.execute(stmt)
        session.commit()

    @classmethod
    def _get_upsert_statement(cls, item) -> Insert:
        """Returns an UPSERT statement for a single item."""
        if not hasattr(cls, '_upsert_index_elements'):
            raise ValueError("No upsert index elements specified for the model.")
        
        to_insert = item.__dict__.copy()
        to_insert["created_on"] = func.now()
        to_update = {k: v for k, v in to_insert.items() if k not in cls._upsert_index_elements}
        stmt = insert(cls).values(to_insert)
        return stmt.on_duplicate_key_update(**to_update)
    
    @classmethod
    def from_dto(cls, dto:BaseModel):
        obj = cls()
        properties = dict(dto)
        for key, value in properties.items():
            try:       
                if is_pydantic(value):
                    value = getattr(cls, key).property.mapper.class_.from_dto(value)
                setattr(obj, key, value)
            except AttributeError as e:
                raise AttributeError(e)
        return obj

# Define each class

class HTA_Document_Product(BaseClass):
    __tablename__ = 'hta_document_product'
    idhta_document = Column(Integer, ForeignKey("hta_document.id"), primary_key=True)
    idproduct = Column(Integer, ForeignKey("product.id"), primary_key=True)
    _upsert_index_elements = {'idhta_document', 'idproduct'}

class HTA_Document_Has_Expert(BaseClass):
    __tablename__ = 'hta_document_has_expert'
    idhta_document = Column(Integer, ForeignKey("hta_document.id"), primary_key=True)
    idexpert = Column(Integer, ForeignKey("expert.idexpert"), primary_key=True)
    _upsert_index_elements = {'idhta_document', 'idexpert'}

    hta_document = relationship("HTA_Document", back_populates="experts")
    expert = relationship("Expert", back_populates="hta_documents")

class HTA_Document_Staff(BaseClass):
    __tablename__ = 'hta_document_staff'
    idhta_document = Column(Integer, ForeignKey("hta_document.id"), primary_key=True)
    idstaff = Column(Integer, ForeignKey("staff.idstaff"), primary_key=True)
    role = Column(String)
    _upsert_index_elements = {'idhta_document', 'idstaff', 'role'}

    hta_document = relationship("HTA_Document", back_populates="document_staff")
    staff = relationship("Staff", back_populates="staff_hta_documents")

class Product_Company(BaseClass):
    __tablename__ = 'product_company'
    idproduct = Column(Integer, ForeignKey("product.id"), primary_key=True)
    idcompany = Column(Integer, ForeignKey("company.id"), primary_key=True)
    role = Column(String, primary_key=True, nullable=True)
    _upsert_index_elements = {'idproduct', 'idcompany', 'role'}

    company = relationship("Company", back_populates="products_company")
    product = relationship("Product", back_populates="product_companies")

class HTA_Agency(BaseClass):
    __tablename__ = 'hta_agency'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    _upsert_index_elements = {"name"}

    staff = relationship("Staff", back_populates="agency")
    hta_documents = relationship("HTA_Document", back_populates="agency")

class Company(BaseClass):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    _upsert_index_elements = {"name"}

    products_company = relationship("Product_Company", back_populates="company")
    hta_documents = relationship("HTA_Document", back_populates="company")

class Expert(BaseClass):
    __tablename__ = 'expert'
    idexpert = Column(Integer, primary_key=True)
    first_name = Column(String(45))
    last_name = Column(String(45))
    position = Column(String(45))
    _upsert_index_elements = {"last_name"}

    HTA_documents = relationship("HTA_Document", back_populates="experts")

class HTA_Document(BaseClass):
    __tablename__ = 'hta_document'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    diarie_nr = Column(String)
    date = Column(String)
    decision = Column(String)
    limitations = Column(String)
    
    currency = Column(String)
    requested_complement = Column(Boolean)
    requested_information = Column(String)
    requested_complement_submitted = Column(Boolean)
    _upsert_index_elements = {"diarie_nr", "title"}

    idhta_agency = Column(Integer, ForeignKey("hta_agency.id"))
    agency = relationship("HTA_Agency", back_populates="hta_documents")

    idcompany = Column(Integer, ForeignKey("company.id"))
    company = relationship("Company", back_populates="hta_documents")

    products = relationship("Product", back_populates="hta_documents")
    picos = relationship("PICO", back_populates="hta_document")
    document_staff = relationship("HTA_Document_Staff", back_populates="hta_document")
   # experts = relationship("Expert", back_populates="HTA_documents")

class Product(BaseClass):
    __tablename__ = 'product'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    _upsert_index_elements = {"name"}

    hta_documents = relationship("HTA_Document", back_populates="products")
    product_companies = relationship("Product_Company", back_populates="product")

class Staff(BaseClass):
    __tablename__ = 'staff'
    idstaff = Column(Integer, primary_key=True)
    title = Column(String)
    name = Column(String)
    _upsert_index_elements = {"name"}

    idHTA_agency = Column(Integer, ForeignKey("hta_agency.id"))
    agency = relationship("HTA_Agency", back_populates="staff")
    staff_hta_documents = relationship("HTA_Document_Staff", back_populates="staff")

class Decision_Maker(BaseClass):
    __tablename__ = 'decision_maker'
    id_dc = Column(Integer, primary_key=True)
    profession = Column(String)
    name = Column(String)
    role = Column(String, default='decision_maker')

class PICO(BaseClass):
    __tablename__ = 'pico'
    idpico = Column(Integer, primary_key=True)
    population = Column(String)
    incidence = Column(String)
    prevalence = Column(String)
    intervention = Column(String)
    
    comparators_company = Column(String)
    comparator_modus_company = Column(String)
    comparator_reason_company = Column(String)
    comparators_agency = Column(String)
    comparator_modus_agency = Column(String)
    comparator_reason_agency = Column(String)

    efficacy_summary = Column(String)
    safety_summary = Column(String)
    decision_summary = Column(String)

    _upsert_index_elements = {'population', 'idhta_document'}

    idhta_document = Column(Integer, ForeignKey("hta_document.id"))
    hta_document = relationship("HTA_Document", back_populates="picos")
    analyses = relationship("Analysis", back_populates="pico")

class Analysis(BaseClass):
    __tablename__ = 'analysis'
    idanalysis = Column(Integer, primary_key=True)
    QALY_gain_company = Column(String)
    QALY_total_cost_company = Column(String)
    QALY_gain_TLV_lower = Column(String)
    QALY_gain_TLV_higher = Column(String)
    QALY_total_cost_TLV_lower = Column(String)
    QALY_total_cost_TLV_higher = Column(String)
    QALY_cost_company = Column(String)
    QALY_cost_TLV_lower = Column(String)
    QALY_cost_TLV_higher = Column(String)
    comparison_method = Column(String)
    indirect_method = Column(String)

    idpico = Column(Integer, ForeignKey("pico.idpico"))
    pico = relationship("PICO", back_populates="analyses")

class Trial(BaseClass):
    __tablename__ = 'trial'
    idtrial = Column(Integer, primary_key=True)
    title = Column(String)
    nr_of_patients = Column(Integer)
    nr_of_controls = Column(Integer)
    duration = Column(String)
    phase = Column(String)
    meta_analysis = Column(Boolean)
    randomized = Column(Boolean)
    controlled = Column(Boolean)
    blinded = Column(Boolean)
    primary_outcome = Column(String)
    results = Column(String)
    safety = Column(String)
    _upsert_index_elements = {"title"}

class Indication_Simplified(BaseClass):
    __tablename__ = 'indication_simplified'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    severity = Column(String)
    icd = Column(String)