from __future__ import annotations
from typing import Dict
from sqlalchemy import create_engine, Column, Integer, String, Date, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship, declarative_base, DeclarativeBase
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm.collections import attribute_keyed_dict

#Base = declarative_base(bind=engine)
class Base(DeclarativeBase):
    pass

class Product(Base):
    __tablename__ = 'product'

    idproduct: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(64))

    # Establishing a bidirectional relationship with CompanyHasProduct
    prod_companies = relationship("ProductCompanyAssociation", back_populates="product")
    # Proxy to access companies and their roles directly from Product
    company_roles = association_proxy('prod_companies', 'role')
    
    active_drugs = relationship("ActiveDrug", secondary="product_has_active_drug", back_populates="products")
    regulatory_statuses = relationship("RegulatoryStatus", back_populates="product")
    EMA_status = relationship("EMAStatus", back_populates="product")
    EMA_orphan = relationship("EMAOrphan", back_populates="product")
    HTA_documents = relationship("HTADocument", secondary="HTA_document_has_product",back_populates="products")
    indications = relationship("Indication", secondary='product_has_indication',back_populates="products")
    NT_council_follow_ups = relationship("NTCouncilFollowUp", back_populates="product")
    NT_council_recommendations = relationship("NTCouncilRecommendation", back_populates="product")
    NT_council_deals = relationship("NTCouncilDeal", back_populates="product")
    forms = relationship("Form", back_populates="product")

class ProductCompanyAssociation(Base):
    __tablename__ = "product_company"
    idproduct: Mapped[int] = mapped_column(ForeignKey("product.idproduct"), primary_key=True)
    idcompany: Mapped[int] = mapped_column(ForeignKey("company.idcompany"), primary_key=True)
    role: Mapped[str] = mapped_column(String(64))
    
    # Establishing relationships
    company = relationship("Company", back_populates="comp_products")
    product = relationship("Product", back_populates="prod_companies")

class Company(Base):
    __tablename__ = "company"
    idcompany: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(64))

    # Establishing a bidirectional relationship with CompanyHasProduct
    comp_products = relationship("ProductCompanyAssociation", back_populates="company")

    # Proxy to access product roles directly from Company
    product_roles = association_proxy('products', 'role')

    HTA_documents = relationship("HTADocument", back_populates="company")
    prices = relationship("Price", back_populates="company")
    NT_council_deals = relationship("NTCouncilDeal", back_populates="company")

    def __init__(self, name: str):
        self.name = name

class ActiveDrug(Base):
    __tablename__ = 'active_drug'

    idactive_drug = Column(Integer, primary_key=True)
    name = Column(String(200))
    ATC = Column(String(45))
    DDD = Column(Integer)  # Consider using Float if needed
    unit = Column(String(45))
    admin_route = Column(String(45))

    products = relationship("Product", secondary="product_has_active_drug", back_populates="active_drugs")


class Analysis(Base):
    __tablename__ = 'analysis'

    idanalysis = Column(Integer, primary_key=True)
    idHTA_document = Column(Integer, ForeignKey('HTA_document.idHTA_document'))
    cohort = Column(String(45))
    intervention = Column(String(45))
    comparators_company = Column(String(100))
    comparators_agency = Column(String(100))
    comparator_reason_company = Column(String(45))
    comparator_reason_agency = Column(String(45))
    QALY_gain_company = Column(String(45))
    QALY_gain_agency_lower = Column(String(45))
    QALY_gain_agency_higher = Column(String(45))
    QALY_total_cost_company = Column(String(45))
    QALY_total_cost_agency_lower = Column(String(45))
    QALY_total_cost_agency_higher = Column(String(45))
    comparison_method = Column(String(45))
    indirect_method = Column(String(45))

    HTA_document = relationship("HTADocument", back_populates="analyses")

    trials = relationship("Trial", back_populates="analysis")

class EMAOrphan(Base):
    __tablename__ = 'EMA_orphan'

    idEMA_orphan = Column(Integer, primary_key=True)
    idproduct = Column(Integer, ForeignKey('product.idproduct'))

    product = relationship("Product", back_populates="EMA_orphan")


class EMAStatus(Base):
    __tablename__ = 'EMA_status'

    idEMA_status = Column(Integer, primary_key=True)
    idproduct = Column(Integer, ForeignKey('product.idproduct'))
    therapeutic_area = Column(String(100))
    active_drug = Column(String(45))
    active_substance = Column(String(45))
    product_number = Column(String(45))
    patient_safety = Column(Boolean)
    authorisation_status = Column(String(45))
    atc = Column(String(45))
    additional_monitoring = Column(Boolean)
    generic = Column(Boolean)
    biosimilar = Column(Boolean)
    conditional_approval = Column(Boolean)
    exceptional_circumstances = Column(Boolean)
    accelerated_assessment = Column(Boolean)
    orphan_medicine = Column(Boolean)
    marketing_authorisation_date = Column(Date)
    date_of_refusal = Column(Date)
    human_pharmacotherapeutic_group = Column(String(45))
    date_of_opinion = Column(Date)
    decision_date = Column(Date)
    revision_number = Column(String(45))
    indication = Column(String(500))
    url = Column(String(150))

    product = relationship("Product", back_populates="EMA_status")



class Expert(Base):
    __tablename__ = 'expert'

    idexpert = Column(Integer, primary_key=True)
    first_name = Column(String(45))
    last_name = Column(String(45))
    position = Column(String(45))

    HTA_documents = relationship("HTADocument", secondary="HTA_document_has_expert", back_populates="experts")


class Form(Base):
    __tablename__ = 'form'

    idform = Column(Integer, primary_key=True)
    idproduct = Column(Integer, ForeignKey('product.idproduct'))
    strength = Column(String(45))
    form = Column(String(100))
    generic = Column(Boolean)
    MT_number = Column(String(45))
    NPL_id = Column(String(45))
    EUMA_number = Column(String(45))
    earlier_name = Column(String(300))

    product = relationship("Product", back_populates="forms")
    prices = relationship("Price", back_populates="form")


class HTAAgency(Base):
    __tablename__ = 'HTA_agency'

    idHTA_agency = Column(Integer, primary_key=True)
    name = Column(String(45))

    staff = relationship("Staff", back_populates="agency")
    HTA_documents = relationship("HTADocument", back_populates="agency")


class HTADocument(Base):
    __tablename__ = 'HTA_document'

    idHTA_document = Column(Integer, primary_key=True)
    title = Column(String(100))
    diarie_nr = Column(String(45))
    application_type = Column(String(150))
    date = Column(Date)
    decision = Column(String(45))
    document_type = Column(String(45))
    idcompany = Column(Integer, ForeignKey('company.idcompany'))
    idHTA_agency = Column(Integer, ForeignKey('HTA_agency.idHTA_agency'))
    summary = Column(Text)
    comparators = Column(String(400))
    ICER_HTA = Column(String(45))
    ICER_company = Column(String(45))
    limitations = Column(String(200))
    three_part_deal = Column(Boolean)
    type_of_analysis = Column(String(45))
    indirect_comparison = Column(Boolean)
    number_of_patients = Column(String(45))
    annual_turnover = Column(String(45))
    costs_total_comp = Column(String(45))
    costs_total_HTA = Column(String(45))
    QALY_comp = Column(String(45))
    QALY_HTA = Column(String(45))
    delta_cost_comp = Column(String(45))
    biosim = Column(Boolean)
    resubmission = Column(Boolean)
    changed_decision = Column(Boolean)
    new_indication = Column(Boolean)
    new_form = Column(Boolean)
    new_price = Column(Boolean)
    new_strength = Column(Boolean)
    removed = Column(Boolean)
    temporary = Column(Boolean)
    sv_indications = Column(String(1000))
    currency = Column(String(45))
    requested_complement = Column(Boolean)
    requested_information = Column(String(45))
    requested_complement_submitted = Column(Boolean)

    indications = relationship("HTADocumentIndicationAssociation",  back_populates="document")

    indication_severity = association_proxy('indications', 'severity')

    staff = relationship("HTADocumentStaffAssocation", back_populates="document")

    staff_roles = association_proxy('staff', 'role')

    analyses = relationship("Analysis", back_populates="HTA_document")
    #forms = relationship("Form", back_populates="HTA_document")
    
    experts = relationship("Expert", secondary="HTA_document_has_expert", back_populates="HTA_documents")
    
    products = relationship("Product", secondary="HTA_document_has_product", back_populates="HTA_documents")
    company = relationship("Company", back_populates="HTA_documents")
    agency = relationship("HTAAgency", back_populates="HTA_documents")


class HTADocumentHasExpert(Base):
    __tablename__ = 'HTA_document_has_expert'

    idHTA_document = Column(Integer, ForeignKey('HTA_document.idHTA_document'), primary_key=True)
    idexpert = Column(Integer, ForeignKey('expert.idexpert'), primary_key=True)

    #document = relationship("HTADocument", back_populates="experts")
    #expert = relationship("Expert", back_populates="HTA_documents")



class HTADocumentIndicationAssociation(Base):
    __tablename__ = 'HTA_document_indication'

    idHTA_document = Column(Integer, ForeignKey('HTA_document.idHTA_document'), primary_key=True)
    idindication = Column(Integer, ForeignKey('indication.idindication'), primary_key=True)
    severity = Column(String(45))

    document = relationship("HTADocument", back_populates="indications")
    indication = relationship("Indication", back_populates="HTA_documents")


class HTADocumentStaffAssocation(Base):
    __tablename__ = 'HTA_document_staff'

    idHTA_document = Column(Integer, ForeignKey('HTA_document.idHTA_document'), primary_key=True)
    idstaff = Column(Integer, ForeignKey('staff.idstaff'), primary_key=True)
    role = Column(String(45))

    document = relationship("HTADocument", back_populates="staff")
    staff = relationship("Staff", back_populates="HTA_documents")

class HTADocumentHasProduct(Base):
    __tablename__ = 'HTA_document_has_product'

    idHTA_document = Column(Integer, ForeignKey('HTA_document.idHTA_document'), primary_key=True)
    idproduct = Column(Integer, ForeignKey('product.idproduct'), primary_key=True)

    #document = relationship("HTADocument")
    #product = relationship("Product")

class Indication(Base):
    __tablename__ = 'indication'

    idindication = Column(Integer, primary_key=True)
    who_full_desc = Column(String(300))
    icd10_code = Column(String(45), unique=True)
    icd10_3_code = Column(String(45))
    icd10_3_code_desc = Column(String(200))
    valid_icd10_clinicaluse = Column(Boolean)
    valid_icd10_primary = Column(Boolean)
    valid_icd10_asterisk = Column(Boolean)
    valid_icd10_dagger = Column(Boolean)
    valid_icd10_sequelae = Column(Boolean)
    age_range = Column(String(45))
    gender = Column(String(3))
    status = Column(String(3))
    who_start_date = Column(Date)
    who_end_date = Column(Date)
    who_revision_history = Column(String(3))

    HTA_documents = relationship("HTADocumentIndicationAssociation", back_populates="indication")
    # Proxy to access product roles directly from Company
    HTA_documents_severities = association_proxy('HTA_documents', 'severity')

    products = relationship("Product", secondary="product_has_indication", back_populates="indications")


class NTCouncilDeal(Base):
    __tablename__ = 'NT_council_deal'

    idNT_council_deal = Column(Integer, primary_key=True)
    date = Column(Date)
    ATC = Column(String(45))
    active_drug = Column(String(200))
    recipe_type = Column(String(45))
    start = Column(Date)
    end = Column(Date)
    option = Column(Date)
    idproduct = Column(Integer, ForeignKey('product.idproduct'))
    idcompany = Column(Integer, ForeignKey('company.idcompany'))

    product = relationship("Product", back_populates="NT_council_deals")
    company = relationship("Company", back_populates="NT_council_deals")


class NTCouncilFollowUp(Base):
    __tablename__ = 'NT_council_follow_up'

    idNT_council_follow_up = Column(Integer, primary_key=True)
    date = Column(Date)
    active_drug = Column(String(200))
    indication = Column(String(200))
    URL = Column(String(200))
    idproduct = Column(Integer, ForeignKey('product.idproduct'))
    ATC = Column(String(45))
    recommendation = Column(String(45))
    comment = Column(String(45))

    product = relationship("Product", back_populates="NT_council_follow_ups")


class NTCouncilRecommendation(Base):
    __tablename__ = 'NT_council_recommendation'

    idNT_council_recommendation = Column(Integer, primary_key=True)
    date = Column(Date)
    indication = Column(String(200))
    ATC = Column(String(45))
    recommendation = Column(String(45))
    comment = Column(String(200))
    active_drug = Column(String(200))
    idproduct = Column(Integer, ForeignKey('product.idproduct'))

    product = relationship("Product", back_populates="NT_council_recommendations")


class Staff(Base):
    __tablename__ = 'staff'
    idstaff = Column(Integer, primary_key=True)
    title = Column(String(45))
    name = Column(String(45))
    idHTA_agency = Column(Integer, ForeignKey('HTA_agency.idHTA_agency'))

    agency = relationship("HTAAgency", back_populates="staff")

    HTA_documents = relationship("HTADocumentStaffAssocation", back_populates="staff")

    staff_roles = association_proxy('HTA_documents', 'role')


class Price(Base):
    __tablename__ = 'price'

    idprice = Column(Integer, primary_key=True)
    varunummer = Column(Integer)
    ATC = Column(String(45))
    NPL = Column(Integer)
    package = Column(String(200))
    size = Column(String(45))
    AIP = Column(Integer)  # Consider using Float if needed
    AUP = Column(Integer)  # Consider using Float if needed
    AIP_piece = Column(Integer)  # Consider using Float if needed
    AUP_piece = Column(Integer)  # Consider using Float if needed
    idcompany = Column(Integer, ForeignKey('company.idcompany'))
    idform = Column(Integer, ForeignKey('form.idform'))

    company = relationship("Company", back_populates="prices")
    form = relationship("Form", back_populates="prices")


class ProductHasActiveDrug(Base):
    __tablename__ = 'product_has_active_drug'

    idproduct = Column(Integer, ForeignKey('product.idproduct'), primary_key=True)
    idactive_drug = Column(Integer, ForeignKey('active_drug.idactive_drug'), primary_key=True)

    #product = relationship("Product")
    #active_drug = relationship("ActiveDrug")


class ProductHasIndication(Base):
    __tablename__ = 'product_has_indication'

    idproduct = Column(Integer, ForeignKey('product.idproduct'), primary_key=True)
    idindication = Column(Integer, ForeignKey('indication.idindication'), primary_key=True)
    source = Column(String(45))

    #product = relationship("Product")
    #indication = relationship("Indication")


class RegulatoryStatus(Base):
    __tablename__ = 'regulatory_status'

    idregulatory_status = Column(Integer, primary_key=True)
    idproduct = Column(Integer, ForeignKey('product.idproduct'))
    strength = Column(String(45))
    form = Column(String(100))
    status = Column(String(45))
    approval_date = Column(Date)
    unregistration_date = Column(Date)
    procedure = Column(String(45))
    side_effect_spec = Column(Boolean)
    narcotics = Column(Boolean)
    exemption = Column(Boolean)
    prescription = Column(String(45))
    country = Column(String(45))

    product = relationship("Product", back_populates="regulatory_statuses")


class Trial(Base):
    __tablename__ = 'trial'

    idtrial = Column(Integer, primary_key=True)
    idanalysis = Column(Integer, ForeignKey('analysis.idanalysis'))
    title = Column(String(45))
    nr_of_patients = Column(Integer)
    nr_of_controls = Column(Integer)
    duration = Column(String(45))
    phase = Column(String(45))
    meta_analysis = Column(Boolean)
    randomized = Column(Boolean)
    controlled = Column(Boolean)
    blinded = Column(String(45))
    primary_outcome = Column(String(45))
    results = Column(String(100))
    safety = Column(String(100))

    analysis = relationship("Analysis", back_populates="trials")



