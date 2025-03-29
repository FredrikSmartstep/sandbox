from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import date

class Product(SQLModel, table=True):
    idproduct: Optional[int] = Field(default=None, primary_key=True)
    name: str

    # Establishing a bidirectional relationship with ProductCompanyAssociation
    prod_companies: List["ProductCompanyAssociation"] = Relationship(back_populates="product")

    active_drugs: List["ActiveDrug"] = Relationship(
        back_populates="products", link_model="ProductHasActiveDrug"
    )
    regulatory_statuses: List["RegulatoryStatus"] = Relationship(back_populates="product")
    EMA_status: List["EMAStatus"] = Relationship(back_populates="product")
    EMA_orphan: List["EMAOrphan"] = Relationship(back_populates="product")
    HTA_documents: List["HTADocument"] = Relationship(
        back_populates="products", link_model="HTADocumentHasProduct"
    )
    indications: List["Indication"] = Relationship(
        back_populates="products", link_model="ProductHasIndication"
    )
    NT_council_follow_ups: List["NTCouncilFollowUp"] = Relationship(back_populates="product")
    NT_council_recommendations: List["NTCouncilRecommendation"] = Relationship(back_populates="product")
    NT_council_deals: List["NTCouncilDeal"] = Relationship(back_populates="product")
    forms: List["Form"] = Relationship(back_populates="product")


class ProductCompanyAssociation(SQLModel, table=True):
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct", primary_key=True)
    idcompany: Optional[int] = Field(default=None, foreign_key="company.idcompany", primary_key=True)
    role: str 

    product: "Product" = Relationship(back_populates="prod_companies")
    company: "Company" = Relationship(back_populates="comp_products")


class Company(SQLModel, table=True):
    idcompany: Optional[int] = Field(default=None, primary_key=True)
    name: str 

    comp_products: List["ProductCompanyAssociation"] = Relationship(back_populates="company")

    HTA_documents: List["HTADocument"] = Relationship(back_populates="company")
    prices: List["Price"] = Relationship(back_populates="company")
    NT_council_deals: List["NTCouncilDeal"] = Relationship(back_populates="company")


class ActiveDrug(SQLModel, table=True):
    idactive_drug: Optional[int] = Field(default=None, primary_key=True)
    name: str 
    ATC: str 
    DDD: Optional[int] = None  # Consider using Float if needed
    unit: str 
    admin_route: str 

    products: List["Product"] = Relationship(
        back_populates="active_drugs", link_model="ProductHasActiveDrug"
    )


class Analysis(SQLModel, table=True):
    idanalysis: Optional[int] = Field(default=None, primary_key=True)
    idHTA_document: Optional[int] = Field(default=None, foreign_key="HTA_document.idHTA_document")
    cohort: str = Field(max_length=45)
    intervention: str = Field(max_length=45)
    comparators_company: str = Field(max_length=100)
    comparators_agency: str = Field(max_length=100)
    comparator_reason_company: str = Field(max_length=45)
    comparator_reason_agency: str = Field(max_length=45)
    QALY_gain_company: str = Field(max_length=45)
    QALY_gain_agency_lower: str = Field(max_length=45)
    QALY_gain_agency_higher: str = Field(max_length=45)
    QALY_total_cost_company: str = Field(max_length=45)
    QALY_total_cost_agency_lower: str = Field(max_length=45)
    QALY_total_cost_agency_higher: str = Field(max_length=45)
    comparison_method: str = Field(max_length=45)
    indirect_method: str = Field(max_length=45)

    HTA_document: "HTADocument" = Relationship(back_populates="analyses")
    trials: List["Trial"] = Relationship(back_populates="analysis")


class EMAOrphan(SQLModel, table=True):
    idEMA_orphan: Optional[int] = Field(default=None, primary_key=True)
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct")

    product: "Product" = Relationship(back_populates="EMA_orphan")


class EMAStatus(SQLModel, table=True):
    idEMA_status: Optional[int] = Field(default=None, primary_key=True)
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct")
    therapeutic_area: str = Field(max_length=100)
    active_drug: str = Field(max_length=45)
    active_substance: str = Field(max_length=45)
    product_number: str = Field(max_length=45)
    patient_safety: bool
    authorisation_status: str = Field(max_length=45)
    atc: str = Field(max_length=45)
    additional_monitoring: bool
    generic: bool
    biosimilar: bool
    conditional_approval: bool
    exceptional_circumstances: bool
    accelerated_assessment: bool
    orphan_medicine: bool
    marketing_authorisation_date: Optional[date]
    date_of_refusal: Optional[date]
    human_pharmacotherapeutic_group: str = Field(max_length=45)
    date_of_opinion: Optional[date]
    decision_date: Optional[date]
    revision_number: str = Field(max_length=45)
    indication: str = Field(max_length=500)
    url: str = Field(max_length=150)

    product: "Product" = Relationship(back_populates="EMA_status")


class Expert(SQLModel, table=True):
    idexpert: Optional[int] = Field(default=None, primary_key=True)
    first_name: str = Field(max_length=45)
    last_name: str = Field(max_length=45)
    position: str = Field(max_length=45)

    HTA_documents: List["HTADocument"] = Relationship(
        back_populates="experts", link_model="HTADocumentHasExpert"
    )


class Form(SQLModel, table=True):
    idform: Optional[int] = Field(default=None, primary_key=True)
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct")
    strength: str = Field(max_length=45)
    form: str = Field(max_length=100)
    generic: bool
    MT_number: str = Field(max_length=45)
    NPL_id: str = Field(max_length=45)
    EUMA_number: str = Field(max_length=45)
    earlier_name: str = Field(max_length=300)

    product: "Product" = Relationship(back_populates="forms")
    prices: List["Price"] = Relationship(back_populates="form")


class HTAAgency(SQLModel, table=True):
    idHTA_agency: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=45)

    # Relationships
    staff: List["Staff"] = Relationship(back_populates="agency")
    HTA_documents: List["HTADocument"] = Relationship(back_populates="agency")


class HTADocument(SQLModel, table=True):
    idHTA_document: Optional[int] = Field(default=None, primary_key=True)
    title: str = Field(max_length=100)
    diarie_nr: str = Field(max_length=45)
    application_type: str = Field(max_length=150)
    date: Optional[date]
    decision: str = Field(max_length=45)
    document_type: str = Field(max_length=45)
    idcompany: Optional[int] = Field(default=None, foreign_key="company.idcompany")
    
    summary: Optional[str]
    comparators: str = Field(max_length=400)
    ICER_HTA: str = Field(max_length=45)
    ICER_company: str = Field(max_length=45)
    limitations: str = Field(max_length=200)
    three_part_deal: bool
    type_of_analysis: str = Field(max_length=45)
    indirect_comparison: bool
    number_of_patients: str = Field(max_length=45)
    annual_turnover: str = Field(max_length=45)
    costs_total_comp: str = Field(max_length=45)
    costs_total_HTA: str = Field(max_length=45)
    QALY_comp: str = Field(max_length=45)
    QALY_HTA: str = Field(max_length=45)
    delta_cost_comp: str = Field(max_length=45)
    biosim: bool
    resubmission: bool
    changed_decision: bool
    new_indication: bool
    new_form: bool
    new_price: bool
    new_strength: bool
    removed: bool
    temporary: bool
    sv_indications: str = Field(max_length=1000)
    currency: str = Field(max_length=45)
    requested_complement: bool
    requested_information: str = Field(max_length=45)
    requested_complement_submitted: bool

    indications: List["HTADocumentIndicationAssociation"] = Relationship(back_populates="document")
    staff: List["HTADocumentStaffAssocation"] = Relationship(back_populates="document")  # Added relationship
    analyses: List["Analysis"] = Relationship(back_populates="HTA_document")
    experts: List["Expert"] = Relationship(
        back_populates="HTA_documents", link_model="HTADocumentHasExpert"
    )
    products: List["Product"] = Relationship(
        back_populates="HTA_documents", link_model="HTADocumentHasProduct"
    )
    company: "Company" = Relationship(back_populates="HTA_documents")
    idHTA_agency: Optional[int] = Field(default=None, foreign_key="HTA_agency.idHTA_agency")
    agency: "HTAAgency" = Relationship(back_populates="HTA_documents")


class HTADocumentIndicationAssociation(SQLModel, table=True):
    idHTA_document: Optional[int] = Field(default=None, foreign_key="HTA_document.idHTA_document", primary_key=True)
    idindication: Optional[int] = Field(default=None, foreign_key="indication.idindication", primary_key=True)
    severity: str = Field(max_length=45)

    document: "HTADocument" = Relationship(back_populates="indications")
    indication: "Indication" = Relationship(back_populates="HTA_documents")


class HTADocumentHasExpert(SQLModel, table=True):
    idHTA_document: Optional[int] = Field(default=None, foreign_key="HTA_document.idHTA_document", primary_key=True)
    idexpert: Optional[int] = Field(default=None, foreign_key="expert.idexpert", primary_key=True)


class HTADocumentHasProduct(SQLModel, table=True):
    idHTA_document: Optional[int] = Field(default=None, foreign_key="HTA_document.idHTA_document", primary_key=True)
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct", primary_key=True)


class HTADocumentStaffAssocation(SQLModel, table=True):  # Added this model
    idHTA_document: Optional[int] = Field(default=None, foreign_key="HTA_document.idHTA_document", primary_key=True)
    idstaff: Optional[int] = Field(default=None, foreign_key="staff.idstaff", primary_key=True)
    role: str = Field(max_length=45)

    document: "HTADocument" = Relationship(back_populates="staff")
    staff: "Staff" = Relationship(back_populates="HTA_documents")


class Indication(SQLModel, table=True):
    idindication: Optional[int] = Field(default=None, primary_key=True)
    who_full_desc: str = Field(max_length=300)
    icd10_code: str = Field(max_length=45, unique=True)
    icd10_3_code: str = Field(max_length=45)
    icd10_3_code_desc: str = Field(max_length=200)
    valid_icd10_clinicaluse: bool
    valid_icd10_primary: bool
    valid_icd10_asterisk: bool
    valid_icd10_dagger: bool
    valid_icd10_sequelae: bool
    age_range: str = Field(max_length=45)
    gender: str = Field(max_length=3)
    status: str = Field(max_length=3)
    who_start_date: Optional[date]
    who_end_date: Optional[date]
    who_revision_history: str = Field(max_length=3)

    HTA_documents: List["HTADocumentIndicationAssociation"] = Relationship(back_populates="indication")
    products: List["Product"] = Relationship(
        back_populates="indications", link_model="ProductHasIndication"
    )


class NTCouncilDeal(SQLModel, table=True):
    idNT_council_deal: Optional[int] = Field(default=None, primary_key=True)
    date: Optional[date]
    ATC: str = Field(max_length=45)
    active_drug: str = Field(max_length=200)
    recipe_type: str = Field(max_length=45)
    start: Optional[date]
    end: Optional[date]
    option: Optional[date]
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct")
    idcompany: Optional[int] = Field(default=None, foreign_key="company.idcompany")

    product: "Product" = Relationship(back_populates="NT_council_deals")
    company: "Company" = Relationship(back_populates="NT_council_deals")


class NTCouncilFollowUp(SQLModel, table=True):
    idNT_council_follow_up: Optional[int] = Field(default=None, primary_key=True)
    date: Optional[date]
    active_drug: str = Field(max_length=200)
    indication: str = Field(max_length=200)
    URL: str = Field(max_length=200)
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct")
    ATC: str = Field(max_length=45)
    recommendation: str = Field(max_length=45)
    comment: str = Field(max_length=45)

    product: "Product" = Relationship(back_populates="NT_council_follow_ups")


class NTCouncilRecommendation(SQLModel, table=True):
    idNT_council_recommendation: Optional[int] = Field(default=None, primary_key=True)
    date: Optional[date]
    indication: str = Field(max_length=200)
    ATC: str = Field(max_length=45)
    recommendation: str = Field(max_length=45)
    comment: str = Field(max_length=200)
    active_drug: str = Field(max_length=200)
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct")

    product: "Product" = Relationship(back_populates="NT_council_recommendations")


class Staff(SQLModel, table=True):
    idstaff: Optional[int] = Field(default=None, primary_key=True)
    title: str = Field(max_length=45)
    name: str = Field(max_length=45)
    idHTA_agency: Optional[int] = Field(default=None, foreign_key="HTA_agency.idHTA_agency")

    agency: "HTAAgency" = Relationship(back_populates="staff")
    HTA_documents: List["HTADocumentStaffAssocation"] = Relationship(back_populates="staff")


class Price(SQLModel, table=True):
    idprice: Optional[int] = Field(default=None, primary_key=True)
    varunummer: int
    ATC: str = Field(max_length=45)
    NPL: int
    package: str = Field(max_length=200)
    size: str = Field(max_length=45)
    AIP: Optional[int] = None  # Consider using Float if needed
    AUP: Optional[int] = None  # Consider using Float if needed
    AIP_piece: Optional[int] = None  # Consider using Float if needed
    AUP_piece: Optional[int] = None  # Consider using Float if needed
    idcompany: Optional[int] = Field(default=None, foreign_key="company.idcompany")
    idform: Optional[int] = Field(default=None, foreign_key="form.idform")

    company: "Company" = Relationship(back_populates="prices")
    form: "Form" = Relationship(back_populates="prices")


class ProductHasActiveDrug(SQLModel, table=True):
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct", primary_key=True)
    idactive_drug: Optional[int] = Field(default=None, foreign_key="active_drug.idactive_drug", primary_key=True)


class ProductHasIndication(SQLModel, table=True):
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct", primary_key=True)
    idindication: Optional[int] = Field(default=None, foreign_key="indication.idindication", primary_key=True)
    source: str = Field(max_length=45)


class RegulatoryStatus(SQLModel, table=True):
    idregulatory_status: Optional[int] = Field(default=None, primary_key=True)
    idproduct: Optional[int] = Field(default=None, foreign_key="product.idproduct")
    strength: str = Field(max_length=45)
    form: str = Field(max_length=100)
    status: str = Field(max_length=45)
    approval_date: Optional[date]
    unregistration_date: Optional[date]
    procedure: str = Field(max_length=45)
    side_effect_spec: bool
    narcotics: bool
    exemption: bool
    prescription: str = Field(max_length=45)
    country: str = Field(max_length=45)

    product: "Product" = Relationship(back_populates="regulatory_statuses")


class Trial(SQLModel, table=True):
    idtrial: Optional[int] = Field(default=None, primary_key=True)
    idanalysis: Optional[int] = Field(default=None, foreign_key="analysis.idanalysis")
    title: str = Field(max_length=45)
    nr_of_patients: int
    nr_of_controls: int
    duration: str = Field(max_length=45)
    phase: str = Field(max_length=45)
    meta_analysis: bool
    randomized: bool
    controlled: bool
    blinded: str = Field(max_length=45)
    primary_outcome: str = Field(max_length=45)
    results: str = Field(max_length=100)
    safety: str = Field(max_length=100)

    analysis: "Analysis" = Relationship(back_populates="trials")
