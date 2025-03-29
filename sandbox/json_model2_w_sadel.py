import instructor
from sadel import Sadel
from sqlmodel import SQLModel, Field, Relationship

class HTA_Document_Product(Sadel, instructor.OpenAISchema, table=True):
    idhta_document: int | None = Field(default=None, foreign_key="hta_document.idhta_document", primary_key=True)
    idproduct: int | None = Field(default=None, foreign_key="product.idproduct", primary_key=True)
    _upsert_index_elements = {}

class HTA_Agency(Sadel, instructor.OpenAISchema, table=True):
    idhta_agency: int | None =  Field(default=None, primary_key=True)
    name: str
    _upsert_index_elements = {"name"}
    # Relationships
    #staff: List["Staff"] = Relationship(back_populates="agency")
    hta_documents: list["HTA_Document"] = Relationship(back_populates="agency")

class Company(Sadel, instructor.OpenAISchema, table=True):
    idcompany: int | None =  Field(default=None, primary_key=True)
    name: str 
    _upsert_index_elements = {"name"}
    #comp_products: List["ProductCompanyAssociation"] = Relationship(back_populates="company")


    hta_documents: list["HTA_Document"] = Relationship(back_populates="company")

class HTA_Document(Sadel, instructor.OpenAISchema, table=True):
    idhta_document: int | None = Field(default=None, primary_key=True)
    _upsert_index_elements = {"title"}
    title: str
    diarie_nr: str
    date: str
    decision: str
    limitations: str
    efficacy_summary: str
    safety_summary: str
    decision_summary: str
    currency: str
    requested_complement: bool
    requested_information: str
    requested_complement_submitted: bool

    idhta_agency: int = Field(foreign_key="hta_agency.idhta_agency")
    agency: HTA_Agency = Relationship(back_populates="hta_documents")

    idcompany: int | None = Field(default=None, foreign_key="company.idcompany")
    company: Company | None = Relationship(back_populates="hta_documents")

    products: list["Product"] = Relationship(back_populates="hta_documents", link_model=HTA_Document_Product)

    #products: List[Product] = Relationship(back_populates="hta_documents", link_model="HTADocumentHasProduct")

    picos: list["PICO"] = Relationship(back_populates="hta_document")
    
    #decision_makers: list["Person"] = Relationship(back_populates="htadocument")
    #presenter_to_the_board: Optional[Person] = Relationship(back_populates="HTADocument")
    #other_participants: List[Person] = Relationship(back_populates="HTADocument")


class Product(Sadel, instructor.OpenAISchema, table=True):
    idproduct: int | None = Field(default=None, primary_key=True)
    name: str
    _upsert_index_elements = {"name"}
    hta_documents: list[HTA_Document] = Relationship(back_populates="products", link_model=HTA_Document_Product)

"""class Form(SQLModel, table=True):
    id: int = Field(primary_key=True)
    form: str
    strength: str
    AIP: float
    AUP: float



class ProductCost(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product: str
    cost: float
    heresult_id: Optional[int] = Field(default=None, foreign_key="heresult.id")


class Indication(SQLModel, table=True):
    id: int = Field(primary_key=True)
    indication: str
    severity: str
    limitation: list[str]
    analysis: str
    form: list[Form] = Relationship(back_populates="indication")
"""

class HTADocumentHasProduct(Sadel, instructor.OpenAISchema, table=True):
    idhta_document: int | None = Field(default=None, foreign_key="hta_document.id", primary_key=True)
    idproduct: int | None =  Field(default=None, foreign_key="product.id", primary_key=True)
    _upsert_index_elements = {}
#class Product(SQLModel, table=True):
#    id: Optional[int] = Field(default=None, primary_key=True)
#    name: str

    # Establishing a bidirectional relationship with ProductCompanyAssociation
    #prod_companies: List["ProductCompanyAssociation"] = Relationship(back_populates="product")

    #active_drugs: List["ActiveDrug"] = Relationship(
    #    back_populates="products", link_model="ProductHasActiveDrug"
    #)
    #regulatory_statuses: List["RegulatoryStatus"] = Relationship(back_populates="product")
    #EMA_status: List["EMAStatus"] = Relationship(back_populates="product")
    #EMA_orphan: List["EMAOrphan"] = Relationship(back_populates="product")
   #hta_documents: List["HTA_Document"] = Relationship(back_populates="products", link_model="HTADocumentHasProduct")
    #indications: List["Indication"] = Relationship(
    #    back_populates="products", link_model="ProductHasIndication"
    #)
    #NT_council_follow_ups: List["NTCouncilFollowUp"] = Relationship(back_populates="product")
    #NT_council_recommendations: List["NTCouncilRecommendation"] = Relationship(back_populates="product")
    #NT_council_deals: List["NTCouncilDeal"] = Relationship(back_populates="product")
    #forms: List["Form"] = Relationship(back_populates="product")


# class HTA_Document(SQLModel, table=True):
#     id: Optional[int] = Field(default=None, primary_key=True)
#     title: str
#     product: str
#     diarie_nr: str
#     date: str
#     decision: str
#     efficacy_summary: str
#     safety_summary: str
#     decision_summary: str
#     currency: str
#     requested_complement: bool
#     requested_information: str
#     requested_complement_submitted: bool

#     company_id: int | None = Field(default=None, foreign_key="company.idcompany")
#     company: Company | None = Relationship(back_populates="hta_documents")

#     #products: List[Product] = Relationship(back_populates="hta_documents", link_model="HTADocumentHasProduct")

#     picos: list["PICO"] = Relationship(back_populates="hta_document")
    
#     #decision_makers: list["Person"] = Relationship(back_populates="htadocument")
#     #presenter_to_the_board: Optional[Person] = Relationship(back_populates="HTADocument")
#     #other_participants: List[Person] = Relationship(back_populates="HTADocument")


class Person(Sadel, instructor.OpenAISchema, table=True):
    id: int | None =  Field(default=None, primary_key=True)
    name: str
    profession: str 
    _upsert_index_elements = {"name"}
    #htadocument_id: int | None = Field(default=None, foreign_key="htadocument.id")
    #htadocument: HTA_Document | None = Relationship(back_populates="decision_makers")

class PICO(Sadel, instructor.OpenAISchema, table=True):
    idpico: int | None =  Field(default=None, primary_key=True)
    _upsert_index_elements = {}
    population: str
    incidence: str
    prevalence: str
    population: str
    intervention: str
    comparators_company: str
    comparator_modus_company: str
    comparator_reason_company: str
    comparators_TLV: str
    comparator_modus_TLV: str
    comparator_reason_TLV: str

    idhta_document: int | None = Field(default=None, foreign_key="hta_document.idhta_document")
    hta_document: HTA_Document | None = Relationship(back_populates="picos")

    analyses: list["Analysis"] = Relationship(back_populates="pico")

class Analysis(Sadel, instructor.OpenAISchema, table=True):
    idanalysis: int | None = Field(default=None, primary_key=True)
    _upsert_index_elements = {}
    QALY_gain_company: str
    QALY_total_cost_company: str
    QALY_gain_TLV_lower: str
    QALY_gain_TLV_higher: str
    QALY_total_cost_TLV_lower: str
    QALY_total_cost_TLV_higher: str
    QALY_cost_company: str
    QALY_cost_TLV_lower: str
    QALY_cost_TLV_higher: str
    comparison_method: str
    indirect_method: str
    
    idpico: int | None = Field(default=None, foreign_key="pico.idpico")
    pico: PICO | None = Relationship(back_populates="analyses")

    #trials_company: list["Trial"] = Relationship(back_populates="analysis")
    #drug_costs_company: List[ProductCost] = Relationship(back_populates="heresult")
    #other_costs_company: List[ProductCost] = Relationship(back_populates="heresult")
    #total_costs_company: List[ProductCost] = Relationship(back_populates="heresult")
    #drug_costs_TLV: List[ProductCost] = Relationship(back_populates="heresult")
    #other_costs_TLV: List[ProductCost] = Relationship(back_populates="heresult")
    #total_costs_TLV: List[ProductCost] = Relationship(back_populates="heresult")


class Trial(Sadel, instructor.OpenAISchema, table=True):
    idtrial: int | None =  Field(default=None, primary_key=True)
    _upsert_index_elements = {"title"}
    title: str
    nr_of_patients: int
    nr_of_controls: int
    duration: str
    phase: str
    meta_analysis: bool
    randomized: bool
    controlled: bool
    blinded: bool
    primary_outcome: str
    results: str
    safety: str

    #analysis_id: int | None = Field(default=None, foreign_key="analysis.id")
    #analysis: Analysis | None = Relationship(back_populates="trials_company")
