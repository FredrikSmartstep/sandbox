import instructor
from sqlmodel import SQLModel, Field, Relationship
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert, Insert

# class BaseClass(SQLModel, instructor.OpenAISchema, table=True):

#     @classmethod
#     def upsert(cls, item: SQLModel, session: sa.orm.Session):
#         """upserts a single item"""
#         stmt = cls._get_upsert_statement(item)
#         session.exec(stmt)
#         session.commit()
    
#     @classmethod
#     def _get_upsert_statement(cls, item: SQLModel) -> Insert:
#         """Returns an UPSERT statement for a single item."""
#         if not cls._upsert_index_elements:
#             raise ValueError("No upsert index elements specified for the model.")

#         to_insert = item.model_dump()
#         to_insert["created_on"] = (
#             sa.func.now()
#         )  # set manually, because on_conflict_do_update doesn't trigger default oninsert
#         to_update = cls._get_record_to_update(to_insert)
#         stmt = insert(cls).values(to_insert)
#         return stmt.on_conflict_do_update(
#             index_elements=cls._upsert_index_elements,
#             set_=to_update,
#         )

class HTA_Document_Product(SQLModel, instructor.OpenAISchema, table=True):
    idhta_document: int | None = Field(default=None, foreign_key="hta_document.id", primary_key=True)
    idproduct: int | None = Field(default=None, foreign_key="product.id", primary_key=True)
    _upsert_index_elements = {'idhta_document', 'idproduct'}

class HTA_Document_Has_Expert(SQLModel, instructor.OpenAISchema, table=True):
    idhta_document: int | None = Field(default=None, foreign_key="hta_document.id", primary_key=True)
    idexpert: int | None = Field(default=None, foreign_key="expert.idexpert", primary_key=True)
    _upsert_index_elements = {'idhta_document', 'idexpert'}

class HTA_Document_Staff(SQLModel, instructor.OpenAISchema, table=True):
    idhta_document: int | None = Field(default=None, foreign_key="hta_document.id", primary_key=True)
    idstaff: int | None = Field(default=None, foreign_key="staff.idstaff", primary_key=True)
    role: str
    _upsert_index_elements = {'idhta_document', 'idstaff', 'role'}

    hta_document: "HTA_Document" = Relationship(back_populates="document_staff")
    staff: "Staff" = Relationship(back_populates="staff_hta_documents")

class Product_Company(SQLModel, table=True):
    _upsert_index_elements = {'idproduct','idcompany','role'}
    idproduct: int | None = Field(default=None, foreign_key="product.id", primary_key=True)
    idcompany: int | None = Field(default=None, foreign_key="company.id", primary_key=True)
    role: str | None = Field(default=None, primary_key=True)

    company: "Company" = Relationship(back_populates='products_company')
    product: "Product" = Relationship(back_populates="product_companies")

class HTA_Agency(SQLModel, instructor.OpenAISchema, table=True):
    __tablename__='hta_agency'
    id: int | None =  Field(default=None, primary_key=True)
    name: str
    _upsert_index_elements = {"name"}
    
    # Relationships
    staff: list["Staff"] = Relationship(back_populates="agency")

    hta_documents: list["HTA_Document"] = Relationship(back_populates="agency")

class Company(SQLModel, instructor.OpenAISchema, table=True):
    id: int | None =  Field(default=None, primary_key=True)
    name: str 
    _upsert_index_elements = {"name"}

    products_company: list[Product_Company] = Relationship(back_populates="company")

    hta_documents: list["HTA_Document"] = Relationship(back_populates="company")

class Expert(SQLModel, instructor.OpenAISchema, table=True):
    _upsert_index_elements = {"lastname"}
    idexpert: int | None = Field(default=None, primary_key=True)
    first_name: str = Field(max_length=45)
    last_name: str = Field(max_length=45)
    position: str = Field(max_length=45)

    HTA_documents: list["HTA_Document"] = Relationship(back_populates="experts", link_model=HTA_Document_Has_Expert)

class HTA_Document(SQLModel, instructor.OpenAISchema, table=True):
    id: int | None = Field(default=None, primary_key=True)
    _upsert_index_elements = {"diarie_nr", "title"}
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

    idhta_agency: int | None = Field(foreign_key="hta_agency.id")
    agency: HTA_Agency | None = Relationship(back_populates="hta_documents")

    idcompany: int | None = Field(default=None, foreign_key="company.id")
    company: Company | None = Relationship(back_populates="hta_documents")

    products: list["Product"] = Relationship(back_populates="hta_documents", link_model=HTA_Document_Product)

    picos: list["PICO"] = Relationship(back_populates="hta_document")

    document_staff: list[HTA_Document_Staff] = Relationship(back_populates="hta_document")

    experts: list[Expert] = Relationship(back_populates="HTA_documents", link_model=HTA_Document_Has_Expert)
    #decision_makers: list["Person"] = Relationship(back_populates="htadocument")
    #presenter_to_the_board: Optional[Person] = Relationship(back_populates="HTADocument")
    #other_participants: List[Person] = Relationship(back_populates="HTADocument")


class Product(SQLModel, instructor.OpenAISchema, table=True):
    __tablename__='product'
    id: int | None = Field(default=None, primary_key=True)
    name: str
    _upsert_index_elements = {"name"}
    hta_documents: list[HTA_Document] = Relationship(back_populates="products", link_model=HTA_Document_Product)

    product_companies: list[Product_Company] = Relationship(back_populates="product")

"""class Form(SQLModel, table=True):
    id: int = Field(primary_key=True)
    form: str
    strength: str
    AIP: float
    AUP: float



class ProductCost(SQLModel, table=True):
    id: int | None = = Field(default=None, primary_key=True)
    product: str
    cost: float
    heresult_id: int | None = = Field(default=None, foreign_key="heresult.id")


class Indication(SQLModel, table=True):
    id: int = Field(primary_key=True)
    indication: str
    severity: str
    limitation: list[str]
    analysis: str
    form: list[Form] = Relationship(back_populates="indication")
"""

#class Product(SQLModel, table=True):
#    id: int | None = = Field(default=None, primary_key=True)
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
#     id: int | None = = Field(default=None, primary_key=True)
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


class Staff(SQLModel, instructor.OpenAISchema, table=True):
    idstaff: int | None = Field(default=None, primary_key=True)
    title: str 
    name: str 
    _upsert_index_elements = {"name"}

    idHTA_agency: int | None = Field(default=None, foreign_key="hta_agency.id")
    agency: HTA_Agency = Relationship(back_populates="staff")

    staff_hta_documents: list[HTA_Document_Staff] = Relationship(back_populates="staff")

class Decision_Maker(SQLModel, instructor.OpenAISchema, table=True):
    id_dc: int | None = Field(default=None, primary_key=True)
    profession: str 
    name: str 
    role: str = Field(default='decision_maker')

class PICO(SQLModel, instructor.OpenAISchema, table=True):
    idpico: int | None =  Field(default=None, primary_key=True)
    _upsert_index_elements = {'population', 'idhta_document'}
    population: str
    incidence: str
    prevalence: str
    intervention: str
    comparators_company: str
    comparator_modus_company: str
    comparator_reason_company: str
    comparators_agency: str
    comparator_modus_agency: str
    comparator_reason_agency: str

    idhta_document: int | None = Field(default=None, foreign_key="hta_document.id")
    hta_document: HTA_Document | None = Relationship(back_populates="picos")

    analyses: list["Analysis"] = Relationship(back_populates="pico")

class Analysis(SQLModel, instructor.OpenAISchema, table=True):
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


class Trial(SQLModel, instructor.OpenAISchema, table=True):
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


class Indication_Simplified(SQLModel, instructor.OpenAISchema, table=True):
    id: int | None =  Field(default=None, primary_key=True)
    name: str
    severity: str
    icd: str
