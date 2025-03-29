from datetime import date
from pydantic import BaseModel, ConfigDict, Field, constr, conlist, field_validator, model_validator
import sqlalchemy_models360 as sqlal_models
from enum import Enum
from typing import Any, Literal, Optional
import re


class MissingDataException(Exception):
    pass

# the validation set is used to ensure that data was indeeed extracted by openAI by checking that each variable in the validation set has been populated

class OurBaseModel(BaseModel):
    class Config:
        validate_assignment = True
        validate_default = True
        use_enum_values = True
    
    #def quality_check(self, instance):
        # Go through the valdation set and its test
        # val_set = [{'variable_name': ,'metric': one of bool/populated, value, }]


class Company(OurBaseModel):
    name: str
    
    class Meta:
        orm_model = sqlal_models.Company 
        validation_set = ['name']

class Product(OurBaseModel):
    name: str

    class Meta:
        orm_model = sqlal_models.Product
        validation_set = ['name']


class Indication(OurBaseModel):
    indication: str
    icd10_code: Optional[constr(pattern='^([a-tA-T]|[v-zV-Z0-9])\d[a-zA-Z0-9](\.[a-zA-Z0-9]{1,4})?$')]

    class Meta:
        validation_set = ['indication']


class Reference(OurBaseModel):
    authors: Optional[str] = None
    title: str
    journal: Optional[str] = None
    vol: Optional[str] = None
    pages: Optional[str] = None
    month: Optional[str] = None
    year: Optional[str] = None
    url: Optional[str] = None

    @field_validator("authors", "title", "journal", "vol", "pages", "month", "year", "url", mode="before")
    @classmethod
    def transform(cls, raw) -> str:
        return str(raw)
    
    class Meta:
        validation_set = ['title']


class HTA_Document_Reference(OurBaseModel):
    reference: Reference

    class Meta:
        orm_model = sqlal_models.HTA_Document_Reference
        validation_set = ['reference']

class References(OurBaseModel):
    references: list[HTA_Document_Reference]

    class Meta:
        validation_set = ['references']

class Profession(str, Enum): # added str class to allow for initialization using strings, see https://github.com/pydantic/pydantic/issues/3850
    OTHER = 'other'
    ANALYST = 'analyst'
    ASSOCIATE_POFESSOR = 'associate professor'

    BOARD_CHAIRMAN = 'board chairman'
    BOARD_MEMBER = 'board member'
    
    #CARDIOLOGIST = 'cardiologist'
    CHIEF_PHARMACIST = 'chief pharmacist'
    CHIEF_LEGAL_OFFICER = 'chief legal officer'
    CHIEF_MEDICAL_OFFICER = 'chief medical officer'
    CHIEF_RESEARCH_OFFICER = 'chief research officer'
    CLINICAL_RESEARCHER = 'clinical researcher'
    COORDINATOR = 'coordinator'
    COUNTRY_COORDINATOR = 'country coordinator'
    COUNTY_COUNCILOR = 'county councilor'
    
    COUNTY_DIRECTOR = 'county director'
    DIRECTOR_GENERAL = 'director general'
    DIECTOR_OF_ADVOCACY = 'director of advocacy'
    DEPARTMENT_MANAGER = 'department manager'
    
    ENGINEER = 'engineer'
    
    FORMER_CHIEF_PHARMACIST = 'former chief pharmacist'
    FORMER_UNION_CHAIRMAN = 'former union chairman'
    
    UNION_CHAIRMAN = 'union chairman'
    FORMER_COUNTY_DIRECTOR = 'former county director'
    
    HEALTH_ECONOMIST = 'health economist'
    HEALTH_AND_MEDICAL_CARE_DIRECTOR = 'Health and Medical Care Director'
    
    INVESTIGATOR = 'investigator'
    
    LAWYER = 'lawyer'
    
    MEDICAL_ADVISOR = 'medical advisor'
    MEDICAL_INVESTIGATOR = 'medical investigator'
    MEDICAL_DOCTOR ='medical doctor'
    
    #PATIENT_REPRESENTATIVE = 'patient representative'
    PROFESSOR = 'professor'
    READER = 'reader'
    STATISTICIAN = 'statistician'


class Staff(OurBaseModel):
    profession: Literal['other', 'analyst','associate professor', 'board chairman',  'board member', 'chief pharmacist', 'chief legal officer', 'chief medical officer', 'chief research officer', \
                       'clinical researcher', 'coordinator', 'country coordinator', 'county councilor', 'county director', 'director general', 'director of advocacy', 'department manager', \
                        'engineer', 'former chief pharmacist', 'former union chairman', 'union chairman', 'former county director', 'health economist', 'Health and Medical Care Director', \
                         'investigator', 'lawyer', 'medical advisor', 'medical investigator', 'medical doctor', 'professor', 'reader', 'statistician'] #Profession
    name: str 

    @field_validator("profession", mode="before")
    @classmethod
    def trans(cls, raw):
        if type(raw)!=str:
            return raw
            
        raw = re.sub(r'tidigare', 'former', raw, flags=re.I)
        raw = re.sub(r'tidigare', 'former', raw, flags=re.I)
        raw = re.sub(r'jur.*', 'lawyer',  raw, flags=re.I)
        raw = re.sub(r'docent.*', 'reader',  raw, flags=re.I)
        raw = re.sub(r'läkemedelschef.*', 'chief pharmacist',  raw, flags=re.I)
        raw = re.sub(r'.*profess.*', 'professor',  raw, flags=re.I)
        raw = re.sub(r'.*hälsoeko.*', 'health economist',  raw, flags=re.I)
        raw = re.sub(r'.*medicinska utr.*', 'medical investigator',  raw, flags=re.I)
        raw = re.sub(r'förbundsord.*', 'union chairman',  raw, flags=re.I)
        raw = re.sub(r'intressepoli.*', 'director of advocacy',  raw, flags=re.I)
        raw = re.sub(r'lektor.*', 'associate professor',  raw, flags=re.I)
        raw = re.sub(r'Enhetschef.*', 'department manager',  raw, flags=re.I)       
        raw = re.sub(r'.*läkare.*', 'medical doctor',  raw, flags=re.I)
        raw = re.sub(r'.*överint.*','director general',  raw, flags=re.I)
        raw = re.sub(r'koordina.*','coordinator',  raw, flags=re.I)
        raw = re.sub(r'.*rådgivare*','medical advisor',  raw, flags=re.I)
        raw = re.sub(r'landsting.*','county director',  raw, flags=re.I)
        raw = re.sub(r'avdelningschef.*','department manager',  raw, flags=re.I)

        return raw 

    class Meta:
        orm_model = sqlal_models.Staff
        validation_set = ['name']

class Agency(Enum):
    TLV='TLV'

    #class Meta:
    #    validation_set = ['TLV']

class HTA_Agency(OurBaseModel):
    name: Agency
    staff: list[Staff]

    class Meta:
        orm_model = sqlal_models.HTA_Agency
        validation_set = []

class Costs(OurBaseModel):
   # model_config = ConfigDict(coerce_numbers_to_str=True)

    assessor: Literal['company','agency']
    product: Literal['product','comparator']
    drug_cost: Optional[str] = Field(coerce_numbers_to_str=True, strict=False)
    other_costs: Optional[str]  = Field(coerce_numbers_to_str=True, strict=False)
    total_treatment_cost: Optional[str]  = Field(coerce_numbers_to_str=True, strict=False)
    cost_type: str

    class Meta:
        orm_model = sqlal_models.Costs
        validation_set = ['assessor']

class Outcome_Measure(OurBaseModel):
    name: Optional[str] = None
    units: Optional[str] = None

    class Meta:
        orm_model = sqlal_models.Outcome_Measure
        validation_set = []

class Outcome_Value(OurBaseModel):
   # model_config = ConfigDict(coerce_numbers_to_str=True)

    trial_arm: Optional[str] = None
    value: Optional[str] = None
    significance_level: Optional[str] = None
    outcome_measure: Outcome_Measure

    class Meta:
        orm_model = sqlal_models.Outcome_Value
        validation_set = []

class Trial(OurBaseModel):
    pico_nr: int
    title: str
    summary: Optional[str] = None
    nr_of_patients: Optional[int] = None
    nr_of_controls: Optional[int] = None
    indication: Optional[str] = None
    duration: Optional[str] = None
    phase: Optional[str] = None
    meta_analysis: Optional[bool] = None
    randomized: Optional[bool] = None
    controlled: Optional[bool] = None
    type_of_control: str
    design: str
    objective: str
    blinded: Optional[str] = None
    primary_outcome_variable: Optional[str] = None
    reference: Optional[str] = None
    url: Optional[str] = None
    outcome_values: list[Outcome_Value]
    safety: Optional[str] = None

    @field_validator("meta_analysis", "randomized", "controlled", mode="before")
    @classmethod
    def transform_bool(cls, raw: str) -> bool:
        if raw in ['no','No','NO','Nej','nej','0']:
            return False
        elif raw in ['yes','Yes','Ja','ja','1']:
            return True
        else:
            return bool(raw)


    class Meta:
        orm_model = sqlal_models.Trial
        validation_set = ['title']

class Trials(OurBaseModel):
    trials: conlist(item_type=Trial, min_length=0)

    class Meta:
        validation_set = ['trials']

class Basic_Analysis(OurBaseModel):
   # model_config = ConfigDict(coerce_numbers_to_str=True)
    analysis_type: Literal['cost-minimization', 'cost-effectiveness', 'cost-benefit']

    QALY_gain_company: str
   # QALY_total_cost_company: str
    QALY_gain_agency_lower: str
    QALY_gain_agency_higher: str
   # QALY_total_cost_agency_lower: str
   # QALY_total_cost_agency_higher: str
    ICER_company: str
    ICER_agency_lower: str
    ICER_agency_higher: str
    comparison_method: str
    indirect_method: str
    efficacy_summary:str 
    safety_summary: str 
    decision_summary: str 
    uncertainty_assessment_clinical: str 
    uncertainty_assessment_he: str

    # @field_validator("ICER_company", "ICER_agency_lower",  "ICER_agency_higher", mode="before")
    # @classmethod
    # def trans(cls, raw):
    #    if type(raw)!=str:
    #        return raw
    #    raw = re.sub('million', '000000', raw, flags=re.I)
    #    raw = re.sub('thousand', '000', raw, flags=re.I)
    #    raw = re.sub(',', '', raw)
    #    raw = re.sub(' ', '', raw)
    #    if re.search('\d*', raw):
    #     raw = re.search('\d*', raw)[0]

    class Meta:
        validation_set = ['analysis_type']

class Analysis(OurBaseModel):
   # model_config = ConfigDict(coerce_numbers_to_str=True)
    analysis_type: Literal['cost-minimization', 'cost-effectiveness', 'cost-benefit']
    QALY_gain_company: str
   # QALY_total_cost_company: str
    QALY_gain_agency_lower: str
    QALY_gain_agency_higher: str
   # QALY_total_cost_agency_lower: str
   # QALY_total_cost_agency_higher: str
    ICER_company: str
    ICER_agency_lower: str
    ICER_agency_higher: str
    comparison_method: str
    indirect_method: str
    efficacy_summary:str 
    safety_summary: str 
    decision_summary: str 
    uncertainty_assessment_clinical: str 
    uncertainty_assessment_he: str

    costs: conlist(item_type=Costs, min_length=2)
    trials: conlist(item_type=Trial, min_length=0) # there are actually dossiers without trial data

    # @field_validator("ICER_company", "ICER_agency_lower",  "ICER_agency_higher", mode="before")
    # @classmethod
    # def trans(cls, raw):
    #    if type(raw)!=str:
    #        return raw
    #    raw = re.sub('million', '000000', raw, flags=re.I)
    #    raw = re.sub('thousand', '000', raw, flags=re.I)
    #    raw = re.sub(',', '', raw)
    #    raw = re.sub(' ', '', raw)
    #    if re.search('\d*', raw):
    #     raw = re.search('\d*', raw)[0]

    class Meta:
        orm_model = sqlal_models.Analysis
        validation_set= ['analysis_type']

class Analysis_List(OurBaseModel):
    analyses: list[Analysis]

    class Meta:
        validation_set= ['analyses'] 

class Analysis_Company(OurBaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    analysis_type: Literal['cost-minimization', 'cost-effectiveness', 'cost-benefit']

    QALY_gain_company: str
  #  QALY_total_cost_company: Optional[str]
    ICER_company: str
    comparison_method: str
    indirect_method: str

    costs: list[Costs]
    trials: list[Trial]

    # @field_validator("ICER_company", mode="before")
    # @classmethod
    # def trans(cls, raw):
    #    if type(raw)!=str:
    #        return raw
    #    raw = re.sub('million', '000000', raw, flags=re.I)
    #    raw = re.sub('thousand', '000', raw, flags=re.I)
    #    raw = re.sub(',', '', raw)
    #    raw = re.sub(' ', '', raw)
    #    if re.search('\d*', raw):
    #     raw = re.search('\d*', raw)[0]

    class Meta:
        validation_set= ['analysis_type']

class Analysis_Agency(OurBaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    QALY_gain_agency_lower: str
    QALY_gain_agency_higher: str

  #  QALY_total_cost_agency_lower: Optional[str]
  #  QALY_total_cost_agency_higher: Optional[str]
    
    ICER_agency_lower: str
    ICER_agency_higher: str

    efficacy_summary:str 
    safety_summary: str 
    decision_summary: str 
    uncertainty_assessment_clinical: str 
    uncertainty_assessment_he: str

    costs: list[Costs]

    # @field_validator("ICER_agency_lower",  "ICER_agency_higher", mode="before")
    # @classmethod
    # def trans(cls, raw):
    #    if type(raw)!=str:
    #        return raw
    #    raw = re.sub('million', '000000', raw, flags=re.I)
    #    raw = re.sub('thousand', '000', raw, flags=re.I)
    #    raw = re.sub(',', '', raw)
    #    raw = re.sub(' ', '', raw)
    #    if re.search('\d*', raw):
    #     raw = re.search('\d*', raw)[0]

    class Meta:
        validation_set= ['efficacy_summary']

class Analysis_List_Comp(OurBaseModel):
    analyses: list[Analysis_Company]

    class Meta:
        validation_set= ['analyses']

class Analysis_List_Ag(OurBaseModel):
    analyses: list[Analysis_Agency]

    class Meta:
        validation_set= ['analyses']


class Demographics(OurBaseModel):
    pediatric: bool
    adolescent: bool
    adult: bool
    elderly: bool
    gender: Literal['Male', 'Female', 'All']

    @field_validator("pediatric", "adolescent", "adult", "elderly", mode="before")
    @classmethod
    def transform_demo(cls, raw: str) -> bool:
        if type(raw)!=str:
           return raw
        if raw in ['no','No','NO','Nej','nej','0']:
            return False
        elif raw in ['yes','Yes','Ja','ja','1']:
            return True
        else:
            return bool(raw)
    
    class Meta:
        validation_set = ['gender']

class Population(OurBaseModel):
    description: str   
    demographics: Demographics 
    ethnic_considerations: str
    genetic_factors: str
    family_history: bool
    sub_diagnosis: str
    disease_stage: str
    biomarker_status: str
    co_morbidities: str
    previous_treatment: str
    response_to_treatment: str
    lifestyle_factors: str
    psychosocial_factors: str
    special_conditions: str

    @field_validator("family_history", mode="before")
    @classmethod
    def transform_pop(cls, raw: str) -> bool:
        if raw in ['no','No','NO','Nej','nej','0']:
            return False
        elif raw in ['yes','Yes','Ja','ja','1']:
            return True
        else:
            return bool(raw)
    class Meta:
        validation_set = ['description']

class Severity(Enum):
    NOT_ASSESSED = 'not assessed'
    VARYING = 'varying'
    LOW = 'low'
    MODERATE = 'moderate'
    HIGH = 'high'
    VERY_HIGH = 'very high'

    #class Meta:
    #    validation_set = []

class Basic_PICO(OurBaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    
    #icd_code: constr(pattern='^([a-tA-T]|[v-zV-Z])\d[a-zA-Z0-9](\.[a-zA-Z0-9]{1,4})?$')
    severity: Severity
    population: Population
    incidence: str
    prevalence: str
    pediatric: bool
    co_medication: str
    intervention: str
    comparator_company: str #Product
    comparator_modus_company: str 
    comparator_reason_company: str
    outcome_measure_company: str
    comparator_agency: str #Product
    comparator_modus_agency: str
    comparator_reason_agency: str
    outcome_measure_agency: str    
    
    analysis: Basic_Analysis
    indication: Indication

    class Meta:
        validation_set = ['intervention']
   

class PICO(OurBaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    
    #icd_code: constr(pattern='^([a-tA-T]|[v-zV-Z])\d[a-zA-Z0-9](\.[a-zA-Z0-9]{1,4})?$')
    severity: Severity
    population: Population
    incidence: str
    prevalence: str
    pediatric: bool
    co_medication: str
    intervention: str
    comparator_company: str #Product
    comparator_modus_company: str 
    comparator_reason_company: str
    outcome_measure_company: str
    comparator_agency: str #Product
    comparator_modus_agency: str
    comparator_reason_agency: str
    outcome_measure_agency: str    
    
    analysis: Analysis
    indication: Indication

    class Meta:
        orm_model = sqlal_models.PICO
        validation_set = ['intervention']

class PICOs(OurBaseModel):
    picos: list[PICO]

    class Meta:
        validation_set = ['picos']

class PICO_Partial_Comp(OurBaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    
    indication: str
    population: Population
    incidence: str
    prevalence: str
    pediatric: bool
    co_medication: str
    intervention: str
    comparator_company: str #Product
    comparator_modus_company: str 
    comparator_reason_company: str
    outcome_measure_company: str   
    
    class Meta:
        validation_set = ['intervention']

class PICOs_comp(OurBaseModel):
    picos: list[PICO_Partial_Comp]

    class Meta:
        validation_set = ['picos']

class PICO_Partial_Ag(OurBaseModel, use_enum_values=True):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    
    indication: str
    severity: Severity
    population: Population
    incidence: str
    prevalence: str
    pediatric: bool
    co_medication: str
    intervention: str
    comparator_agency: str #Product
    comparator_modus_agency: str
    comparator_reason_agency: str
    outcome_measure_agency: str   

    class Meta:
        validation_set = ['intervention']
    

class PICOs_ag(OurBaseModel):
    picos: list[PICO_Partial_Ag]

    class Meta:
        validation_set = ['picos']

class PICO_Partial(OurBaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    
    #icd_code: str
    severity: Severity
    population: str
    incidence: str
    prevalence: str
    pediatric: bool
    co_medication: str
    intervention: str
    comparator: str 
    comparator_reason: str
    comparator_modus: str
    outcome_measure: str    
    
    analysis: Analysis
    indication: Indication

    class Meta:
        validation_set = ['intervention']

class PICO_Analysis_Cost_Company(OurBaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    #icd_code: str
    severity: Severity
    population: Population
    incidence: str
    prevalence: str
    pediatric: bool
    co_medication: str
    intervention: str
    comparator: str 
    comparator_reason: str
    comparator_modus: str
    outcome_measure: str    

    QALY_gain_company: str
   # QALY_total_cost_company: str
    ICER_company: str
    comparison_method: str
    indirect_method: str

    costs_company_product: Costs
    costs_company_comparator: Costs

    indication: Indication

    class Meta:
        validation_set = ['intervention']

class PICO_Analysis_Cost_Agency(OurBaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    #icd_code: str
    severity: Severity
    population: str
    co_medication: str
    intervention: str
    comparator: str 
    comparator_reason: str
    comparator_modus: str
    outcome_measure: str    

    QALY_gain_agency_lower: str
    QALY_gain_agency_higher: str
 #   QALY_total_cost_agency_lower: str
 #   QALY_total_cost_agency_higher: str
    ICER_agency_lower: str
    ICER_agency_higher: str

    efficacy_summary:str 
    safety_summary: str 
    decision_summary: str 
    uncertainty_assessment_clinical: str 
    uncertainty_assessment_he: str
    
    costs_agency_product: Costs
    costs_agency_comparator: Costs

    indication: Indication

    class Meta:
        validation_set = ['intervention']

class PICO_Partial_Company(PICO_Partial):
    analysis: Analysis_Company

    class Meta:
        validation_set = ['analyses']

class PICOs_Partial_Company(OurBaseModel):
    picos_company: list[PICO_Partial_Company]

    class Meta:
        validation_set = ['picos_company']

class PICO_Partial_Agency(PICO_Partial):
    analysis: Analysis_Agency

    class Meta:
        validation_set = ['analysis']

class PICOs_Partial_Agency(OurBaseModel):
    picos_agency: list[PICO_Partial_Agency]

    class Meta:
        validation_set = ['picos_agency']

class HE_results(OurBaseModel):
    picos: list[PICO]

    class Meta:
        validation_set = ['picos']
    

class Role(Enum):
    chairman = 'chairman'
    board_member = 'board member'
    presenter = 'presenter'
    non_voting_board_member = 'non-voting board member'
    assessor = 'assessor'

    #class Meta:
    #    validation_set = []

class HTA_Document_Staff(OurBaseModel):
    staff: Staff
    role: Role
    dissent: str

    class Meta:
        orm_model = sqlal_models.HTA_Document_Staff
        validation_set = ['staff']

class Team(OurBaseModel):
    staff: list[HTA_Document_Staff] 

    class Meta:
        validation_set = ['staff']

class Expert(OurBaseModel):
    first_name: str 
    last_name: str
    position: str

    class Meta:
        validation_set = []

class HTA_Document_Expert(OurBaseModel):
    expert: Expert

    class Meta:
        validation_set = []

class Panel(OurBaseModel):
    staff: list[Staff]
    experts: list[Expert]

    class Meta:
        validation_set = ['staff']

class Panel2(OurBaseModel):
    staff: list[HTA_Document_Staff] 
    experts: list[HTA_Document_Expert]

    class Meta:
        validation_set = ['staff']


class HTA_Document_Indication(OurBaseModel):
    indication: Indication
    severity: Severity

    class Meta:
        validation_set = ['indication']

class Currency(Enum):
    SEK = 'SEK'
    EUR = 'EUR'
    DKK = 'DKK'
    NOK = 'NOK'
    USD = 'USD'

   # class Meta:
   #     validation_set = []

class Decision(Enum):
    full = 'full'
    limited = 'limited'
    rejected = 'rejected'
    no_decision = 'no decision'

    #class Meta:
    #    validation_set = []

class Population_Cohort(OurBaseModel):
    population: str
    incidence: str
    prevalance: str

    class Meta:
        validation_set = []

class Indications_and_Population_Cohorts(OurBaseModel):
    indications: list[HTA_Document_Indication]
    population_cohorts: list[Population_Cohort]

    class Meta:
        validation_set = {'indications'}

class HTA_Document(OurBaseModel):
    title: str
    diarie_nr: constr(pattern=r'\d{1,4}\/20[0-2]\d')
    date: str
    decision: Decision
    limitations: str
    efficacy_summary: str
    safety_summary: str
    decision_summary: str
    currency: Currency
    analysis: Literal['cost-minimization', 'cost-effectiveness', 'cost-benefit', 'combination']
    requested_complement: bool
    requested_information: str
    requested_complement_submitted: bool
    previously_licensed_medicine: bool

    indications: list[HTA_Document_Indication]
    
    hta_agency: HTA_Agency
    products: conlist(Product, min_length=1)
    company: Company
    picos: list[Basic_PICO]
    staff: list[HTA_Document_Staff]
    population_cohorts: list[Population_Cohort]

    parsing_model: str = Field(description='The LLM model used to extract the information', max_length=45)
    
    class Meta:
        validation_set = {'diarie_nr'}
        orm_model = sqlal_models.HTA_Document

class HTA_Document_Basis(HTA_Document):
    application_type: str
    latest_decision_date: constr(pattern=r'\d{4}\-\d{2}\-\d{2}') 
    annual_turnover: str 
    three_part_deal: bool 
    comparators: list[Product]
    experts: list[HTA_Document_Expert]
    picos: list[PICO]
    uncertainty_assessment_clinical: str
    uncertainty_assessment_he: str

    references: list[HTA_Document_Reference]

    # @model_validator(mode='before')
    # @classmethod
    # def check_date_present(cls, data: Any) -> Any:  
    #     if isinstance(data, dict):  
    #         if data['date']=='':
    #             raise MissingDataException("empty date")
    #     return data

    class Meta:
        validation_set = {'date'}

class HTA_Document_Extend(BaseModel):
    diarie_nr: constr(pattern=r'\d{1,4}\/20[0-2]\d')
    application_type: str
    latest_decision_date: constr(pattern=r'\d{4}\-\d{2}\-\d{2}') 
    annual_turnover: str 
    three_part_deal: bool 
    comparators: list[Product]

 #   @field_validator('latest_decision_date', mode='before')
 #   @classmethod
 #   def check_date_present(cls, raw) -> str:  
 #       if raw=='':
 #               raise MissingDataException("empty latest decision date")
 #       return raw

    class Meta:
        validation_set = {'diarie_nr'}#{'comparators'} some ((old nt) documents do not contain comparators


class HTA_Document_NT_Extend(HTA_Document_Extend):
    title: str
    date: str
    products: conlist(Product, min_length=1)
    company: Company
    hta_agency: HTA_Agency

    class Meta:
        validation_set = ['products']


class HTA_Summary(BaseModel):
    analysis: str
    efficacy_summary: str
    safety_summary: str
    decision_summary: str
    uncertainty_assessment_clinical: str
    uncertainty_assessment_he: str
    limitations: str
    requested_complement: bool
    requested_information: str
    requested_complement_submitted: bool
    previously_licensed_medicine: bool

    class Meta:
        validation_set = {'decision_summary'} # May be missing. See intrarosa where the efficacy was not demonstrated
