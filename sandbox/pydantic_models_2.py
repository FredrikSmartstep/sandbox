from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, constr, conlist, field_validator, model_validator, ValidationError, ValidatorFunctionWrapHandler
import sandbox.sqlalchemy_models360 as sqlal_models
from enum import Enum
from typing import Any, Literal, Optional
import re


class OurBaseModel(BaseModel):
    class Config:
        validate_assignment = True
        validate_default = True
        use_enum_values = True

# -------------------------------
class Company(OurBaseModel):
    name: str = Field(description="Name of the company filing the reimbursement application", max_length=100)
    class Meta:
        orm_model = sqlal_models.Company 

class Product(OurBaseModel):
    name: str = Field(description="Name of the product the dossier is all about", max_length=100)

    class Meta:
        orm_model = sqlal_models.Product


class Indication(OurBaseModel):
    indication: str  = Field(description="(The name of the medical indication", max_length=300)
    icd10_code: Optional[constr(pattern='^([a-tA-T]|[u-zU-Z0-9])\d[a-zA-Z0-9](\.[a-zA-Z0-9]{1,4})?')] = Field(description="The ICD-10 code.")


class Reference(OurBaseModel):
    authors: Optional[str] = Field(default=None, description="The authors of the publications", max_length=500)
    title: str = Field(description="The title of the publications", max_length=500)
    journal: Optional[str] = Field(default=None, description="The journal the paper was published in", max_length=200)
    vol: Optional[str] = Field(default=None, description="The volume of the journal", max_length=45)
    pages: Optional[str] = Field(default=None, description="Pages in the journal", max_length=45)
    month: Optional[str]= Field(default=None, description="Publication month", max_length=45)
    year: Optional[str] = Field(default=None, description="Publication year", max_length=45)
    url: Optional[str] = Field(default=None, description="An url to the publication", max_length=500)


class HTA_Document_Reference(OurBaseModel):
    reference: Reference

    class Meta:
        orm_model = sqlal_models.HTA_Document_Reference

class References(OurBaseModel):
    references: list[HTA_Document_Reference]


class Staff(OurBaseModel):
    profession: Literal['other', 'analyst','associate professor', 'board chairman',  'board member', 'chief pharmacist', 'chief legal officer', 'chief medical officer', 'chief research officer', \
                       'clinical researcher', 'coordinator', 'country coordinator', 'county councilor', 'county director', 'director general', 'director of advocacy', 'department manager', \
                        'engineer', 'former chief pharmacist', 'former union chairman', 'union chairman', 'former county director', 'health economist', 'Health and Medical Care Director', \
                         'investigator', 'lawyer', 'medical advisor', 'medical investigator', 'medical doctor', 'professor', 'reader', 'statistician'] #Profession
    name: str = Field(description="The full name of this staff member.", max_length=45)


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

class Agency(Enum):
    TLV='TLV'


class HTA_Agency(OurBaseModel):
    name: Agency = Field(description="(The HTA agency that is responsible for the health technology assessment.", max_length=45)
    staff: list[Staff] = Field(description="The staff from the agency that has been working on this dossier.")


    class Meta:
        orm_model = sqlal_models.HTA_Agency

class Costs(OurBaseModel):

    assessor: Literal['company','agency'] = Field(description="The entity that made this cost assessment.")
    product: Literal['product','comparator'] = Field(description="Whether the costs relate to the evaluated product or the comparator.")
    drug_cost: Optional[float] = Field(description="The drug cost. If the analysis type was cost minimization, this information is not optional.")
    other_costs: Optional[float]  = Field(description="Other costs that relate to the treatment. If the analysis type was cost minimization, this information is not optional and should be set to zero if not found.")
    total_treatment_cost: Optional[float]  = Field(description="The total treatment cost. If the analysis type was cost minimization, this information is not optional and can be expected to be the sum of the drug cost and the other costs.")
    cost_type: str = Field(description="What the cost relate to, e.g., per treatment period, per package, etc", max_length=200)

    class Meta:
        orm_model = sqlal_models.Costs

class Outcome_Measure(OurBaseModel):
    name: Optional[str] = Field(default=None, description="The name of the variable. Only one variable. Separate into separate rows in the table if more than one.", max_length=100)
    units: Optional[str] = Field(default=None, description="The quantitative unit of the variable", max_length=45)

    class Meta:
        orm_model = sqlal_models.Outcome_Measure

class Outcome_Value(OurBaseModel):
   # model_config = ConfigDict(coerce_numbers_to_str=True)

    trial_arm: Optional[str] = Field(description="Which arm the results relate to.", max_length=200)
    value: Optional[str] = Field(description="value achieved for the primary outcome variable.", max_length=300) 
    significance_level: Optional[str] = Field(description="compared to the control arm.", max_length=45) 
    outcome_measure: Outcome_Measure

    class Meta:
        orm_model = sqlal_models.Outcome_Value

class Trial(OurBaseModel):
    title: Optional[str] = Field(description="Title of this trial.", max_length=400) 
    summary: Optional[str] = Field(description="a brief summary in at most four sentences of the research.", max_length=1000) 
    nr_of_patients: Optional[int] = Field(description="number of patients in the trial.") 
    nr_of_controls: Optional[int] = Field(description="(number of patients in control arm, if applicable.") 
    indication: Optional[str] = Field(description="medical indication of the patients.", max_length=100) 
    duration: Optional[str] = Field(description="the duration of the trial.", max_length=150) 
    phase: Optional[str] = Field(description="phase I, II, III or IV.", max_length=45) 
    meta_analysis: Optional[bool] = Field(description="If this was a meta-analysis.") 
    randomized: Optional[bool] = Field(description="If it was randomized") 
    controlled: Optional[bool] = Field(description="If a control arm was used.") 
    type_of_control: str = Field(description="placebo/no treatment/active treatment/dose comparison/historical control.", max_length=45) 
    design: str = Field(description="equivalence/noninferiority/superiority.", max_length=45) 
    objective: str = Field(description="the intended purpose of the trial, efficacy/safety/both efficacy and safety.", max_length=45) 
    blinded: Optional[str] = Field(description="single, double, no.", max_length=45) 
    primary_outcome_variable: Optional[str] = Field(description="sometimes referred to as 'effektmått'.", max_length=100) 
    reference: Optional[str] = Field(description="full reference to the paper.", max_length=500) 
    url: Optional[str] = Field(description="an url address to the paper if possible.", max_length=500) 
    outcome_values: list[Outcome_Value] = Field(description="For each trial arm include the results for all investigated outcome variables or key metrics. Focus on quantitative results.") 
    safety: Optional[str] = Field(description="description of adverse events, side effects in all arms.", max_length=300) 

    class Meta:
        orm_model = sqlal_models.Trial

class Trials(OurBaseModel):
    trials: conlist(item_type=Trial, min_length=1)


class Basic_Analysis(OurBaseModel):
    analysis_type: Literal['cost-minimization', 'cost-effectiveness', 'cost-benefit'] = Field(description="cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys)")
    QALY_gain_company: Optional[float] = Field(description="The number of gained quality-adjusted life years (QALY) as calculated by the company, if applicable.") 
    QALY_gain_agency_lower: Optional[float] = Field(description="The lower number of gained quality-adjusted life years (QALY) as calculated by TLV, if applicable.") 
    QALY_gain_agency_higher: Optional[float] = Field(description="The upper number of gained quality-adjusted life years (QALY) as calculated by TLV, if applicable.") 
    ICER_company: Optional[float] = Field(description="The incremental cost of a quality-adjusted life year (QALY) compared to the comparator as calculated by the company, if applicable. Look for 'kostnad per kvalitetsjusterat år'.") 
    ICER_agency_lower: Optional[float] = Field(description="The lower incremental cost of a quality-adjusted life year (QALY) as calculated by the TLV, if applicable. Look for  'kostnad per kvalitetsjusterat år'.") 
    ICER_agency_higher: Optional[float] = Field(description="The lower incremental cost of a quality-adjusted life year (QALY) as calculated by the TLV, if applicable. Look for  'kostnad per kvalitetsjusterat år'.") 
    comparison_method: Literal["direct", "indirect"] = Field(description= "if the statistical comparison between the drug and the comparators was made using a direct or indirect method.") 
    indirect_method: Optional[str] = Field(description="For example Bayesian network meta-analysis, if applicable.", max_length=300) 
    efficacy_summary:str = Field(description="A brief summary, no longer than three sentences, of TLV:s assessment of the product's efficacy for this pico.", max_length=1000) 
    safety_summary: str = Field(description="A brief summary, no longer than three sentences, of TLV:s assessment of the product's safety profile for this pico.", max_length=1000) 
    decision_summary: str = Field(description="A brief summary, no longer than three sentences, of TLV:s reasons for their decision for this pico.", max_length=1000) 
    uncertainty_assessment_clinical: str = Field(description="low/medium/high/very high/not assessed. How TLV assess the uncertainty of the clinical results presented by the company for this pico.", max_length=45) 
    uncertainty_assessment_he: str = Field(description="low/medium/high/very high/not assessed. How TLV assess the uncertainty of the health economic results presented by the company for this pico.", max_length=45) 


class Analysis(Basic_Analysis):

    costs: conlist(item_type=Costs, min_length=2) = Field(description="The costs associated with the treatment.")
    trials: conlist(item_type=Trial, min_length=0) = Field(description="The trials that were referenced in the analysis.") # there are actually dossiers without trial data

    class Meta:
        orm_model = sqlal_models.Analysis

class Analysis_List(OurBaseModel):
    analyses: list[Analysis]


class Demographics(OurBaseModel):
    pediatric: bool = Field(description="Whether the population cohort includes pediatric patients.")
    adolescent: bool = Field(description="Whether the population cohort includes adolescent patients.")
    adult: bool = Field(description="Whether the population cohort covers adult patients.")
    elderly: bool = Field(description="Whether the population cohort includes elderly patients.")
    gender: Literal['Male', 'Female', 'All'] = Field(description="If no information is provided, 'All' is assumed.")

class Population(OurBaseModel):
    description: str = Field(description="Description of the identified population covering the associated indication. This description should be short and information covered by the other columns in this table does not have to be described here as well.", max_length=500)  
    demographics: Demographics = Field(description="The demographics of this population.")
    ethnic_considerations: Optional[str] = Field(description="Description of ethnic aspects, if relevant.", max_length=200)
    genetic_factors: Optional[str] = Field(description="Any genetic factors that characterize the population.", max_length=200)
    family_history: Optional[bool] = Field(description="If inheritance is present.")
    sub_diagnosis: Optional[str] = Field(description="name of sub-diagnosis if relevant", max_length=100)
    disease_stage: Optional[str] = Field(description="disease progression stages 1-5 like so, stage 1 mild/early, stage 2 moderate/stable, stage 3 severe/progressive, stage 4 very severe/advanced, stage 5 terminal/end-stage", max_length=100)
    biomarker_status: Optional[str] = Field(description="Any biomarker status that are criteria for being included in this population.", max_length=100)
    co_morbidities: Optional[str] = Field(description="Names of comorbidities that define this population.", max_length=100)
    previous_treatment: Optional[str] = Field(description="Any previous treatment that have been employed in this cohort.", max_length=300)
    response_to_treatment: Optional[str] = Field(description="complete response CR, partial response PR, stable disease SD and progressive disease PD", max_length=200)
    lifestyle_factors: Optional[str] = Field(description="Relevant life style factors present in this population.", max_length=100)
    psychosocial_factors: Optional[str] = Field(description="Relevant psychosocial conditions present in this population.", max_length=100)
    special_conditions: Optional[str] = Field(description="Other conditions.", max_length=300)

    @field_validator('sub_diagnosis','disease_stage', 'co_morbidities', mode='wrap')
    @classmethod
    def truncate(cls, value: Any, handler: ValidatorFunctionWrapHandler) -> str:
        try:
            return handler(value)
        except ValidationError as err:
            if err.errors()[0]['type'] == 'string_too_long':
                return handler(value[:99])
            else:
                raise

class Severity(Enum):
    NOT_ASSESSED = 'not assessed'
    VARYING = 'varying'
    LOW = 'low'
    MODERATE = 'moderate'
    HIGH = 'high'
    VERY_HIGH = 'very high'


class Basic_PICO(OurBaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    
    severity: Severity = Field(description="The assessed severity of the condition. The severity assessment is usually in a sentence like 'TLV bedömer (?:att )?svårighetsgraden'.")
    population: Population = Field(description="The population cohort")
    incidence: Optional[str] = Field(description="Estimated incidence in Sweden.", max_length=200)
    prevalence: Optional[str] = Field(description="Estimated prevalence in Sweden.", max_length=300)
    pediatric: bool = Field(description="If the population cohort covers pediatric patients")
    co_medication: str = Field(description="Name of other product that was used in combination with the evalauted product.", max_length=300)
    intervention: str = Field(description="Short description of the evaluated treatment.", max_length=200)
    comparator_company: str = Field(description="Name of the product that the company has compared the product against.", max_length=200) #Product 
    comparator_modus_company: str = Field(description="Treatment regime used in the comparison by the company.", max_length=150)
    comparator_reason_company: str = Field(description="Reason provided by the company for using these comparators.", max_length=400)
    outcome_measure_company: str = Field(description="(Outcome measure used by the company in clinical trials related to this pico.", max_length=300)
    comparator_agency: str = Field(description="Name of the product that the TLV has compared the product against.", max_length=200) #Product
    comparator_modus_agency: str = Field(description="Treatment regime used in the comparison by TLV.", max_length=150)
    comparator_reason_agency: str = Field(description="Reason provided by TLV for using these comparators.", max_length=400)
    outcome_measure_agency: str = Field(description="(Outcome measure used by the TLV.", max_length=300)    
    
    indication: Indication = Field(description="The indication in this pico combination.")
   

class PICO(Basic_PICO):
    
    analysis: Analysis

    class Meta:
        orm_model = sqlal_models.PICO


class Basic_PICOs(OurBaseModel):
    picos: list[Basic_PICO]


class PICOs(OurBaseModel):
    picos: list[PICO]
    

class Role(Enum):
    chairman = 'chairman'
    board_member = 'board member'
    presenter = 'presenter'
    non_voting_board_member = 'non-voting board member'
    assessor = 'assessor'

class HTA_Document_Staff(OurBaseModel):
    staff: Staff
    role: Role
    dissent: Optional[str] = Field(default= None, description="If the staff member expressed dissent to the decision and the objections advocated.", max_length=300)

    class Meta:
        orm_model = sqlal_models.HTA_Document_Staff


class Expert(OurBaseModel):
    first_name: str = Field(max_length=45)
    last_name: str = Field(max_length=45)
    position: str = Field(max_length=100)

class HTA_Document_Expert(OurBaseModel):
    expert: Expert


class HTA_Document_Indication(OurBaseModel):
    indication: Indication 
    severity: Severity = Field(description="The assessed severity of the indication. The severity assessment is usually found in the sub section 'TLV gör följande bedömning'.")

class Currency(Enum):
    SEK = 'SEK'
    EUR = 'EUR'
    DKK = 'DKK'
    NOK = 'NOK'
    USD = 'USD'

class Decision(Enum):
    full = 'full'
    limited = 'limited'
    rejected = 'rejected'
    no_decision = 'no decision'

class Population_Cohort(OurBaseModel):
    population: str = Field(description='A short description of the population.', max_length=100)
    incidence: str = Field(description='Estimated incidence.', max_length=200)
    prevalance: str = Field(description='Estimated prevalence.', max_length=300)

class Indications_and_Population_Cohorts(OurBaseModel):
    indications: list[HTA_Document_Indication]
    population_cohorts: list[Population_Cohort]

    class Meta:
        validation_set = {'indications'}

class HTA_Document_Base(OurBaseModel):
    title: str = Field(description='The title of the document. Found in the meta data of the pdf file or on the front page.', max_length=150)
    diarie_nr: constr(pattern=r'\d{1,4}\/20[0-2]\d') = Field(description="diarienummer or sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning'")
    date: str = Field(description='Format YYYY-MM-DD, usually on the front page')
    decision: Decision = Field(description='fully reimbursed/limited reimbursement/rejected')
    limitations: str = Field(description='Limitations that applies to the reimbursement. May be none)', max_length=1000)
    efficacy_summary: str = Field(description="A brief summary no longer than three sentences of TLV:s assessment of the product's efficacy", max_length=1000)
    safety_summary: str = Field(description="A brief summary no longer than three sentences of TLV:s assessment of the product's safety profile", max_length=1000)
    decision_summary: str = Field(description="A brief summary no longer than three sentences of TLV:s reasons for their decision or health economic assessment", max_length=1000)
    currency: Currency = Field(description="The currency used in the dossier")
    analysis: Literal['cost-minimization', 'cost-effectiveness', 'cost-benefit', 'combination'] = Field(description="cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys)")
    requested_complement: bool = Field(description="If TLV requested the company to complement with additional information")
    requested_information: str = Field(description="(what type of information TLV requested the company to complement the application with if applicable", max_length=400)
    requested_complement_submitted: bool = Field(description="If applicable, whether the company submitted the requested complementary info.")
    previously_licensed_medicine: bool = Field(description=" Whether the active ingredient or the drug previously was a licensed medicine")

    hta_agency: HTA_Agency = Field(description="The name of the agency evaluating the product")
    products: conlist(Product, min_length=1) = Field(description='The products that the application concerns.')
    company: Company = Field(description='Name of applicant. Only company name')
    
    indications: list[HTA_Document_Indication] = Field(description="List of medical indications the medicinal product is evaluated for. The indication information is usually found in section 'Ansökan' and/or in the section 'Utredning i ärendet'")

    parsing_model: str = Field(description='The LLM model used to extract the information', max_length=45)

class HTA_Document(HTA_Document_Base):

    #picos: list[Basic_PICO] 
    staff: list[HTA_Document_Staff]
    population_cohorts: list[Population_Cohort] = Field(description="List of the population cohorts the medicinal product is evaluated for.")


    class Meta:
        validation_set = {'diarie_nr'}
        orm_model = sqlal_models.HTA_Document

class HTA_Document_Preamble(OurBaseModel): 
    diarie_nr: constr(pattern=r'\d{1,4}\/20[0-2]\d') = Field(description="diarienummer or sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning'.")
    application_type: Literal['nyansökan', 'ny indikation', 'nytt läkemedel', 'prisändring', 'ny beredningsform', 'redovisning av uppföljningsvillkor', 'begränsad utvärdering'] = Field(description="Usually found in a table on page 2 or 3 (sv 'Typ av ansökan')")
    latest_decision_date: datetime = Field(description="sv. 'Sista beslutsdag'. Usually found in a table on page 2 or 3.")
    annual_turnover: Optional[str] = Field(description="estimated annual turnover. Usually found in a table on page 2 or 3", max_length=200)
    three_part_deal: bool = Field(description="Whether a three-part negotiation (sv. 'treparts' or 'sidoöverenskommelse') took place")
    comparators: list[Product] = Field(description="List of product to compare against, comparators, sv. 'Relevant jämförelsealternativ'. Usually found in a table on page 2 or 3. Also look in the section 'TLV:s bedömning och sammanfattning'.")
    experts: list[HTA_Document_Expert] = Field(description="List of possible engaged experts. usually found at the bottom of page 2 or 3 following the word 'Klinisk expert'")
    uncertainty_assessment_clinical: str = Field(description="how TLV assess the uncertainty of the clinical results presented by the company (low,/medium/high/ery high/not assessed))")
    uncertainty_assessment_he: str = Field(description="how TLV assess the uncertainty of the health economic results presented by the company (low/medium/high/very high/not assessed))")

    staff: list[HTA_Document_Staff]

class HTA_Document_Basis(HTA_Document):
    application_type: Literal['nyansökan', 'ny indikation', 'nytt läkemedel', 'prisändring', 'ny beredningsform', 'redovisning av uppföljningsvillkor', 'begränsad utvärdering'] = Field(description="Usually found in a table on page 2 or 3")
    latest_decision_date: constr(pattern=r'\d{4}\-\d{2}\-\d{2}') = Field(description="sv. 'Sista beslutsdag'. Usually found in a table on page 2 or 3.")
    annual_turnover: Optional[str] = Field(description="estimated annual turnover. Usually found in a table on page 2 or 3", max_length=200)
    three_part_deal: bool = Field(description="Whether a three-part negotiation (sv. 'treparts' or 'sidoöverenskommelse') took place")
    comparators: list[Product]
    experts: list[HTA_Document_Expert]
    picos: list[PICO]
    uncertainty_assessment_clinical: str = Field(description="how TLV assess the uncertainty of the clinical results presented by the company (low,/medium/high/ery high/not assessed))")
    uncertainty_assessment_he: str = Field(description="how TLV assess the uncertainty of the health economic results presented by the company (low/medium/high/very high/not assessed))")

    references: Optional[list[HTA_Document_Reference]]

    class Meta:
        validation_set = {'date'}


class HTA_Document_NT(OurBaseModel):
    title: str = Field(description='The title of the document. Found in the meta data of the pdf file or on the front page.', max_length=150)
    diarie_nr: constr(pattern=r'\d{1,4}\/20[0-2]\d') = Field(description="diarienummer or sometimes abbreviated as 'Dnr' followed by the number formatted as nnnn/YYYY. Also look for swedish 'vår beteckning'")
    date: str = Field(description='Format YYYY-MM-DD, usually on the front page')
    limitations: str = Field(description='Limitations that applies to the assessment results. May be none)', max_length=1000)
    efficacy_summary: str = Field(description="A brief summary no longer than three sentences of TLV:s assessment of the product's efficacy", max_length=1000)
    safety_summary: str = Field(description="A brief summary no longer than three sentences of TLV:s assessment of the product's safety profile", max_length=1000)
    decision_summary: str = Field(description="A brief summary no longer than three sentences of TLV:s health economic assessment", max_length=1000)
    currency: Currency = Field(description="The currency used in the dossier")
    analysis: Literal['cost-minimization', 'cost-effectiveness', 'cost-benefit', 'combination'] = Field(description="cost-effectiveness (cost per QALY is referred to in the text), cost-minimization (sv: kostnadsjämförelse) or cost-benefit analysis (sv: kostnadsnyttoanalys)")
    requested_complement: bool = Field(description="If TLV requested the company to complement with additional information")
    requested_information: str = Field(description="(what type of information TLV requested the company to complement the application with if applicable", max_length=400)
    requested_complement_submitted: bool = Field(description="If applicable, whether the company submitted the requested complementary info.")
    previously_licensed_medicine: bool = Field(description=" Whether the active ingredient or the drug previously was a licensed medicine")
    comparators: list[Product] = Field(description="List of product to compare against, comparators, sv. 'Relevant jämförelsealternativ'. Usually found in a table on page 2 or 3. Also look in the section 'TLV:s bedömning och sammanfattning'.")
    experts: list[HTA_Document_Expert] = Field(description="List of possible engaged experts. usually found at the bottom of page 2 or 3 following the word 'Klinisk expert'")
    uncertainty_assessment_clinical: str = Field(description="how TLV assess the uncertainty of the clinical results presented by the company (low,/medium/high/ery high/not assessed))")
    uncertainty_assessment_he: str = Field(description="how TLV assess the uncertainty of the health economic results presented by the company (low/medium/high/very high/not assessed))")

    staff: list[HTA_Document_Staff]

    hta_agency: HTA_Agency = Field(description="The name of the agency evaluating the product")
    products: conlist(Product, min_length=1) = Field(description='The products that the document concerns.')
    company: Company = Field(description='Name of manufacturer of the product. Only company name')
    
    indications: list[HTA_Document_Indication] = Field(description="List of medical indications the medicinal product is evaluated for. The indication information is usually found in section 'Ansökan' and/or in the section 'Utredning i ärendet'")

    parsing_model: str = Field(description='The LLM model used to extract the information', max_length=45)

class Dossier(HTA_Document, HTA_Document_Preamble):
    references: Optional[list[HTA_Document_Reference]]
    picos: PICOs