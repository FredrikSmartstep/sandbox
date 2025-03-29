from pydantic import BaseModel
from typing import List

class Cohort(BaseModel):
     cohort: str
     incidence: str
     prevalence: str

class Form(BaseModel):
    form: str
    strength: str
    AIP: float
    AUP: float

class Trial(BaseModel):
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

class ProductCost(BaseModel):
    product: str 
    cost: float

class HEResult(BaseModel):
    population: str 
    intervention: str
    comparators_company: str 
    comparator_modus_company: str 
    comparator_reason_company: str 
    comparators_TLV: str 
    comparator_modus_TLV: str 
    comparator_reason_TLV: str 
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
    trials_company: List[Trial]
    drug_costs_company: List[ProductCost]
    other_costs_company: List[ProductCost]
    total_costs_company: List[ProductCost]
    drug_costs_TLV: List[ProductCost]
    other_costs_TLV: List[ProductCost]
    total_costs_TLV: List[ProductCost]

class Indication(BaseModel):
    indication: str
    severity: str
    limitation: List[str]
    analysis: str
    form: List[Form]

class Person(BaseModel):
    name: str 
    profession: str 

class Document(BaseModel):
    title: str
    applicant: str
    product_name: str
    diarie_number: str
    date: str
    decision: str
    currency: str
    requested_complement: bool
    requested_information: str
    requested_complement_submitted: bool
    cohorts: List[Cohort]
    HE_results: List[HEResult]
    efficacy_summary: str
    safety_summary: str
    decision_summary: str
    decision_makers: List[Person]
    presenter_to_the_board: Person
    other_participants: List[Person]
