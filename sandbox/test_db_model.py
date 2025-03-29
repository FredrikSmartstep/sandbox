
import worker
from datahandler3 import DataHandler
import json
data =  {
  "title": "Adcetris ingår i högkostnadsskyddet med begränsning",
  "company": "Takeda Pharma AB",
  "product": "Adcetris",
  "diarie_nr": "3230/2012",
  "date": "2013-06-20",
  "decision": "begränsad",
  "currency": "SEK",
  "requested_complement": "yes",
  "requested_information": "uppdaterad hälsoekonomisk analys för Adcetris inklusive fullständiga data från studierna SGN35-005 och SGN35-003",
  "requested_complement_submitted": "no",
  "indication": [
    {
      "name": "Hodgkins sjukdom",
      "severity": "very high"
    },
    {
      "name": "non-Hodgkin-lymfom",
      "severity": "very high"
    }
  ],
  "limitations": "Subventioneras som förbehandling för patienter som ska genomgå en allogen stamcellstransplantation om patienten förväntas behöva högst 6 behandlingscykler samt vid monoterapi.",
  "analysis_type": "cost-effectiveness",
  "form": [
    {
      "form": "Pulver till koncentrat till infusionsvätska, lösning",
      "strength": "50 mg",
      "AIP": "27 686,00",
      "AUP": "27 853,00"
    }
  ],
  "picos": [
    {
      "population": "Patienter med Hodgkins sjukdom",
      "incidence": "",
      "prevalance": "",
      "intervention": "Adcetris som monoterapi",
      "comparators_company": ["kemoterapi"],
      "comparator_modus_company": "monoterapi",
      "comparator_reason_company": "standard behandling",
      "comparators_TLV": ["kemoterapi"],
      "comparator_modus_TLV": "monoterapi",
      "comparator_reason_TLV": "standard behandling",
      "analyses": {
        "QALY_gain_company": "N/A",
        "QALY_total_cost_company": "470 000 kr",
        "QALY_gain_TLV_lower": "N/A",
        "QALY_gain_TLV_higher": "N/A",
        "QALY_total_cost_TLV_lower": "N/A",
        "QALY_total_cost_TLV_higher": "N/A",
        "QALY_cost_company": "N/A",
        "QALY_cost_TLV_lower": "N/A",
        "QALY_cost_TLV_higher": "N/A",
        "comparison_method": "direct",
        "indirect_method": "",
        "trials": [
          {
            "title_of_paper": "SGN35-005",
            "number_of_patients": "",
            "number_of_controls": "",
            "indication": "Hodgkins sjukdom",
            "duration": "",
            "phase": "III",
            "meta_analysis": "no",
            "randomized": "yes",
            "controlled": "yes",
            "blinded": "double",
            "primary_outcome_variable": "",
            "results": "",
            "safety": ""
          },
          {
            "title_of_paper": "SGN35-003",
            "number_of_patients": "",
            "number_of_controls": "",
            "indication": "non-Hodgkin-lymfom",
            "duration": "",
            "phase": "III",
            "meta_analysis": "no",
            "randomized": "yes",
            "controlled": "yes",
            "blinded": "double",
            "primary_outcome_variable": "",
            "results": "",
            "safety": ""
          }
        ],
        "drug_cost_company": ["Adcetris: 27 686,00", "kemoterapi: N/A"],
        "other_costs_company": ["N/A"],
        "total_treatment_cost_company": ["N/A"],
        "drug_cost_TLV": ["Adcetris: 27 686,00", "kemoterapi: N/A"],
        "other_costs_TLV": ["N/A"],
        "total_treatment_cost_TLV": ["N/A"]
      }
    }
  ],
  "efficacy_summary": "TLV bedömer att Adcetris är kostnadseffektivt för patienter med Hodgkins sjukdom och non-Hodgkin-lymfom.",
  "safety_summary": "TLV anser att säkerhetsprofilen för Adcetris är acceptabel med beaktande av den höga svårighetsgraden av sjukdomarna.",
  "decision_summary": "TLV beslutar att Adcetris ska ingå i läkemedelsförmånerna med begränsningar för specifika patientgrupper.",
  "decision_makers": [
    {
      "name": "Namn på beslutsfattare",
      "profession": "Ekonom"
    }
  ],
  "presenter_to_the_board": {
    "name": "Namn på presentatör",
    "profession": "Läkare"
  },
  "other_participants": [
    {
      "name": "Namn på ytterligare deltagare",
      "profession": "Ekonom"
    }
  ]
}
file_dir_out = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/decisions2/'
k=0
with open(file_dir_out + "sample_" + str(k)+ ".json", "w") as outfile:
            json.dump(data, outfile)

with  open(file_dir_out + "sample_" + str(k)+ ".json") as f:
                data2 = json.load(f)
dh = DataHandler()

dh.insert_HTA(data2)