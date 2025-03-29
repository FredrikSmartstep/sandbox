import openai
from openai import OpenAI
import pandas as pd
import time
import re
from secret import secrets
from logger_tt import getLogger

log = getLogger(__name__)


client = OpenAI(api_key=secrets.secrets.open_ai_key, timeout=60.0)
#https://chatgpt.com/g/g-xjGRu8osa-hta-agent
def get_response(messages): 
# "Present a summary of the products of {}, including medical indications (also ICD code), product names and development stage (if appropriate). List all products and medical indications. Specifically investigate their website for info: {}".format(name, url)
    # chat completion without streaming
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.5,
            max_tokens=1000,
            top_p=1
        )
    except openai.APIConnectionError as e:
        print("The server could not be reached")
        print(e.__cause__)  # an underlying Exception, likely raised within httpx.
        return 'Could not get data'
    except openai.RateLimitError as e:
        print("A 429 status code was received; we should back off a bit.")
        time.sleep(5)
        return 'Could not get data'
    except openai.APIStatusError as e:
        print("Another non-200-range status code was received")
        print(e.status_code)
        print(e.response)
        return 'Could not get data'
    return response.choices[0].message.content

def get_doc_summary_json():
    return  [
    {
      "role": "user",
      "content": 
        """Please extract title, company name, product name, diarie number,
        application_type (new drug, new form, new strength, etc),
        three_part (yes/no, if a three part negation was conducted),
        last_decision_date (format YYYYMMDD),
        indication (list of medical indication the medicinal product is evaluated for and the associated severity of that indication), 
        limitation (list of limitations, if any, that applies to the reimbursement),
        comparators_company (list of name(s) of the product(s) that the company has compared the product against), 
        comparator_modus_company (treatment regime used in the comparison by the company),
        comparator_modus_TLV (treatment regime used in the comparison by TLV),
        comparators_TLV (list of name(s) of the product(s) that the TLV has compared the product against), 
        analysis (cost-effectiveness, cost-minimization or cost benefit analysis), 
        form (list of form, strength, AIP and AUP of the medicinal product),
        population_cohorts (list of population cohorts and their incidence and prevalance),
        QALY_cost_company (the cost of a quality-adjusted life year as calculated by the company, if applicable),
        QALY_cost_TLV_lower (the lower cost of a quality-adjusted life year as calculated by the TLV, if applicable),
        QALY_cost_TLV_higher (the higher cost of a quality-adjusted life year as calculated by the TLV, if applicable),
        decision (fully reimbursed, limited reimbursement, rejected), 
        comparison_method (direct/indirect, if the statistical comparison between the drug and the comparators 
        was made using a direct or indirect method),
        indirect_method (for example Bayesian network meta-analysis, if applicable),
        trials (list of all referred trials: 
                        title of paper (if applicable), 
                        number_of_patients (number of patients in the trial), 
                        number_of_controls (number of patients in control arm, if applicable),
                        indication (medical indication of the patients), 
                        duration,
                        phase (phase I, II, III or IV)
                        meta-analysis (yes/no),
                        randomized (yes/no),
                        controlled (yes/no),
                        blinded (single, double, no),
                        primary_outcome_variable,
                        results (list of outcome variables in all arms and if significance was achieved with p-value),
                        safety (list of adverse events, side effects in all arms) 
                ), 
        drug_cost_company (list of drug costs for the product and the comparators according to the company),
        other_costs_company (list of other associated costs for the product and the comparators according to the company),
        total_treatment_cost_company (list total cost for the product and the comparators according to the company),
        drug_cost_TLV (list of drug costs for the product and the comparators according to TLV),
        other_costs_TLV (list of other associated costs for the product and the comparators according to TLV),
        total_treatment_cost_TLV (list total cost for the product and the comparators according to TLV),
        and 
        decision_makers (list of names of decision makers and their profession and possible dissent on the decision)
        from this document in a plain JSON format without any Markdown or code block formatting""",
      # Attach the new file to the message.
      "attachments": [
        { "file_id": message_file.id, "tools": [{"type": "file_search"}] }
      ],
    }
  ]

def get_product_info(name, url):
    return [
        {
            "role": "system",
            "content": (
                "You are a professional life science expert and you need to search the web for information to present summarized information."
            ),
        },
        {
            "role": "user",
            "content": (
                "Present a summary of the products of {}, including medical indications (also ICD code), product names and development stage (if appropriate). List all products and medical indications. Specifically investigate their website for info: {}".format(name, url)
            ),
        },
    ]

def translate_spec(description):
    return [
        {
            "role": "system",
            "content": (
                "Translate the swedish text to english."
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def severity_spec(description):
    return [
        {
            "role": "system",
            "content": (
                "Extract the estimated severity (very high, high, moderate, low, varying, not assessed) of the medical indication from the text you are presented. Be concise and only present the severity (very high, high, moderate, low, varying, not assessed). medical indication(s)."
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def base_indication_ind(description):
    return [
        {
            "role": "system",
            "content": (
                "Classify what type of medical indication (base ICD code) the text you are presented with likely is related to. The text is in Swedish. Be concise and only present the medical indication(s) in English and the ICD code as: <name of medical indication> (ICD code). Separate the conditions by a pipe (|)"
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def base_indication_ind_en(description):
    return [
        {
            "role": "system",
            "content": (
                "Classify what type of medical indication (base ICD code) the text you are presented with likely is related to. Be concise and only present the medical indication(s) in English and the ICD code as: <name of medical indication> (ICD code). Separate the conditions by a pipe (|)"
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def get_ICD_code(description):
    return [
        {
            "role": "system",
            "content": (
                "Get the ICD10 code for this medical indication. Be concise and only present the ICD code."
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]


def get_ICD_codes(description):
    return [
        {
            "role": "system",
            "content": (
                "Get the ICD10 code for the medical indications described in the text. Be concise and only present the ICD codes separated by a pipe (|)."
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def extract_document_data(file):
    functions = [
        {
            "name": "extract_info",
            "description": "Extracts useful information from a text document",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {
                        "type":"string",
                        "description": "Title of the document",
                    },
                    "diarie": {
                        "type":"string",
                        "description": "The diarie number of the document which has the format nnnn/YYYY",
                    }
                }
            }
        }
    ]
    return [
        {
            "role": "system",
            "content": ( 
                """You will be presented a document in Swedish describing the reasons for a reimburement decision for a medicinal product.
                Act as a professional health economist and provide the following information in a plain JSON format without any Markdown or code block formatting. 
                {
                diarie_nr:<>, 
                applicant: <Name of the company applying>, 
                product: <List of name(s) of the product(s) that the application relates to>, 
                indication: <Medical indication the medicinal product is indicated for>,
                limitation: <Limitations, if any, that applies to the reimbursement>,
                form: {}
                comparator: <List of name(s) of the product(s) that the application product has been compared against>,
                analysis: <Cost-effectiveness, cost-minimization or cost benefit analysis>},
                evaluator: """
                ),
        },
        {
            "role": "user",
            "content": (file),
        },
    ]

def extract_comparators(description):
    return [
        {
            "role": "system",
            "content": (
                "Extract what treatment the TLV agency considers the appropriate comparators to be from the text you are presented with. The text is in Swedish. Be concise and only present the treatment or drug in English. Separate the treatments (if multiple) by a pipe (|)"
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def extract_comparators_ATC(description):
    return [
        {
            "role": "system",
            "content": (
                "The text you will be presented sometimes contains names of drug brand names or generic names of active drugs. Find the associated ATC codes. Separate the codes (if multiple) by a pipe (|). If no ATC codes can be associated, return an empty response."
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def extract_comparators_names(description):
    return [
        {
            "role": "system",
            "content": (
                "The text you will be presented sometimes contains names of drug brand names or generic names of active drugs. Extract these names. Separate the names (if multiple) by a pipe (|). If no names, return an empty response."
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def classify_application_type(description):
    return [
        {
            "role": "system",
            "content": (
                "You will be presented a text (in Swedish) which relates to an application for reimbursement for a drug or medical device. Extract what type of application (new product, new indication, new form, new strength, price change, withdrawal) the text relates to. Be concise and only present the application type in English. If multiple types apply, separate them by a pipe (|)."
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def extract_experts(description):
    return [
        {
            "role": "system",
            "content": (
                "Extract the names and possibly titles and affiliation of the experts from the text you are presented with. The text is in Swedish. Present the experts with first name, last name, title, affiliation (sometimes missing). Separate the experts (if multiple) by a pipe (|)"
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def classify_industry(url):
    return [
        {
            "role": "system",
            "content": (
                "Classify what industry this company belongs to (Biotechnology, Pharmaceuticals, Medical Devices, Other)."
            ),
        },
        {
            "role": "user",
            "content": (url),
        },
    ]

def ATC_spec(description):
    return [
        {
            "role": "system",
            "content": (
                "What is the ATC code of this drug? Be concise and only respond with the code."
            ),
        },
        {
            "role": "user",
            "content": (description),
        },
    ]

def classify_company_independece(url):
    return [
        {
            "role": "system",
            "content": (
                "Classify if the company is an independant company, a parent company, a subsidiary or a joint venture. Kepp it concise and only respond with either independant, parent, subsidiary or joint venture"
            ),
        },
        {
            "role": "user",
            "content": 'Company: {}'.format(url),
        },
    ]

def find_subsidiaries(name):
    return [
        {
            "role": "system",
            "content": (
                "Find (if any) subsidiaries in the Nordic region for this company, registered in Sweden (AB) or Denmark (AS). Respond with company name and URL, separated by a pipe (|). If none found, leave an empty response."
            ),
        },
        {
            "role": "user",
            "content": 'Company: {}'.format(name),
        },
    ]

def search_linkedin(url, linkedin):
    return [
        {
            "role": "system",
            "content": (
                "Act as a professional recruiter within the life science industry. Search LinkedIn after people working within market access in the Nordics (Sweden, Denmark, Norway, Finland) for this company. List the name, title, LinkedIn profile. Separate each person by a pipe (|)."
            ),
        },
        {
            "role": "user",
            "content": 'Company URL: {} and LinkedIn: {}'.format(url, linkedin),
        },
    ]
