import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_model4 import Base, HTADocument, Company, HTAAgency, Form, Indication, Trial, NTCouncilRecommendation, Price, \
    Analysis, HTADocumentHasIndication, HTADocumentHasProduct, Personal, Product, ProductCompanyAssociation
from secret import secrets
SSL_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/ssl/DigiCertGlobalRootCA.crt.pem'
host = 'smartstepnordics-mysql8.mysql.database.azure.com'#cfg['mysql']['host']
port = '3306'#cfg['mysql']['port']
user = 'smartstepnordics_adm'#cfg['mysql']['user']
pw = secrets.mysql_pwd
temp = r'mysql+pymysql://' + user + ':' + pw + '@' + host + ':' + str(port) + '/' + 'documents_db' + "?charset=utf8mb4"
ssl_args = {'ssl': { 'ca':SSL_PATH}}
conn = temp
engine = create_engine(conn, connect_args=ssl_args)

Session = sessionmaker(bind=engine)
session = Session()

logic_dict = {"no": False, "yes": True, "NA": None, "": None}

# Sample JSON data (as a Python dictionary)
data = {
    "title": "Itulazax",
    "applicant": "ALK Nordic A/S",
    "product_name": "Itulazax",
    "diarie_number": "1639/2019",
    "date": "2019-12-12",
    "decision": "fully reimbursed",
    "currency": "SEK",
    "requested_complement": "no",
    "requested_information": "",
    "requested_complement_submitted": "NA",
    "indication": [
        {
            "medical_indication": "allergisk rinit och/eller konjunktivit orsakad av björkpollen",
            "severity": "moderate"
        }
    ],
    "form": [
        {
            "form": "Frystorkad tablett",
            "strength": "12 SQ-Bet",
            "AIP": "1078,20",
            "AUP": "1146,01"
        },
        {
            "form": "Frystorkad tablett",
            "strength": "12 SQ-Bet",
            "AIP": "2845,65",
            "AUP": "2948,81"
        }
    ],
    "HE_results": [
        {
            "population": "Patienter med allergisk rinit och/eller konjunktivit orsakad av björkpollen",
            "intervention": "Itulazax",
            "comparators_company": ["Alutard SQ Björk"],
            "comparator_modus_company": "Kostnadsminimeringsanalys",
            "comparator_reason_company": "Jämför kostnader och behandlingseffekter",
            "QALY_gain_company": "",
            "QALY_total_cost_company": "",
            "comparison_method": "direct"
        }
    ],
    "trials_company": [
        {
            "title_of_paper": "Kliniska studier av Itulazax",
            "number_of_patients": "Inte specificerat",
            "number_of_controls": "Inte specificerat",
            "indication": "Allergisk rinit",
            "duration": "Tre år",
            "phase": "III",
            "meta-analysis": "no",
            "randomized": "yes",
            "controlled": "yes",
            "blinded": "double",
            "primary_outcome_variable": "Sjukdomskontroll",
            "results": "Förbättrad sjukdomskontroll, signifikant minskning av symtom",
            "safety": "Väl tolererad, inga allvarliga biverkningar rapporterade"
        }
    ],
    "decision_makers": [
        {
            "name": "Staffan Bengtsson",
            "profession": "Tidigare överintendent"
        },
        {
            "name": "Margareta Berglund Rödén",
            "profession": "Överläkare"
        },
        {
            "name": "Elisabeth Wallenius",
            "profession": "Förbundsordförande"
        }
    ],
    "presenter_to_the_board": {
        "name": "Sara Massena",
        "title": "Medicinsk utredare"
    },
}



# Ensure the agency exists (assuming you have an agency name)
hta_agency = session.query(HTAAgency).filter_by(name='TLV').first()  # Replace with actual agency name

if not hta_agency:
    hta_agency = HTAAgency(name='TLV')  # Add the agency if it does not exist
    session.add(hta_agency)

# Ensure the company exists
company = session.query(Company).filter_by(name=data['applicant']).first()
if not company:
    company = Company(name=data['applicant'])
    session.add(company)

product = session.query(Product).filter_by(name=data['product_name']).first()
if not product:
    product = Product(name=data['product_name'])
    product.prod_companies.append(ProductCompanyAssociation(ass_company=company, role="agent"))

    session.add(product)

# Create HTADocument
hta_document = HTADocument(
    title=data['title'],
    diarie_nr=data['diarie_number'],
    date=data['date'],
    decision=data['decision'],
    currency=data['currency'],
    requested_complement=logic_dict[data['requested_complement']],
    requested_information=logic_dict[data['requested_information']],
    requested_complement_submitted=logic_dict[data['requested_complement_submitted']],
    company=company,
    agency=hta_agency,
    products=[product]
)

# Add indications
for ind in data['indication']:
    indication = Indication(who_full_desc=ind['medical_indication'])
    session.add(indication)
    # Create the many-to-many relationship
    hta_document.indications.append(indication)

# Add forms
for form_data in data['form']:
    form = Form(form=form_data['form'], strength=form_data['strength'])
    form.product = product
    session.add(form)
    price = Price(AIP=float(form_data['AIP'].replace(",",".")), AUP=float(form_data['AUP'].replace(",",".")))
    price.company = company
    form.prices.append(price)
    product.forms.append(form)
    #hta_document.forms.append(form)

# Add analysis (if needed)
analysis = Analysis(cohort='sjuka',comparators_company='mjölk',comparator_reason_company='gott')
hta_document.analyses.append(analysis)
session.add(analysis)

# Add decision makers
for decision_maker in data['decision_makers']:
    # Assuming you have a DecisionMaker model
    dm = Personal(name=decision_maker['name'], title=decision_maker['profession'])
    dm.agency = hta_agency
    session.add(dm)
    hta_document.personnel.append(dm)

# Add trials
for trial_data in data['trials_company']:
    trial = Trial(
        title = trial_data['title_of_paper'],
        nr_of_patients = trial_data['number_of_patients'] if isinstance(trial_data['number_of_patients'], int) else None,
        nr_of_controls =  trial_data['number_of_controls'] if isinstance(trial_data['number_of_controls'], int) else None,
        duration = trial_data['duration'],
        phase = trial_data['phase'],
        meta_analysis = logic_dict[trial_data['meta-analysis']],
        randomized = logic_dict[trial_data['randomized']],
        controlled = logic_dict[trial_data['controlled']],
        blinded = trial_data['blinded'],
        primary_outcome = trial_data['primary_outcome_variable'],
        results = trial_data['results'],
        safety = trial_data['safety'],
    )
    session.add(trial)
    hta_document.trials.append(trial)

# Commit the session
session.add(hta_document)
session.commit()

# Close the session
session.close()
