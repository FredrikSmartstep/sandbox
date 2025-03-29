import pandas as pd
import numpy as np
import re
from openAI_response import get_response, ATC_spec, base_indication_ind, extract_comparators, \
    classify_application_type, extract_experts, extract_comparators_ATC, extract_experts, extract_comparators_names, get_ICD_code, get_ICD_codes
#from scraping_LMV_products import get_product_info 

from active_drugs_translation import translation_dict

SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'

# def get_LMV_data():
#     df_prod_lmv = pd.read_xml(SAVE_PATH + 'Lakemedelsfakta_produktdokument_1_1.xml')
#     df = pd.DataFrame(columns=['product', 'company', 'ATC', 'active_drug', 'approval_date'])
#     for ind, row in df_prod_lmv.iterrows():
#         row['product'] = row['ProduktNamn']
#         row['company'] = row['Företag']
#         row = get_product_info(row)
#         df.loc[ind] = row[['product', 'company', 'ATC', 'active_drug', 'approval_date']]

#     return df

def tidy_split(df, column, sep=r'\|', keep=False):
    """
    Split the values of a column and expand so the new DataFrame has one split
    value per row. Filters rows where the column is missing.

    Params
    ------
    df : pandas.DataFrame
        dataframe with the column to split and expand
    column : str
        the column to split and expand
    sep : str
        the string used to split the column's values
    keep : bool
        whether to retain the presplit value as it's own row

    Returns
    -------
    pandas.DataFrame
        Returns a dataframe with the same columns as `df`.
    """
    indexes = list()
    new_values = list()
    #df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = re.split(sep, presplit)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            new_values.append(value.strip())
    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values
    return new_df

def find_without_substance(df, column):
    # Find duplicates
    # Find empty activw drug
    # Keep at least one instance
    df_unique_products = df.drop_duplicates(subset=['product'])
    df_has_substance = df[~df[column].isna()].drop_duplicates(subset=['product'])
    df_missing = df_unique_products[~df_unique_products['product'].isin(df_has_substance['product'])]

    #df_duplicates_df[df.duplicated(subset=['product', 'ATC', 'company'], keep=False) & ~df[column].isna()]
    #df_drugs = pd.concat([df_drugs, df_drugs_price, df_drugs_deal, df_drugs_rec, df_drugs_follow, df_drugs_ema], ignore_index=True)
    return df_missing 

def rename_drug(df, drug_dict={}):
    df = df.replace({r'\N{REGISTERED SIGN}'}, {''}, regex=True)  
    df = df.replace({'drug_name': r' \(.*'}, {'drug_name': ''}, regex=True)
    df = df.replace({'drug_name': r'\.'}, {'drug_name': ''}, regex=True)
    df['drug_name'] = df['drug_name'].str.title()
    df['drug_name'] = df['drug_name'].str.strip()
    df = df.replace({'drug_name': r'Produkt \(Substanser Och Atc-Kod\)'}, {'drug_name': ''}, regex=True)
    df = df.replace({'drug_name': r'Krka.*'}, {'drug_name': 'Krka'}, regex=True)
    df = df.replace({'drug_name': r'Actavis.*'}, {'drug_name': 'Actavis'}, regex=True)
    df = df.replace({'drug_name': r'Sandoz.*'}, {'drug_name': 'Sandoz'}, regex=True)
    df = df.replace({'drug_name': r'Teva.*'}, {'drug_name': 'Teva'}, regex=True)
    df = df.replace({r'EVENITY'}, {'Evenity'}, regex=True)
    df = df.replace({'drug_name': r' injektionsvätska'}, {'drug_name': ''}, regex=True)
    df = df.replace({'drug_name': r' 5 och 10 mg'}, {'drug_name': ''}, regex=True)
    df = df.replace({'drug_name': r' \d* mg'}, {'drug_name':''}, regex=True)
    
    #df = df.replace(drug_dict, regex=True) 

    df = df.replace({'drug_name': r'Alkeran'}, {'drug_name':'Alkéran'}, regex=True)
    
    df = df.replace({'drug_name': r'Abakavir/Lamivudin Mylan Ab.*'}, {'drug_name':'Abacavir/Lamivudine Mylan AB'}, regex=True)
    df = df.replace({'drug_name': r'Clopidogrel Acino.*'}, {'drug_name':'Clopidogrel Acino'}, regex=True)
    #df = df.replace({'drug_name': r'Clopidogrel Teva.*'}, {'drug_name':'Clopidogrel Teva'}, regex=True)
    df = df.replace({'drug_name': r'Darunavir Krka.*'}, {'drug_name':'Darunavir Krka'}, regex=True)
    #df = df.replace({'drug_name': r'Docetaxel Teva.*'}, {'drug_name':'Docetaxel Teva'}, regex=True)
    #df = df.replace({'drug_name': r'Imatinib Teva.*'}, {'drug_name':'Imatinib Teva'}, regex=True)
    df = df.replace({'drug_name': r'Imprida Hct.*'}, {'drug_name':'Imprida'}, regex=True)
    df = df.replace({'drug_name': r'Incruse.*'}, {'drug_name':'Incruse Ellipta'}, regex=True)
    df = df.replace({'drug_name': r'Sprimeo Hct'}, {'drug_name':'Sprimeo'}, regex=True)
    df = df.replace({r'GAVRETO'}, {'Gavreto'}, regex=True)
    df = df.replace({r'IMBRUVICA'}, {'Imbruvica'}, regex=True)
    df = df.replace({'drug_name': r'; '}, {'drug_name': ' och '}, regex=True)
    df = df.replace({r'LIVTENCITY'}, {'Livtencity'}, regex=True)
    df = df.replace({r'Melatonin Unimedic Pharma'}, {'Melatonin Unimedic'}, regex=True)
    df = df.replace({'drug_name': r', startkit'}, {'drug_name': ''}, regex=True)
    df = df.replace({r'PALFORZIA'}, {'Palforzia'}, regex=True)
    df = df.replace({r'PREVYMIS'}, {'Prevymis'}, regex=True)
    df = df.replace({r'REKOVELLE'}, {'Rekovelle'}, regex=True)
    df = df.replace({r'TAKHZYRO'}, {'Takhzyro'}, regex=True)
    df = df.replace({r'TEPMETKO'}, {'Tepmetko'}, regex=True)
    df = df.replace({r'Viktrakvi'}, {'Vitrakvi'}, regex=True)

    df = df.replace({'drug_name': r'Accord.*'}, {'drug_name':'Accord'}, regex=True)
    df = df.replace({'drug_name': r'Glenmark.*'}, {'drug_name':'Glenmark'}, regex=True)
    df = df.replace({'drug_name': r'Stada.*'}, {'drug_name':'Stada'}, regex=True)
    df = df.replace({'drug_name': r'Mylan.*'}, {'drug_name':'Mylan'}, regex=True)
    df = df.replace({'drug_name': r'Zentiva.*'}, {'drug_name':'Zentiva'}, regex=True)
    df = df.replace({'drug_name': r'Orion.*'}, {'drug_name':'Orion'}, regex=True)
    df = df.replace({'drug_name': r'Sun.*'}, {'drug_name':'Sun'}, regex=True)
    df = df.replace({'drug_name': r'Orifarm.*'}, {'drug_name':'Orifarm'}, regex=True)
    df = df.replace({'drug_name': r'Serb.*'}, {'drug_name':'Serb'}, regex=True)
    df = df.replace({'drug_name': r'Baxalta.*'}, {'drug_name':'Baxalta'}, regex=True)
    df = df.replace({'drug_name': r'Medac.*'}, {'drug_name':'Medac'}, regex=True)
    df = df.replace({'drug_name': r'Fresenius Kabi.*'}, {'drug_name':'Fresenius Kabi'}, regex=True)
    df = df.replace({'drug_name': r'Hospira.*'}, {'drug_name':'Hospira'}, regex=True)
    df = df.replace({'drug_name': r'Pfizer.*'}, {'drug_name':'Pfizer'}, regex=True)
    df = df.replace({'drug_name': r'Richter.*'}, {'drug_name':'Richter'}, regex=True)
    df = df.replace({'drug_name': r'Bluefish.*'}, {'drug_name':'Bluefish'}, regex=True)
    df = df.replace({'drug_name': r'Sanofi.*'}, {'drug_name':'Sanofi'}, regex=True)
    df = df.replace({'drug_name': r'Santenpharma Ab.*'}, {'drug_name':'Santen'}, regex=True)  
    df = df.replace({'drug_name': r'Ucb.*'}, {'drug_name':'Ucb'}, regex=True)
    df = df.replace({'drug_name': r'Viatris.*'}, {'drug_name':'Viatris'}, regex=True)
    df = df.replace({'drug_name': r'Baxter.*'}, {'drug_name':'Baxter'}, regex=True)
    df = df.replace({'drug_name': r'Oresund.*'}, {'drug_name':'Oresund'}, regex=True)

    df['drug_name'] = df['drug_name'].str.title()
    df['drug_name'] = df['drug_name'].str.strip()
    return df

def rename_company(df, column='company'):
    df[column] = df[column].str.replace(r'\.', '', regex=True, flags=re.IGNORECASE)
    df[column] = df[column].str.replace(r'\,', '', regex=True, flags=re.IGNORECASE)
    df[column] = df[column].str.replace(r'Aktiebolag', 'AB', regex=True, flags=re.IGNORECASE)
    df[column] = df[column].str.replace(r'ApS', 'Aps', regex=True, flags=re.IGNORECASE)
    df[column] = df[column].str.replace(r'Limited', 'Ltd', regex=True, flags=re.IGNORECASE)
    df[column] = df[column].str.strip()
    df[column] = df[column].str.lower()

    df[column] = df[column].str.replace(r'Företgets namn: ', '', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Företagets namn: ', '', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Aktiebolag', 'AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'a\/s', 'A/S', regex=True, flags=re.I)


    df[column] = df[column].str.replace(r'1 A Pharma GmbH', '1A Pharma Gmbh', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'2care4 Aps$', '2Care4 Generics Aps', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'2care4 Generics Aps$', '2Care4 Generics Aps', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Abbott S.*', 'Abbott Srl', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Abbvie AB', 'AbbVie AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'AbbVie$', 'AbbVie AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'AbbVie Ltd', 'AbbVie AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'AbbVie Deutschland GmbH.*', 'AbbVie AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Accord.*', 'Accord Healthcare AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Acino.*', 'Acino Pharma GmbH', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Aco .*', 'Aco Hud Nordic', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Acom.*', 'Acom - Advanced Center Oncology', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Actavis.*', 'Actavis AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'ADIENNE Srl.*', 'ADIENNE Srl', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'ADVANZ PHARMA Ltd', 'Advanz Pharma Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'ADVICENNE.*', 'Advicenne SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'AGB-Pharma AB$', 'AGB Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Aimmune.*', 'Aimmune Therapeutics Ireland Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Akcea.*', 'Akcea Therapeutics Ireland Limited', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Albireo.*', 'Albireo AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Alternova.*', 'Alternova A/s', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Alcon.*', 'Alcon Nordic A/S', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Alexion.*', 'Alexion Pharma Nordics AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'ALK Nordic.*', 'ALK Nordic A/S', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Allergan.*', 'Allergan Norden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Almirall.*', 'Almirall SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Alnylam.*', 'Alnylam Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Amgen.*', 'Amgen AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Amicus Therapeutics UK Ltd', 'Amicus Therapeutics Europe Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Amicus.*', 'Amicus Therapeutics Europe Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Amryt.*', 'Amryt AG', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'\"Anpharm\" Przedsiębiorstwo Farmaceutyczne Sa', 'Anpharm Przedsiebiorstwo Farmaceutyczne Sa', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'AOP Orphan.*', 'AOP Orphan Pharmaceuticals Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Apotek Produktion.*', 'Apotek Produktion & Laboratorier AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Archie.*', 'Archie Samuel sro', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Arvelle Therapeutics Sweden filial$', 'Arvelle Therapeutics Sweden', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Aspen Nordic.*', 'Aspen Nordic', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Astellas.*', 'Astellas Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Astra.*', 'Astra Zeneca AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Atnahs.*', 'Atnahs Pharma Nordics A/S', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Aurobindo Pharma (Malta) Ltd', 'Aurobindo Pharma Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Aziende Chimiche Riunite Angelini Francesco.*', 'Aziende Chimiche Riunite Angelini Francesco Spa', regex=True, flags=re.I) 
    df[column] = df[column].str.replace(r'Bausch [\&|\+] Lomb.*', 'Bausch & Lomb Nordic AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'BAYER.*', 'Bayer AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Bayer.*', 'Bayer AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Baxalta.*', 'Baxalta Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Baxter.*', 'Baxter Medical AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'BeiGene.*', 'Beigene Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Beigene.*', 'Beigene Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'bene-Arzneimittel GmbH', 'Bene Arzneimittel Gmbh', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Besins Healthcare.*', 'Besins Healthcare', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Bial.*', 'Bial Portela & Companhia SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'BIAL.*', 'Bial Portela & Companhia SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'BIAL.*', 'Bial Portela & Companhia SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Biocodex$', 'Biocodex Oy', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Biocryst.*', 'BioCryst Pharmaceuticals', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Biogen.*', 'Biogen Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'BIOGEN SWEDEN', 'Biogen Sweden', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'BioMarin.*', 'BioMarin Europe Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'BMS', 'Bristol-Myers Squibb AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Bristol-Myers.*', 'Bristol-Myers Squibb AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Bristol Myers.*', 'Bristol-Myers Squibb AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Boehringer Ingelheim.*', 'Boehringer Ingelheim AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'CAMPUS.*', 'Campus Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Celgene.*', 'Celgene AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Celltrion Healthcare$', 'Celltrion Healthcare Hungary Kft', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Chem Works Of G.*', 'Gedeon Richter Nordics Ab', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'CHEPLAPHARM.*', 'Cheplapharm Arzneimittel GmbH', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Chiesi.*', 'Chiesi Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Cipla.*', 'Cipla (EU) Limited', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'CIS bio.*', 'CIS bio international', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'CSL.*', 'CSL Behring AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Daiichi.*', 'Daiichi Sankyo Europe GmbH', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Diurnal Limited', 'Diurnal Europe BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Domp.*', 'Dompé Biotec SpA', regex=True, flags=re.I)
    #df[column] = df[column].str.replace(r'DNE Pharma AS', 'dne pharma AS', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Dr Gerhard Mann.*', 'Dr Gerhard Mann', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Eagle.*', 'Eagle Laboratories Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Eckert  Ziegler Radiopharma GmbH', 'Eckert Ziegler Radiopharma Gmbh', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Eisai.*', 'Eisai AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Eli.*', 'Eli Lilly Sweden AB', regex=True, flags=re.I)
    #df[column] = df[column].str.replace(r'ELI LILLY SWEDEN AB', 'Eli Lilly Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'EQL Pharma Int AB$', 'EQL Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Essential.*', 'Essential Pharma Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Esteve.*', 'Esteve Pharmaceuticals GmbH', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Eurocept.*', 'Eurocept International BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Extrovis Eu Ltd', 'Extrovis Eu Kft', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'EUSA.*', 'EUSA Pharma (UK) Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Fair-Med', 'Fairmed', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Farco Pharma GmbH', 'Farco-Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Ferring.*', 'Ferring Läkemedel AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Fresenius Kabi.*', 'Fresenius Kabi AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'FrostPharma$', 'FrostPharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Frost Pharma', 'FrostPharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Galapagos.*', 'Galapagos BioPharma Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Galderma.*', 'Galderma Nordic AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Galenicum Health.*', 'Galenicum Health Slu', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Gideon Richters Nordic AB', 'Gedeon Richter Nordics AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Gedeon Richter.*', 'Gedeon Richter Nordics AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'GENZYME A/S', 'Genzyme AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Genzyme.*', 'Genzyme AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'GE Healthcare.*', 'GE Healthcare AS', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Gilead.*', 'Gilead Sciences Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Glaxo.*', 'GlaxoSmithKline AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Glenmark.*', 'Glenmark Pharmaceuticals Nordic AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'GMP Orphan$', 'GMP-Orphan SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Grnenthal Sweden AB$', 'Grunenthal Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Grunenthal.*', 'Grunenthal Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Grünenthal.*', 'Grunenthal Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'GW Pharma.*', 'GW Pharmaceuticals (International) BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'H Lundbeck.*', 'H Lundbeck AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Hameln Pharmaceuticals Gmbh', 'Hameln Pharma Gmbh', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Hansa.*', 'Hansa Biopharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Hexal.*', 'Hexal AG', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Hikma Farmaceutica \(Portugal\) Sa', 'Hikma Farmacêutica (Portugal) Sa', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Hospira.*', 'Hospira Nordic AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Immunocore.*', 'Immunocore Ireland Limited', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Incyte.*', 'Incyte Biosciences Nordic AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Invidior', 'Indivior', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Indivior.*', 'Indivior Nordics Aps', regex=True, flags=re.I) 
    df[column] = df[column].str.replace(r'IPSEN.*', 'Ipsen AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Ipsen.*', 'Ipsen AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Institut Produits.*', 'Ipsen AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Instituto Grifols.*', 'Instituto Grifols SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Janssen.*', 'Janssen-Cilag AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Jensonr+ Ltd', 'Jensonr + Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Karo Pharma AB', 'Karo Healthcare AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Kite.*', 'Kite/Gilead Sciences Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Krka.*', 'KRKA Sverige AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'KRKA.*', 'KRKA Sverige AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Kyowa.*', 'Kyowa Kirin AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'L Molteni.*', 'L Molteni & C dei Fratelli Alitti Società di Esercizio SpA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Laboratorios Farmacéuticos ROVI SA$', 'Laboratorios Farmacéuticos Rovi SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Laboratoire Fr.*', 'Laboratoire Français Du Fractionnement Et Des Biotechnologies', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Laboratoires Juvisé Pharmaceuticals$', 'Laboratoires Juvise Pharmaceuticals', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'LEO.*', 'Leo Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Mallinckrodt.*', 'Mallinckrodt Pharmaceuticals Ireland Limited', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'medac.*', 'Medac GmbH, filial', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Medac.*', 'Medac GmbH, filial', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Medice Arzneimittel.*', 'Medice Arzneimittel Pütter Gmbh & Co Kg', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Medivir$', 'Medivir AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Medtronic.*', 'Medtronic BioPharma BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Menarini.*', 'Menarini International Operations Luxembourg SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Merck$', 'Merck AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Merck Europe.*', 'Merck AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Merck Ger.*', 'Merck AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Merck Ser.*', 'Merck AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Merck Sharp.*', 'Merck Sharp and Dohme AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Merus.*', 'Merus Labs Luxco Ii Sárl', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Mitsubishi.*', 'Mitsubishi Tanabe Pharma Europe Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Mundipharma.*', 'Mundipharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Mylan.*', 'Mylan AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Neuraxpharm.*', 'Neuraxpharm Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'NordicInfu Care AB$', 'Nordicinfu Care AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Novartis.*', 'Novartis Sverige AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'NOVARTIS SVERIGE AB', 'Novartis Sverige AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Norgine.*', 'Norgine Danmark A/S', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Novo Nor.*', 'Novo Nordisk Scandinavia AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Octapharma.*', 'Octapharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Omrix.*', 'Omrix Biopharmaceuticals Nv', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Orchad.*', 'Orchard Therapeutics (Netherlands) BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Oresund.*', 'Oresund Pharma Aps', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Orkla*', 'Orkla Care A/S', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Orifarm.*', 'Orifarm AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Orion.*', 'Orion Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'ORPHAN EUROPE NORDIC AB', 'Orphan Europe Nordic AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Otsuka.*', 'Otsuka Pharma Scandinavia AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Pacira.*', 'Pacira Ireland Limited', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Panpharma$', 'Panpharma Sa', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Pfizer.*', 'Pfizer AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Pharmacia.*', 'Pfizer AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Pharmathen.*', 'Pharmathen Pharmaceutics Sa', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'PharmaPrim AB$', 'Pharmaprim AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Photonamic.*', 'Photonamic GmbH & Co KG', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Pierre Fabre.*', 'Pierre Fabre Pharma Norden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Portola.*', 'Portola Pharma UK Limited', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'PTC Therapeutics.*', 'PTC Therapeutics International Limited', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'ratiopharm GmbH', 'Ratiopharm GmbH', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Reckitt Benckiser Healthcare \(Scandinavia\) A\/S', 'Reckitt Benckiser Nordic A/S', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Recordati.*', 'Recordati AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Regeneron.*', 'Regeneron UK Limited', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Rhythm P.*', 'Rhythm Pharmaceuticals BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Roche.*', 'Roche AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'SAKEN', 'POA Pharma Scandinavia AB', regex=True, flags=re.I) # Easiest
    df[column] = df[column].str.replace(r'SAM$', 'SAM Nordic', regex=True, flags=re.I) 
    df[column] = df[column].str.replace(r'Sandoz.*', 'Sandoz A/S', regex=True, flags=re.I) 
    df[column] = df[column].str.replace(r'Sanofi.*', 'Sanofi AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Santen.*', 'SantenPharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Santhera.*', 'Santhera Pharmaceuticals', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Schering.*', 'Merck Sharp and Dohme AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Seagen.*', 'Seagen BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Seacross.*', 'Seacross Pharmaceuticals Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Secura Bio.*', 'Secura Bio Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Seqirus.*', 'Seqirus GmbH', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Serb.*', 'Serb SA', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Servier.*', 'Servier Sverige AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Shire Pharmaceuticals Ireland Limited*', 'Shire Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Sloan Pharma Sarl.*', 'Sloan Pharma Sàrl', regex=True, flags=re.I)
    
    df[column] = df[column].str.replace(r'SmithKline Beecham.*', 'SmithKline Beecham Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Stada.*', 'Stada Nordics AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Steba Biotech$', 'Steba Biotech Sa', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Sun Pharmaceutical Industries Europe BV', 'Sun Pharmaceutical Ind Europe BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'SUN.*', 'Sun Pharmaceutical Ind Europe BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Swedish Orphan.*', 'Swedish Orphan Biovitrum AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Takeda.*', 'Takeda Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Techdow.*', 'Techdow Europe AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Tesaro.*', 'Tesaro Bio Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Teva.*', 'Teva Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Thea Nordic Ab', 'Théa Nordic Ab', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Theravance.*', 'Theravance Biopharma Ireland Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'THERAVIA.*', 'Theravia', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'TMC.*', 'TMC Pharma Services Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Tillotts.*', 'Tillotts Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'UCB.*', 'UCB Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Unimedic AB', 'Unimedic Pharma AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Univar.*', 'Univar BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Valneva.*', 'Valneva Sweden AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Vanda.*', 'Vanda Pharmaceuticals Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Viatris.*', 'Viatris AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Viiv.*', 'Viiv Healthcare Bv', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Vertex.*', 'Vertex Pharmaceuticals (Ireland) Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Warner Chilcott.*', 'Warner Chilcott UK Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Wyeth.*', 'Pfizer AB', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Zaklady.*', 'Zaklady Farmaceutyczne Polpharma Sa', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Zambon SpA.*', 'Zambon Sweden filial of Zambon Nederland BV', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Zentiva.*', 'Zentiva Denmark Aps', regex=True, flags=re.I)

    df[column] = df[column].str.replace(r'\(lokal företrädare\)', '', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r' \(SE\)', '', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r' \(Sweden\)', '', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r' SAS', '', regex=True, flags=re.I)

    df[column] = df[column].str.title()

    df[column] = df[column].str.replace(r'Eckert \& Ziegler Radiopharma Gmbh', 'Eckert Ziegler Radiopharma Gmbh', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Essential Pharma (M) Ltd', 'Essential Pharma Ltd', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Hikma Farmaceutica (Portugal) Sa', 'Hikma Farmacêutica (Portugal) Sa', regex=True, flags=re.I)
    df[column] = df[column].str.replace(r'Italfarmaco Sa', 'Italfarmaco Spa', regex=True, flags=re.I)
    
    return df

def clean_up_deal(df):
    if 'company' in df.columns:
        df = rename_company(df)
    df = df.rename(columns={'product':'drug_name'})
    df = rename_drug(df)
    df = df.rename(columns={'drug_name':'product'})
    return df

def clean_up_LMV(df):
    df = df[df['H/V']=='HUM']
    df = df.rename(columns={'Innehavare':'company', 'Namn':'drug_name',
                            'ATC-kod':'ATC','Ombud':'agent'})
    
    df = rename_company(df)
    df = rename_company(df,'agent')
    df = rename_drug(df)
    df = df.drop(columns={'Restsituation','Försäljningsstatus','H/V',
                          'Djurslag','Direktimporterat läkemedel',
                          'Avvikelse från direktimporterat läkemedel','Produktkategori',
                          'Maskinell dos.disp'})
    df = df.rename(columns={'drug_name':'product','Styrka':'strength',
                            'Form': 'form','Registrerings-status':'status',
                            'MT-nummer': 'MT_number','NPL-id':'NPL_id', 
                            'EUMA-nummer': 'EUMA_number', 
                            'Tidigare läkemedelsnamn':'earlier_name','Utbytbarhet':'generic',
                            'Godkännande-datum':'approval_date',
                            'Avregistrerings-datum':'unregistration_date',
                            'Godkännande-procedur':'procedure',
                            'Särskilda regler för biv.rap.':'side_effect_spec',
                            'Narkotika':'narcotics','Dispens-beslut':'exemption',
                            'Receptstatus':'prescription',
                            'Parallellimport':'parallel'})
    trans = dict({'Ja': 1, 'Nej': 0})
    df['generic'] =  df['generic'].replace(trans, regex=True)
    # To separate old from new with same name [df['status']=='Avregistrerad']
    #df['product'] = df.apply(lambda x: '{} AVREG'.format(x['product']) if x.status=='Avregistrerad' else x['product'], axis=1)

    #df = drop_duplicated_without_substance(df)
    #df['active_drug'] = df['active_drug_1']
    #df = df.drop(['active_drug', 'active_drug_1'],axis=1)
    df = df.drop_duplicates()
    return df

def clean_up_ema(df, drug_dict={}):
    df = rename_company(df)
    df = df.rename(columns={'product':'drug_name'})
    df = rename_drug(df, drug_dict)
    df = df.rename(columns={'drug_name':'product'})
    #df = drop_duplicated_without_substance(df)
    df['active_drug'] = df['active_drug'].str.replace(r'[\(|\[][a-zA-Z0-9\-\[\]\, ]*[\)|\]]','', regex=True)
    df = tidy_split(df, 'active_drug', sep=r'\,|\/| and ', keep=False)
    df = df.drop_duplicates()
    return df

def clean_up_price(df, drug_dict={}):
    df = rename_company(df)
    df = df.rename(columns={'product':'drug_name'})
    df = rename_drug(df, drug_dict)
    df = df.rename(columns={'drug_name':'product'})
    df = df.replace({r'\N{REGISTERED SIGN}'}, {''}, regex=True)

    df['product'] = df['product'].str.title()

    return df 

def clean_up_work(df_work):
    df_work = df_work.replace({r'\n'}, {''}, regex=True)
    df_work = df_work.replace({r'och '}, {''}, regex=True)
    df_work = df_work.replace({r'-'}, {''}, regex=True)
    df_work = df_work.replace({r'- '}, {''}, regex=True)
    df_work = df_work.replace({r'Åsa Carnefeldt Levin'}, {'Åsa Levin'}, regex=True)
    df_work = df_work.replace({r'Sofia Johanson'}, {'Sofia Johansson'}, regex=True)
    df_work = df_work.replace({r'Lena Telerud Vaerlien'}, {'Lena Telerud Vaerlien Vaerlien'}, regex=True)
    df_work = df_work.replace({r' Palmquist'}, {' Palmqvist'}, regex=True)
    df_work = df_work.replace({r'Åsa CarnefeldtLevin'}, {'Åsa Carnefeldt Levin'}, regex=True)
    df_work = df_work.replace({r'Matttias'}, {'Mattias'}, regex=True)
    df_work = df_work.replace({r'hälsoekonomer'}, {'hälsoekonom'}, regex=True)
    df_work = df_work.replace({r'jurister'}, {'jurist'}, regex=True)
    df_work = df_work.replace({r'medicinska'}, {'medicinsk'}, regex=True)
    df_work = df_work.replace({r'hälsoekonom/analytiker'}, {'hälsoekonom'}, regex=True)
    df_work = df_work.replace({r'analytiker/hälsoekonom'}, {'hälsoekonom'}, regex=True)
    df_work = df_work.replace({r'Lena Telerud$'}, {'Lena Telerud Vaerlien'}, regex=True)
    df_work = df_work.replace({r'Lena Telerud Vaerlien Vaerlien'}, {'Lena Telerud Vaerlien'}, regex=True)
    df_work = df_work.replace({r'Olven'}, {'Olvén'}, regex=True)
    df_work = df_work.replace({r'Vaerlin '}, {''}, regex=True)
    df_work = df_work.replace({r'senior '}, {''}, regex=True)
    df_work = df_work.replace({r'Anderas Pousette'}, {'Andreas Pousette'}, regex=True)

    # remove white spaces
    df_work = df_work.map(lambda x: x.strip() if isinstance(x, str) else x)
    return df_work

def clean_up(df, drug_dict={}):
    df[df['company'].isna()].company = ''
    df=df.replace({r'\n'}, {' '}, regex=True)
    df=df.replace({r'\x0c'}, {''}, regex=True) # formfeed introduced in indication text

    df=df.replace({'three_part_deal': r'.* löper ut .*'}, {'three_part_deal': 'TRUE'}, regex=True)
    df=df.replace({'three_part_deal': r'^Ne.*'}, {'three_part_deal': 'FALSE'}, regex=True)
    # Qaly. not present -> np.nan
    df=df.replace({'QALY_comp': 'not presented'}, {'QALY_comp': np.nan}, regex=True)
    df=df.replace({'QALY_TLV': 'not presented'}, {'QALY_TLV': np.nan}, regex=True)

    df=df.replace('not present', np.nan, regex=True)

    df = df.drop(columns=['Unnamed: 0'], errors='ignore')

    df = df.reset_index(drop=True)

    FIX_SEV = False
    if FIX_SEV:
        df['severity'] = df['severity'].str.replace(r'.*mycket.*', 'very high', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*låg.*', 'low', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*hög.*', 'high', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*måttlig.*', 'moderate', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*medel.*', 'moderate', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*ej.*', '', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*inte.*', '', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*inte.*', 'not assessed', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*variera.*', 'varying', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*olik.*', 'varying', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'.*variera.*', 'varying', regex=True, flags=re.IGNORECASE)
        df['severity'] = df['severity'].str.replace(r'\b(?!very high|high|moderate|low|varying\b)\w+\b|\s|[^\w\s]+', '', regex=True, flags=re.IGNORECASE)

    pattern = r'([\d\,]*( miljon| miljard)?(er)?( SEK| kr))'
    df['annual_turnover'] = df['annual_turnover'].str.replace(r'\d( )\d', '', regex=True, flags=re.IGNORECASE)
    #df['annual_turnover'] = df['annual_turnover'].str.extract(pattern, flags=re.IGNORECASE)
    df['annual_turnover'] = df['annual_turnover'].apply(lambda x: re.match(pattern,str(x))[0] if bool(re.match(pattern,str(x))) else '')
    # TODO: Some turnover not correct!

    df = tidy_split(df,'company', r'\,(?![^,]*\bfilial\b)')
    df = rename_company(df)

    df = rename_drug(df, drug_dict)

    df = df.replace({r'22 maj 2015 om inget klockstopp'}, {'2015-05-15'}, regex=True)
    df = df.replace({r'16-12-2021'}, {'2021-12-16'}, regex=True)
    
    df['decision_date'] = df['decision_date'].replace({r' \(.*\)'}, {''}, regex=True)
    # active drug
    df['active substance'] = df['active substance'].str.lower()
    df = df.replace({r'axicabtagen- ciloleucel'}, {'axicabtagene ciloleucel'}, regex=True)
    df = df.replace({r'brexukabtagen-  autoleucel'}, {'brexucabtagene autoleucel'}, regex=True)
    df = df.replace({r'brexucabtagene  autoleucel'}, {'brexucabtagene autoleucel'}, regex=True)
    df = df.replace({r'zanubrutunib'}, {'zanubrutinib'}, regex=True)
    df = df.replace({r'nirmatrelvir och ritonavir'}, {'nirmatrelvir/ritonavir'}, regex=True)
    df = df.replace({r'evolokumab'}, {'evolocumab'}, regex=True)
    

    df = df.replace({r'Ansökan om ny beredningsform'}, {'Ny Beredningsform'}, regex=True)
    df = df.replace({r'Ansökan om subvention för nytt läkemedel'}, {'Nytt läkemedel'}, regex=True)
    df = df.replace({r'Nya Läkemedel'}, {'Nytt läkemedel'}, regex=True)
    df = df.replace({r'Ny ansökan'}, {'Nyansökan'}, regex=True)
    
    df['drug_name'] = df['drug_name'].str.replace(r'2,5 mg,,  ', '', regex=True, flags=re.IGNORECASE)

    # remove white spaces
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    # Let's copy the severity for duplicate rows of the same product
    # First find missing severity
    df_missing_sev = df[pd.isnull(df['severity'])]
    # Then, let's find duplicates
    df_duplicates = df[df.duplicated(subset='drug_name', keep = False)]
    # Inner join of these two sets
    affected_index = df_missing_sev.index.join(df_duplicates.index, how='inner')
    df_to_fix = df.iloc[affected_index,:]
    # Go through list and find the other duplicates and check if any of them has a severity
    for ind, row in df_to_fix.iterrows():
        # duplicates
        bo = (df['drug_name'] == row['drug_name'])
        df_same_drug = df[bo]
        # of which has severity
        bo2 = ~pd.isnull(df_same_drug['severity'])
        df_w_sev = df_same_drug[bo2]
    
        if not df_w_sev.empty:
            sev = df_w_sev.iloc[0]['severity']
            # Check if the same severity if more than 1
            if len(df_w_sev)>1:
                for i, r in df_w_sev.iterrows():
                    #if sev != r['severity']:
                    #    print(r['drug_name'] + ' Not consistent!')
                    sev = r['severity']       
            # Copy severity from this instance
            df.loc[ind,'severity'] = sev
    return df


def clean_atc_codes(df):
    # This function will remove rows where the ATC code is a prefix of another longer ATC code in the same group
    # remove non ATC
    df['ATC'] = df['ATC'].str.extract(r'([A-Z]\d\d[A-Z]*[\d]*)')

    df['ATC'] = df['ATC'].str.strip()
    df = tidy_split(df, 'ATC', sep='\,', keep=False)
    # remove duplicates
    df = df.drop_duplicates()
    # Group by product to handle them together
    groups = df.groupby('product')
    
    cleaned_rows = []
    for name, group in groups:
        atc_codes = group['ATC'].tolist()
        
        # Remove rows where the ATC code is a prefix of another ATC code
        to_remove = []
        for i, code in enumerate(atc_codes):
            for other_code in atc_codes:
                if code != other_code and other_code.startswith(code):
                    to_remove.append(i)
        
        # Append the remaining rows
        cleaned_rows.append(group.drop(group.index[to_remove]))
    
    # Concatenate all the cleaned groups
    cleaned_df = pd.concat(cleaned_rows)
    return cleaned_df

def fill_missing_companies(df):
    # Fill missing company names by carrying forward the last known company name in each product group
    #df['company'] = df.groupby('product')['company'].fillna(method='ffill')
    gidx = df.groupby('product', sort=False)['company'].ngroup()
    df2 = df.groupby(gidx).ffill().groupby(gidx).bfill()
    return df2

def fill_ATC(df, df_class):
    # First find missing ATC
    df_missing_ATC = df[df['ATC'].isna()]
    if not df_missing_ATC.empty:
        for ind, row in df_missing_ATC.iterrows():
            #print(ind)
            mask = df_class['Product'].str.contains(row['drug_name'], flags=re.IGNORECASE)
            atcs = df_class['ATC'][mask]
            if len(atcs)>0:
                print('Found ATC')
                atc = atcs.mode().values[0]
            else:
                atc = get_response(ATC_spec(row['drug_name']))
                m = re.search(r'([A-Z]\d\d[A-Z]*[\d]*)', atc)
                if bool(m):
                    atc = m.group(0)
                else:
                    atc = ''
            #print(row['drug_name'])
            #print(atc)
            df.loc[ind,'ATC'] = atc.strip()
    return df

def fill_indication(df):
    df = df.fillna(value='')
    df['sv_indications'] = df['indication']
    if not df.empty:
        for ind, row in df.iterrows():
            #print(type(row['indication']))
            #if not type(row['indication'])==str:
            #    print(type(row['indication']))
                #row['indication'] = ''
            if not bool(re.search(r'[A-Z]\d\d', row['indication'])): 
                indication = get_response(base_indication_ind(row['indication']))
                df.loc[ind,'indication'] = indication
    df['all_indications'] = df['indication']
    return df

def get_ICD(df):
    if not df.empty:
        for ind, row in df.iterrows():
            icd_code = get_response(get_ICD_code(row['therapeutic_area']))
            df.loc[ind,'ICD'] = icd_code
    return df

def get_ICD_codes(df):
    if not df.empty:
        for ind, row in df.iterrows():
            icd_codes = get_response(get_ICD_codes(row['indication']))
            df.loc[ind,'ICD'] = icd_codes
            df = tidy_split(df, 'ICD')
    return df

def get_comparators(df):
    if not df.empty:
        for ind, row in df.iterrows():
            if len(row['comparators'])>50:
                comparators = get_response(extract_comparators(row['comparators']))
                df.loc[ind,'comparators'] = comparators
    return df

def get_comparators_names(df):
    if not df.empty:
        for ind, row in df.iterrows():
            if len(str(row['comparators']))>3:
                comparators_names = get_response(extract_comparators_names(str(row['comparators'])))
                df.loc[ind,'comparators_names'] = comparators_names
    return df

def get_experts(df):
    if not df.empty:
        for ind, row in df.iterrows():
            if len(str(row['experts']))>3:
                experts = get_response(extract_experts(str(row['experts'])))
                df.loc[ind,'experts_names'] = experts
    return df

def get_application_type(df):
    df2 = df.dropna(subset='application_text')
    if not df2.empty:
        for ind, row in df2.iterrows():
            if len(row['application_text'])>5:
                application_type = get_response(classify_application_type(row['application_text']))
                df.loc[ind,'application_text'] = application_type
    return df

def get_experts(df):
    if not df.empty:
        for ind, row in df.iterrows():
            if len(str(row['experts']))>5:
                exps = get_response(extract_experts(str(row['experts'])))
                df.loc[ind,'expert_list'] = exps
    return df

def prod_ATC_fix(df, fill_comp=True):
    # one product msay have several ATC codes. Some are obsolete
    # Import ATC register
    # split atc on \,
    df = tidy_split(df, 'ATC', sep='\,', keep=False)
    # remove duplicates
    df = df.drop_duplicates()
    # remove non ATC
    df['ATC'] = df['ATC'].str.extract(r'([A-Z]\d\d[A-Z]*[\d]*)')
    # replace if more elobrate code
    # collect those with full atc:s
    df_full_atc = df[df['ATC'].str.len()>4] 
    # find all with atc with less than 5 letters
    df_short_atc = df[df['ATC'].str.len()<=4] 
    # try to match and replace to get full atc code 
    for index, row in df_short_atc.iterrows():
        matches = df_full_atc[df_full_atc['product'].isin([row['product']])]
        if not matches.empty:
           for i, m in matches.iterrows():
               print(m['product'])
           df_short_atc.loc[index]['ATC'] = matches.iloc[0]['ATC'] 
    df = pd.concat([df_full_atc, df_short_atc])
    if not fill_comp:
        return df
    # remove/fill empty corparate
    # Find all with companies
    df_w_corp = df.dropna(subset='company')
    # find those without companies
    df_no_corp = df[pd.isnull(df['company'])]
    # try to match and fill 
    for index, row in df_no_corp.iterrows():
        matches = df_w_corp[df_w_corp['product'].isin([row['product']])]
        if not matches.empty:
           df_no_corp.iloc[index]['company'] = matches.iloc[0]['company'] 
    df = pd.concat([df_w_corp, df_no_corp])
    return df

if __name__ == '__main__':
    #-------------------------------------
    df_class = pd.read_csv(SAVE_PATH + 'ATC_class.csv', sep=';')
    # get data
    df_LMV_prod = pd.read_excel(SAVE_PATH + 'lakemedelsprodukter-2024-08-15.xlsx', sheet_name='2024-08-15')

    df_price = pd.read_csv(SAVE_PATH + 'MEDPrice.csv', sep=';')
    df_nt_deal = pd.read_csv(SAVE_PATH + 'nt_deal.csv', sep=';')
    df_nt_rec = pd.read_csv(SAVE_PATH + 'nt_radet.csv', sep=';')
    df_nt_follow = pd.read_csv(SAVE_PATH + 'nt_follow.csv', sep=';')
    df_ema = pd.read_csv(SAVE_PATH + 'EMA_dec_2023.csv', sep=';')
    df_indications = pd.read_excel(SAVE_PATH + 'ICD-10_MIT_2021_Excel_16-March_2021_from_republic_of_SA.xlsx', sheet_name='SA ICD-10 MIT 2021')
    df_indications = df_indications.drop(columns=['Group_Desc','Number', 'Chapter_No',
                                                  'Chapter_Desc', 'Group_Code',
                                                  'SA_Start_Date','SA_End_Date',
                                                  'SA_Revision_History','Comment',
                                                  'WHO_Start_date','WHO_End_date',
                                                  'WHO_Revision_History'])
    df_indications.columns = df_indications.columns.str.lower()
    df_indications = df_indications.apply(lambda x: x.astype(str).str.title())
    df_indications = df_indications.apply(lambda x: x.astype(str).str.strip())
    trans = dict({'Y': 1, 'N': 0})
    df_indications[df_indications.columns[pd.Series(df_indications.columns).str.startswith('valid')]] = df_indications[df_indications.columns[pd.Series(df_indications.columns).str.startswith('valid')]].replace(trans)

    df1 = pd.read_csv(SAVE_PATH + 'files.csv', sep=';')
    df2 = pd.read_csv(SAVE_PATH + 'files_decision.csv', sep=';')
    df3 = pd.read_csv(SAVE_PATH + 'files_for_nt.csv', sep=';')
    df12 = pd.read_csv(SAVE_PATH + 'files_agg_new3.csv', sep=';')
    df22 = pd.concat([df1,df2])
    print(len(df22))
    print(len(df12))
    #df12['severity'] = df22['severity']
    #df12 = pd.concat([df12,df3])
    df_work = pd.read_csv(SAVE_PATH + 'work_tot.csv', sep=';')
    df_work2 = pd.read_csv(SAVE_PATH + 'work_tot_for_nt.csv', sep=';')
    df_work = pd.concat([df_work,df_work2]).reindex()
    #--------------------------------------
    # clean it
    print('clean up')
    df_LMV_prod = clean_up_LMV(df_LMV_prod)

    # The code below does not work since multiple drugs have had the same old name
    # the LMV file contains old drug names. Let's add that to our dict for translating drug names
    #df_drugs_trans = df_LMV_prod.loc[:, ['product','earlier_name']].drop_duplicates()
    #df_drugs_trans = tidy_split(df_drugs_trans, 'earlier_name','\,').dropna()
    # append old name with regex sheit to make the regex work as we want
    #df_drugs_trans['earlier_name'] = df_drugs_trans.apply(lambda x: '(?i){}$'.format(x['earlier_name']), axis=1)
    #df_drugs_trans.to_csv(SAVE_PATH + 'drug_trans.csv', sep=';', encoding='utf-8-sig', index = False)
    #dict_drug_trans = pd.Series(df_drugs_trans['product'].values,index=df_drugs_trans['earlier_name']).to_dict()

    df_price = df_price.rename(columns={'Företag':'company','ATC-kod':'ATC','Produktnamn':'product'})
    df_price = clean_up_price(df_price)#, dict_drug_trans)
    df_nt_deal = clean_up_deal(df_nt_deal)
    df_nt_rec = clean_up_deal(df_nt_rec)
    df_nt_follow = clean_up_deal(df_nt_follow)
    df_ema = clean_up_ema(df_ema)#, dict_drug_trans)
    df12 = clean_up(df12)#, dict_drug_trans)

    print(len(df12))
    df_work = clean_up_work(df_work)
    #--------------------------------------
    # fill up missing data
    
    FILL_MISSING = False
    if FILL_MISSING:
        df12 = get_comparators_names(df12)
        df12 = get_experts(df12)
        print('fill ATC')
        df12 = fill_ATC(df12, df_class)
        print('fill ind')
        df12 = fill_indication(df12)
        df12 = get_comparators(df12)
        df12 = get_application_type(df12)
    print('BEFORE split, LEN: ' + str(len(df12)))
    df12 = tidy_split(df12, 'indication')
    df12 = tidy_split(df12, 'active substance', sep=r'\/|\, |\+|och', keep=False)
    
    df12['ICD'] = df12['indication'].str.extract(r'([A-Z]\d\d\.?[\d]*)')
    print('BEFORE tidy icd, LEN: ' + str(len(df12)))
    #df12 = tidy_split(df12, 'ICD', sep='+', keep=False)
    print('BEFORE SAVE, LEN: ' + str(len(df12)))
    #--------------------------------------
    df12.to_csv(SAVE_PATH + 'files_agg_new.csv', sep=';', encoding='utf-8-sig')
    # extract 
    # extract companies
    print('extract')
    
    df_comp_LMV = pd.DataFrame(df_LMV_prod['company'].unique())
    df_comp_agent_LMV = pd.DataFrame(df_LMV_prod['agent'].unique())
    df_comp_price = pd.DataFrame(df_price['company'].unique())
    df_comp_deal = pd.DataFrame(df_nt_deal['company'].unique())
    df_comp_ema = pd.DataFrame(df_ema['company'].unique())
    df_comp = pd.DataFrame(df12['company'].unique())
    df_comp = df_comp.dropna()
    df_comp = pd.concat([df_comp_agent_LMV,df_comp_LMV,df_comp,df_comp_price,df_comp_deal, df_comp_ema], ignore_index=True)
    df_comp = df_comp.drop_duplicates()
    df_comp = df_comp.dropna()
    df_comp = df_comp.set_axis(['name'], axis='columns')
    # extract agent to drug
    df_drug_12_agent = df12.loc[:, ['drug_name', 'company']].drop_duplicates()
    df_drug_12_agent = df_drug_12_agent.rename(columns={'drug_name':'product'})
    df_drug_12_agent['role'] = 'agent'
    df_drug_deal_agent = df_nt_deal.loc[:, ['product', 'company']].drop_duplicates()
    df_drug_deal_agent['role'] = 'agent'
    df_drug_LMV_agent = df_LMV_prod.loc[:, ['product','agent']].drop_duplicates()
    df_drug_LMV_agent['role'] = 'agent'
    df_drug_LMV_agent = df_drug_LMV_agent.rename(columns = {'agent':'company'})
    # owner
    df_drug_ema_owner = df_ema.loc[:, ['product', 'company']]
    df_drug_ema_owner['role'] = 'manufacturer'
    df_drug_LMV_owner = df_LMV_prod.loc[df_LMV_prod['parallel'].isna(), ['product','company']].drop_duplicates()
    df_drug_LMV_owner['role'] = 'mah'
    # dist
    df_drug_LMV_par_dist = df_LMV_prod.loc[~df_LMV_prod['parallel'].isna(), ['product','company']].drop_duplicates()
    df_drug_LMV_par_dist['role'] = 'dist'
    df_drug_price_dist = df_price.loc[:, ['product', 'company']].drop_duplicates()
    df_drug_price_dist['role'] = 'dist'
    # continues after df_drugs...


    df_drugs_form = df_LMV_prod.loc[:, ['product', 'strength', 'form','MT_number',
                                        'NPL_id','EUMA_number','earlier_name','generic']].drop_duplicates()
    df_drugs_reg = df_LMV_prod.loc[:, ['product', 'strength', 'form', 'status', 'approval_date',
                                       'unregistration_date','procedure',
                                       'side_effect_spec','narcotics',
                                       'exemption','prescription']].drop_duplicates()
    # extract indications
    #df_indications = df12.loc[:, ['indication','ICD']].drop_duplicates(subset=['ICD'])
    #df_ICD = pd.DataFrame(df12['ICD'].unique())

    # extract drugs
    df_drugs_LMV = df_LMV_prod.loc[:, ['product', 'ATC']].drop_duplicates(subset=['product', 'ATC']) #,'MT_number','NPL_id','EUMA_number'
    # no, we want to keep multiple ATC:s df_drugs_LMV = prod_ATC_fix(df_drugs_LMV, fill_comp=False).drop_duplicates(subset=['product', 'ATC'])
    df_drugs_LMV_ATC = df_drugs_LMV.loc[:, ['product', 'ATC']].drop_duplicates()
    df_drugs_LMV = df_drugs_LMV.drop(columns='ATC').drop_duplicates()
    df_drugs_price = df_price.loc[:, ['product', 'ATC']].drop_duplicates(subset=['product', 'ATC'])
    #df_drugs_price = pd.DataFrame(df_price['product'].unique()) # TODO: extract correct msanufacturer
    df_drugs_deal = df_nt_deal.loc[:, ['product', 'ATC', 'active_drug']].drop_duplicates()
    df_drugs_deal = tidy_split(df_drugs_deal,'active_drug',r'\/')
    #df_drugs_deal = pd.DataFrame(df_nt_deal['product'].unique())
    df_drugs_deal = tidy_split(df_drugs_deal,'active_drug',r'\/|\,|\+')
    df_drugs_rec = df_nt_rec.loc[:, ['product', 'ATC','active_drug']].drop_duplicates()
    #df_drugs_rec = pd.DataFrame(df_nt_rec['product'].unique())
    df_drugs_rec = tidy_split(df_drugs_rec,'active_drug',r'\/')
    df_drugs_follow = df_nt_follow.loc[:, ['product', 'ATC','active_drug']].drop_duplicates()
    df_drugs_follow = tidy_split(df_drugs_follow,'active_drug',r'-')
    #df_drugs_follow = pd.DataFrame(df_nt_follow['product'].unique())
    df_drugs_ema = df_ema.loc[:, ['product', 'ATC', 'active_drug']]
    #df_drugs_ema = pd.DataFrame(df_ema['product'].unique())
    df_drugs = df12.loc[:, ['drug_name', 'ATC', 'active substance']].drop_duplicates('drug_name')
    df_drugs = df_drugs.rename(columns={'drug_name':'product', 'active substance':'active_drug'})
    # We need to consider also the productg that werre not authorized by the EMA. Sometimes filing for reimbursement was made in paralllel with the EMA submission
    df_drugs = pd.concat([df_drugs_LMV, df_drugs, df_drugs_price, df_drugs_deal, df_drugs_rec, df_drugs_follow, df_drugs_ema], ignore_index=True)
    df_drugs['active_drug'] = df_drugs['active_drug'].str.lower()
    df_drugs = tidy_split(df_drugs, 'active_drug', sep=r'\/|\,|\+|och', keep=False)
    df_drugs['product'] = df_drugs['product'].str.title()
    df_drugs = df_drugs.rename(columns={'active_drug':'active substance'})
    
    df_drugs['active substance eng'] = df_drugs['active substance'].map(translation_dict)

    df_comp_has_prod = pd.concat([df_drug_12_agent, df_drug_deal_agent,
                                  df_drug_LMV_agent, df_drug_ema_owner, df_drug_LMV_owner, 
                                  df_drug_LMV_par_dist, df_drug_price_dist], ignore_index=True).drop_duplicates().dropna(subset='company')
    
    drugs_with_man = set(df_comp_has_prod.loc[df_comp_has_prod['role']=='manufacturer', 'product'])

    unique_drugs = set(df_drugs['product'].drop_duplicates())
    drugs_no_namufacturer = unique_drugs.difference(drugs_with_man)

    df_set_mah_as_man = df_LMV_prod.loc[df_LMV_prod['parallel'].isna(), ['product','company']].drop_duplicates()
    df_set_mah_as_man = df_set_mah_as_man.loc[df_set_mah_as_man['product'].isin(drugs_no_namufacturer), ['product','company']].drop_duplicates()
    df_set_mah_as_man['role'] = 'manufacturer'

    df_comp_has_prod = pd.concat([df_drug_12_agent, df_drug_deal_agent,
                                  df_drug_LMV_agent, df_drug_ema_owner, df_drug_LMV_owner, 
                                  df_drug_LMV_par_dist, df_drug_price_dist, df_set_mah_as_man], ignore_index=True).drop_duplicates().dropna(subset='company')

    with open(SAVE_PATH + 'prod_no_man.txt','w') as f:
        f.write(str(drugs_no_namufacturer))

    df_drugs_2 = df_drugs.copy()
    # Replace NaN in 'ATC' column with empty strings
    df_drugs['ATC'] = df_drugs['ATC'].replace('nan','')
    # Clean ATC codes
    df_drugs = clean_atc_codes(df_drugs)

    # Fill missing companies
    #df_drugs = fill_missing_companies(df_drugs)
    #df_drugs = prod_ATC_fix(df_drugs)

    #df_drugs_no_owner = df_drugs[df_drugs['company'].isna()].drop_duplicates(subset=['product']) #find_without_substance(df_drugs,'company')
    df_drugs_no_ATC = df_drugs[df_drugs['ATC']=='nan'].drop_duplicates(subset=['product']) #find_without_substance(df_drugs,'ATC')
    df_drugs_no_active = df_drugs[df_drugs['active substance eng'].isna()].drop_duplicates(subset=['product']) #find_without_substance(df_drugs,'active substance')

    df_drugs_unique = pd.DataFrame({'product': df_drugs['product'].unique()}).dropna()
    df_drugs_to_active = df_drugs.loc[:, ['product', 'active substance eng']].drop_duplicates()
    df_drugs_to_ATC = df_drugs.loc[:, ['product', 'ATC']].drop_duplicates()
    
    # extract indication to drug
    print('len df12: ' + str(len(df12)))
    print(df12.columns)
    df_indication_to_drug = df12.loc[:, ['drug_name','ICD']].drop_duplicates().dropna()
    df_indication_to_drug['source'] = 'TLV/NT'
    # extract active drug
    df_active_drugs = pd.DataFrame(df_drugs['active substance eng'].unique(),columns=['name']).dropna()
    df_active_drugs = df_active_drugs.rename(columns={'0':'name'})
    #df_active_drugs.columns = 'name'
    # extract drug to hta
    df_drugs_to_hta = df12.loc[:, ['drug_name','diarie_nr']].drop_duplicates()
    # extract hta
    df_diarie_to_indication = df12[['diarie_nr','indication','ICD']]
    
    df_hta = df12.drop(columns=['drug_name','ATC','indication','experts','active substance','forms','strengths','CEA','CM','application_text','all_indications','ICD'])
    df_hta = df_hta.drop_duplicates(subset=['diarie_nr'])
    df_hta = df_hta.rename(columns={'decision_date':'date','decision_summary':'summary','indirect_comp':'indirect_comparison',
                                    'ICER_comp':'ICER_company'})
    # extract ATC
    df_atc = pd.DataFrame(df_drugs['ATC'].unique())
    # extract reviewers
    df_reviewers =  df_work[['name','title']].drop_duplicates()
    df_reviewers = df_reviewers.dropna()
    df_reviewers = df_reviewers.reset_index(drop=True)

    df_work = df_work.drop(columns=['title', 'Unnamed: 0'])
    df_work = df_work.reset_index(drop=True)

    df_comp = df_comp.reset_index(drop=True)
    print(df_comp.columns)
    #df_comp = df_comp.rename(columns={df_comp.columns[0]:'name'})

    df_comp_almost_same = df_comp.copy()
    df_comp_almost_same['short_name'] = df_comp['name'].str[0:4]
    df_comp_almost_same['short_name'] = df_comp_almost_same['short_name'].str.capitalize()
    df_duplicate_comp = df_comp_almost_same[df_comp_almost_same.duplicated(subset='short_name', keep=False)]

    df_drug_almost_same = df_drugs_LMV.copy()
    df_drug_almost_same['short_name'] = df_drugs_LMV['product'].str[0:4]
    df_drug_almost_same['short_name'] = df_drug_almost_same['short_name'].str.capitalize()
    df_duplicate_drug = df_drug_almost_same[df_drug_almost_same.duplicated(subset='short_name', keep=False)]
    df_duplicate_drug = df_drug_almost_same[df_drug_almost_same.duplicated(subset='product', keep=False)]

    df_drugs = df_drugs.reset_index(drop=True)
    df_atc = df_atc.reset_index(drop=True)

    df_ema = df_ema.drop(columns=['active_drug','Active substance', 'company', 'Revision number', 'First published', 'Revision date'])
    df_ema = df_ema.rename(columns={'Condition / indication':'indication',
                                    'Date of refusal of marketing authorisation':'Date of refusal'})
    df_ema.columns = df_ema.columns.str.lower()
    df_ema.columns = df_ema.columns.str.replace(' ','_')
    trans = dict({'yes': 1, 'no': 0})
    df_ema['patient_safety'] = df_ema['patient_safety'].replace(trans)
    df_ema['additional_monitoring'] = df_ema['additional_monitoring'].replace(trans)
    df_ema['generic'] = df_ema['generic'].replace(trans)
    df_ema['biosimilar'] = df_ema['biosimilar'].replace(trans)
    df_ema['conditional_approval'] = df_ema['conditional_approval'].replace(trans)
    df_ema['exceptional_circumstances'] = df_ema['exceptional_circumstances'].replace(trans)
    df_ema['accelerated_assessment'] = df_ema['accelerated_assessment'].replace(trans)
    df_ema['orphan_medicine'] = df_ema['orphan_medicine'].replace(trans)

    # unsure about this...
    #df_prod_to_ind_ema = df_ema[['product','ICD']]
    #df_prod_to_ind_ema = df_prod_to_ind_ema.rename(columns={"product": "drug_name"})
    

    df_indication_to_drug_ema = pd.read_csv(SAVE_PATH + 'EMA_indications.csv', sep=';')
    if False:
        df_indication_to_drug_ema = df_ema.loc[:, ['product','therapeutic_area']].drop_duplicates().dropna()
        df_indication_to_drug_ema = tidy_split(df_indication_to_drug_ema,'therapeutic_area','\;')
        df_indication_to_drug_ema = get_ICD(df_indication_to_drug_ema)
        df_indication_to_drug_ema.to_csv(SAVE_PATH + 'EMA_indications.csv', sep=';', encoding='utf-8-sig', index = False)
    df_indication_to_drug_ema = df_indication_to_drug_ema.drop(columns='therapeutic_area').drop_duplicates()
    df_indication_to_drug_ema['source'] = 'EMA' 
    df_indication_to_drug = df_indication_to_drug.rename(columns={'drug_name':'product'})
    df_indication_to_drug = pd.concat([df_indication_to_drug, df_indication_to_drug_ema])
    #df_ema.astype({'patient_safety': 'bool','additional_monitoring': 'bool',
    #                        'generic': 'bool','biosimilar': 'bool',
    #                        'conditional_approval': 'bool','exceptional_circumstances': 'bool',
    #                        'accelerated_assessment': 'bool','orphan_medicine': 'bool',})
    
    #df_atc.columns = ['atc_index', 'ATC']
    #df_comp.columns = ['index', 'name']
    df_drugs = df_drugs.rename(columns={'index':'drug_index'})
    #--------------------------------------
    print('save')
    # Save
    df_duplicate_comp.to_csv(SAVE_PATH + 'almost_same.csv', sep=';', encoding='utf-8-sig', index = False)
    df_duplicate_drug.to_csv(SAVE_PATH + 'almost_same_drug.csv', sep=';', encoding='utf-8-sig', index = False)
    df_comp_has_prod.to_csv(SAVE_PATH + 'comp_has_prod.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_reg.to_csv(SAVE_PATH + 'drug_regulatory.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_form.to_csv(SAVE_PATH + 'drug_form.csv', sep=';', encoding='utf-8-sig', index = False)
    #df_drug_agent.to_csv(SAVE_PATH + 'drug_agent.csv', sep=';', encoding='utf-8-sig', index = False)
    #df_comp_LMV.to_csv(SAVE_PATH + 'LMV_comp.csv', sep=';', encoding='utf-8-sig', index = False)
    #df_comp_agent_LMV.to_csv(SAVE_PATH + 'LMV_agent.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_LMV.to_csv(SAVE_PATH + 'LMV_drugs.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_LMV_ATC.to_csv(SAVE_PATH + 'LMV_drugs_ATC.csv', sep=';', encoding='utf-8-sig', index = False)
    df_work.to_csv(SAVE_PATH + 'reviewer_to_file.csv', sep=';', encoding='utf-8-sig', index = False)
    df_reviewers.to_csv(SAVE_PATH + 'reviewers.csv', sep=';', encoding='utf-8-sig', index = False)
    df_indication_to_drug.to_csv(SAVE_PATH + 'indication_to_drug.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_to_hta.to_csv(SAVE_PATH + 'drugs_to_hta.csv', sep=';', encoding='utf-8-sig', index = False)
    df_hta.to_csv(SAVE_PATH + 'hta.csv', sep=';', encoding='utf-8-sig', index = False)
    df_diarie_to_indication.to_csv(SAVE_PATH + 'diarie_to_indication.csv', sep=';', encoding='utf-8-sig', index = False)
    df_comp.to_csv(SAVE_PATH + 'companies.csv', sep=';', encoding='utf-8-sig', index = False)
    df_active_drugs.to_csv(SAVE_PATH + 'active_drugs.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs.to_csv(SAVE_PATH + 'drugs.csv', sep=';', encoding='utf-8-sig', index = False)
    #df_LMV_prod.to_csv(SAVE_PATH + 'drugs_LMV.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_2.to_csv(SAVE_PATH + 'drugs_2.csv', sep=';', encoding='utf-8-sig', index = False)
    #df_drugs_no_owner.to_csv(SAVE_PATH + 'drugs_no_owner.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_no_ATC.to_csv(SAVE_PATH + 'drugs_no_ATC.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_no_active.to_csv(SAVE_PATH + 'drugs_no_active.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_unique.to_csv(SAVE_PATH + 'drugs_unique.csv', sep=';', encoding='utf-8-sig', index = False)
    df_drugs_to_active.to_csv(SAVE_PATH + 'df_drugs_to_active.csv', sep=';', encoding='utf-8-sig', index = False)
    df_ema.to_csv(SAVE_PATH + 'EMA.csv', sep=';', encoding='utf-8-sig', index = False)
    #df_prod_to_ind_ema.to_csv(SAVE_PATH + 'EMA_ICD.csv', sep=';', encoding='utf-8-sig', index = False)
    df_atc.to_csv(SAVE_PATH + 'ATC.csv', sep=';', encoding='utf-8-sig', index = False)
    df_indications.to_csv(SAVE_PATH + 'indications.csv', sep=';', encoding='utf-8-sig', index = False)
    #df_ICD.to_csv(SAVE_PATH + 'ICD.csv', sep=';', encoding='utf-8-sig', index = False)

    df_price.to_csv(SAVE_PATH + 'price_new.csv', sep=';', encoding='utf-8-sig')