import pandas as pd
import numpy as np
import re

import sandbox.openAI_response as oa
from sandbox.openAI_response import get_response
from logger_tt import getLogger


log = getLogger(__name__)

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

def rename_staff(df):
    df = df.replace({r'Åsa Carnefeldt Levin'}, {'Åsa Levin'}, regex=True)
    df = df.replace({r'Sofia Johanson'}, {'Sofia Johansson'}, regex=True)
    df = df.replace({r'Lena Telerud Vaerlien'}, {'Lena Telerud Vaerlien Vaerlien'}, regex=True)
    df = df.replace({r' Palmquist'}, {' Palmqvist'}, regex=True)
    df = df.replace({r'Åsa CarnefeldtLevin'}, {'Åsa Carnefeldt Levin'}, regex=True)
    df = df.replace({r'Åsa Levin'}, {'Åsa Carnefeldt Levin'}, regex=True)
    df = df.replace({r'Matttias'}, {'Mattias'}, regex=True)
    df = df.replace({r'Lena Telerud$'}, {'Lena Telerud Vaerlien'}, regex=True)
    df = df.replace({r'Lena Telerud Vaerlien Vaerlien'}, {'Lena Telerud Vaerlien'}, regex=True)
    df = df.replace({r'Olven'}, {'Olvén'}, regex=True)
    df = df.replace({r'Vaerlin '}, {''}, regex=True)
    df = df.replace({r'Anderas Pousette'}, {'Andreas Pousette'}, regex=True)
    df = df.replace({r'Barbro Narosky'}, {'Barbro Naroskyin'}, regex=True)
    df = df.replace({r'Catarina Andersson-Forsman'}, {'Catharina Andersson Forsman'}, regex=True)
    df = df.replace({r'Egill Johnsson Bachmann'}, {'Egil Jonsson Bachmann'}, regex=True)
    df = df.replace({r'Johanna Ringqvist'}, {'Johanna Ringkvist'}, regex=True)
    df = df.replace({r'Katarina Zackrisson'}, {'Katarina Zackrisson Persson'}, regex=True)
    df = df.replace({r'Margareta Berglund-Rödén'}, {'Margareta Berglund Rödén'}, regex=True)
    df = df.replace({r'Margaretha Berglund Rödén'}, {'Margareta Berglund Rödén'}, regex=True)
    df = df.replace({r'Rebecka Lantho Graham'}, {'Rebecka Lantto Graham'}, regex=True)

    return df

def clean_up(df, drug_dict={}):
    df[df['company'].isna()].company = ''
    df=df.replace({r'\n'}, {' '}, regex=True)
    df=df.replace({r'\x0c'}, {''}, regex=True) # formfeed introduced in indication text

    df=df.replace({'three_part_deal': r'.* löper ut .*'}, {'three_part_deal': 'TRUE'}, regex=True)
    df=df.replace({'three_part_deal': r'^Ne.*'}, {'three_part_deal': 'FALSE'}, regex=True)
    # Qaly. not present -> np.nan
    df=df.replace({'QALY_comp': 'not presented'}, {'QALY_comp': np.nan}, regex=True)
    df=df.replace({'QALY_TLV': 'not presented'}, {'QALY_TLV': np.nan}, regex=True)

    # Remove for now
    #df=df.replace('not present', np.nan, regex=True)

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

    df = rename_staff(df)

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

def get_ema_indications(df_ema):
    log.info('getting indications')
    df_indication_to_drug_ema = df_ema.loc[:, ['product','indication']].drop_duplicates().dropna()
    df_indication_to_drug_ema['ICD'] =  df_indication_to_drug_ema['indication'].apply(lambda row: get_response(oa.get_ICD_codes(row)))
    df_indication_to_drug_ema = tidy_split(df_indication_to_drug_ema,'ICD')

    return df_indication_to_drug_ema
    