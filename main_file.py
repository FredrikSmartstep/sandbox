import os #, sys; sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import re
from backbone.data_handler import data_handler
import pandas as pd
from backbone.utils import data_cleaner as dc
from backbone.scraper import scraper
from backbone.scraper import scraping_tools as st
from logger_tt import getLogger, setup_logging

setup_logging(config_path=os.getcwd() + '/log_config.json')
log = getLogger(__name__)

dh = data_handler.DataHandlerProduction()

SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'


def upsert_ATC():
    log.info('Getting ATC')
    # TODO: Get the file from the website

    # Get a df
    df_active_drugs = pd.read_csv(SAVE_PATH + 'WHO ATC-DDD 2024-07-31.csv', sep=',')
    df_active_drugs = df_active_drugs.drop(columns='note')
    df_active_drugs = df_active_drugs.rename(columns ={'atc_code':'ATC', 'atc_name':'name',
                                            'ddd':'DDD','uom':'unit','adm_r':'admin_route'})
    dh.insert_active_drug(df_active_drugs)
    log.info('ATC upserted')

def upsert_indication():
     # TODO: Get the file from the website
    log.info('Getting indications')
    df_indications = pd.read_excel(SAVE_PATH + 'ICD-10_MIT_2021_Excel_16-March_2021_from_republic_of_SA.xlsx', 
                               sheet_name='SA ICD-10 MIT 2021')
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

    dh.insert_indications(df_indications)
    log.info('Indications upserted')

def upsert_LMV():

    def clean_up_LMV(df):
        df = df[df['H/V']=='HUM']
        df = df.rename(columns={'Innehavare':'company', 'Namn':'drug_name',
                                'ATC-kod':'ATC','Ombud':'agent'}) 
        df = dc.rename_company(df)
        df = dc.rename_company(df,'agent')
        df = dc.rename_drug(df)
        df = df.drop(columns={'Restsituation','H/V',
                            'Djurslag','Direktimporterat läkemedel',
                            'Avvikelse från direktimporterat läkemedel','Produktkategori',
                            'Maskinell dos.disp'})
        df = df.rename(columns={'drug_name':'product','Styrka':'strength',
                                'Form': 'form', 'Registrerings-status': 'status',
                                'MT-nummer': 'MT_number','NPL-id':'NPL_id', 
                                'EUMA-nummer': 'EUMA_number', 'Försäljningsstatus':'sales_status',
                                'Tidigare läkemedelsnamn':'earlier_name','Utbytbarhet':'generic',
                                'Godkännande-datum':'approval_date',
                                'Avregistrerings-datum':'deregistration_date',
                                'Godkännande-procedur':'procedure',
                                'Särskilda regler för biv.rap.':'side_effect_spec',
                                'Narkotika':'narcotics','Dispens-beslut':'exemption',
                                'Receptstatus':'prescription','Ombud':'agent',
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
    # ----------------------------------------------------------------------------------------------
    # Get LMV produkts ->prod, prod_active, company, company_product_owner, company_product_agent, reg_status, form
    # -------------------------
    log.info('Getting LMV')
    scraper.get_LMV_data()
    df_LMV = pd.read_excel(SAVE_PATH + 'LMV.xlsx')
    df_LMV = clean_up_LMV(df_LMV)
    # add products
    df_drugs_LMV_ATC = df_LMV.loc[:, ['product', 'ATC']].drop_duplicates()
    df_drugs_LMV_ATC = df_drugs_LMV_ATC.rename(columns={'product':'name'})
    df_drugs_LMV = df_drugs_LMV_ATC.drop(columns='ATC').drop_duplicates()
    dh.insert_product(df_drugs_LMV)
    log.info('Products upserted')
    # add prod_active
    dh.insert_product_has_ATC(df_drugs_LMV_ATC)
    log.info('Product has active upserted')

    # insert companies
    df_comp_LMV = pd.DataFrame(df_LMV['company'].unique())
    df_comp_LMV = df_comp_LMV.set_axis(['name'], axis='columns')
    dh.insert_companies(df_comp_LMV)
    log.info('Companies upserted')

    # Add product-company relationship
    # Market Auth Holder
    df_drug_LMV_mah = df_LMV.loc[df_LMV['parallel'].isna(), ['product','company']].drop_duplicates()
    df_drug_LMV_mah['role'] = 'mah'
    # Listed as agent
    df_drug_LMV_agent = df_LMV.loc[:, ['product','agent']].drop_duplicates()
    df_drug_LMV_agent['role'] = 'agent'

    # The parallel importers
    df_drug_LMV_par_dist = df_LMV.loc[~df_LMV['parallel'].isna(), ['product','company']].drop_duplicates()
    df_drug_LMV_par_dist['role'] = 'distributor'
    
    # get current manufacturers
    drugs_no_namufacturer = dh.get_products_no_man()
    # The parallel importers are not manufacturers
    df_set_mah_as_man = df_LMV.loc[df_LMV['parallel'].isna(), ['product','company']].drop_duplicates()
    
    df_set_mah_as_man = df_set_mah_as_man.loc[df_set_mah_as_man['product'].isin(drugs_no_namufacturer), ['product','company']].drop_duplicates()
    df_set_mah_as_man['role'] = 'manufacturer'

    df_comp_prod = pd.concat([df_drug_LMV_agent, df_drug_LMV_mah, df_set_mah_as_man], ignore_index=True).drop_duplicates().dropna(subset='company')
    dh.insert_company_has_product(df_comp_prod)
    log.info('Product owner/agent upserted')

    # Add Form
    df_LMV_form = df_LMV.loc[:, ['product', 'strength', 'form','MT_number',
                                            'NPL_id','EUMA_number','earlier_name']].drop_duplicates()
    dh.insert_form(df_LMV_form)
    log.info('Form upserted')

    df_LMV_reg = df_LMV.loc[:, ['product', 'strength', 'form', 'status', 'approval_date',
                                        'deregistration_date', 'sales_status', 'procedure',
                                        'side_effect_spec','narcotics',
                                        'exemption','prescription', 'generic']].drop_duplicates()
    # reg status
    dh.insert_regulatory_status(df_LMV_reg)
    log.info('Regulatory status upserted')


def upsert_price():
    # TODO: Get the file from the website
    log.info('Getting prices')

    df_price = pd.read_csv(SAVE_PATH + 'MEDPrice.csv', sep=';')
    df_price = df_price.rename(columns={'Företag':'company','ATC-kod':'ATC','Produktnamn':'product'})
    df_price = dc.clean_up_price(df_price)
    
    # Add product-company relationship
    df_price_distributor = df_price.loc[:, ['product','company']].drop_duplicates().dropna() # deregistered product may still linger but then the distributor column may be empty thus dropna
    df_price_distributor['role'] = 'distributor'
    dh.insert_company_has_product(df_price_distributor)
    log.info('Distributors upserted')

    #df_missing_form = df_medprice[~df_medprice['NPL id'].isin(df_lmv_form['NPL_id'])]
    df_price = df_price[['product','Varunummer','ATC','NPL id','Förpackning','Antal','company','AIP','AUP','AIP per st']]
    df_price = df_price.rename(columns={'Varunummer':'varunummer','ATC-kod':'ATC','NPL id':'NPL_id',
                                        'Förpackning':'package','Antal':'size','AIP per st':'AIP_piece','company':'name'})
    dh.insert_price(df_price)
    log.info('Prices upserted')


def upsert_ema():
    # TODO: Get the file from the website
    log.info('Getting EMA')

    # ema -> ema_status, products, company, pro_comp, prod_indication, pro_atc
    df_ema = pd.read_csv(SAVE_PATH + 'EMA_dec_2023.csv', sep=';')
    df_ema = dc.clean_up_ema(df_ema)#, dict_drug_trans)

    # add possible new products
    df_ema_product_ATC = df_ema.loc[:, ['product', 'ATC']].drop_duplicates()
    df_ema_product_ATC = df_ema_product_ATC.rename(columns={'product':'name'})
    df_drugs_ema = df_ema_product_ATC.drop(columns='ATC').drop_duplicates()
    dh.insert_product(df_drugs_ema)
    log.info('Products upserted')

    # add prod_active
    dh.insert_product_has_ATC(df_ema_product_ATC)
    log.info('Product has ATC upserted')

    # insert companies
    df_comp_ema = pd.DataFrame(df_ema['company'].unique()).dropna() # some unapproved drugs lack company in the ema file
    df_comp_ema = df_comp_ema.set_axis(['name'], axis='columns')
    dh.insert_companies(df_comp_ema)
    log.info('Companies upserted')

     # Add product-company relationship
    df_drug_ema_owner = df_ema.loc[:, ['product', 'company']].drop_duplicates().dropna() # some unapproved drugs lack company in the ema file
    df_drug_ema_owner['role'] = 'manufacturer'
    dh.insert_company_has_product(df_drug_ema_owner)
    log.info('Product owner upserted')


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

    dh.insert_EMA(df_ema)
    log.info('EMA status upserted')

    processed = True
    if not processed:
        df_prod_to_ind_ema = dc.get_ema_indications(df_ema)
        df_prod_to_ind_ema = df_prod_to_ind_ema[['product','ICD']]
        df_prod_to_ind_ema = df_prod_to_ind_ema.rename(columns={"product": "drug_name"})
        df_prod_to_ind_ema = df_prod_to_ind_ema.drop_duplicates()
        df_prod_to_ind_ema.to_csv(SAVE_PATH + 'EMA_prod_ind.csv',sep=';')
    else:
        df_prod_to_ind_ema = pd.read_csv(SAVE_PATH + 'EMA_prod_ind.csv',sep=';')
    df_prod_to_ind_ema['ICD'][df_prod_to_ind_ema['ICD'].isna()] = ''
    df_prod_to_ind_ema['ICD'] =  df_prod_to_ind_ema['ICD'].apply(lambda row: re.sub(r'(?<=\.\d)\d','', row))
    
    log.info('time for prod to ind')
    log.info('Length of ema ind2prod: ' + str(len(df_prod_to_ind_ema.index)))
    dh.insert_product_has_indication(df_prod_to_ind_ema,'EMA')
    log.info('Product has indications upserted')

def upsert_ema_atmp_status():
    # TODO Get file
    log.info('EMA ATMP upsert')
    df_atmp = pd.read_csv(SAVE_PATH + 'atmps_nov_2024.csv', sep=';')
    df_atmp = df_atmp.rename(columns={"product": "drug_name"})
    df_atmp = dc.rename_drug(df_atmp)
    df_atmp = df_atmp.rename(columns={"drug_name": "product"})
    dh.insert_atmp_status(df_atmp)

def upsert_NT():
    log.info('Getting NT data')

    df_nt_deal = st.get_NT_deals_df()
    #df_nt_deal = pd.read_csv(SAVE_PATH + 'nt_deal.csv', sep=';')
    df_nt_deal = dc.clean_up_deal(df_nt_deal)
    dh.insert_nt_council_deal(df_nt_deal)
    log.info('NT rec upserted')
    
    df_nt_rec = st.get_NT_recommendation_df()
    #df_nt_rec = pd.read_csv(SAVE_PATH + 'nt_radet.csv', sep=';')
    df_nt_rec = dc.clean_up_deal(df_nt_rec)
    dh.insert_nt_council_rec(df_nt_rec)
    log.info('NT rec upserted')
    
    df_nt_follow = pd.read_csv(SAVE_PATH + 'nt_follow.csv', sep=';')
    df_nt_follow = dc.clean_up_deal(df_nt_follow)
    dh.insert_nt_council_follow_up(df_nt_follow)
    log.info('NT follow-up upserted')
    
    df_nt_no_ass = st.get_NT_no_assessment_df()
    df_nt_no_ass = dc.clean_up_deal(df_nt_no_ass)
    dh.insert_nt_council_no_ass(df_nt_no_ass)
    log.info('NT no assessment upserted')

if __name__ == '__main__':    
    # initialize scraper

    #upsert_ATC()
    #----------------
    # Get product classification (hosptial or basic drug, Med Dev) and med devices
    #df_class = pd.read_csv(SAVE_PATH + 'ATC_class.csv', sep=';')
    # ---------------
    #upsert_indication()

    #upsert_ema()

    #upsert_ema_atmp_status()

    #upsert_LMV()

    #upsert_price()

    upsert_NT()

    # fill scraping_queue with links to each dossier
    # start workers
    # worker:
    # download files to temp dir
    # parse dossier
    # remove dir

