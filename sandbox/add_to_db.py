import data_handler
import pandas as pd
dh = data_handler.DataHandlerProduction()

INIT = False

SAVE_PATH = 'C:/Users/stahl-pnordics/OneDrive - SmartStep Consulting GmbH/python/tlv/'

df_comp = pd.read_csv(SAVE_PATH + 'companies.csv', sep=';')
df_comp = df_comp.rename(columns={'0':'name'})
df_comp = df_comp.dropna()

df_reviewers = pd.read_csv(SAVE_PATH + 'reviewers.csv', sep=';')
df_indications = pd.read_csv(SAVE_PATH + 'indications.csv', sep=';')

df_active_drugs = pd.read_csv(SAVE_PATH + 'WHO ATC-DDD 2024-07-31.csv', sep=',')
df_active_drugs = pd.read_csv(SAVE_PATH + 'WHO ATC-DDD 2024-07-31.csv', sep=',')
df_active_drugs = df_active_drugs.drop(columns='note')
df_active_drugs = df_active_drugs.rename(columns ={'atc_code':'ATC', 'atc_name':'name',
                                          'ddd':'DDD','uom':'unit','adm_r':'admin_route'})

df_drugs = pd.read_csv(SAVE_PATH + 'drugs.csv', sep=';')
  
df_hta = pd.read_csv(SAVE_PATH + 'hta.csv', sep=';')

df_comp_drugs = pd.read_csv(SAVE_PATH + 'company_has_prod.csv', sep=';')

df_indication_to_drug = pd.read_csv(SAVE_PATH + 'indication_to_drug.csv', sep=';')
df_drugs_to_hta = pd.read_csv(SAVE_PATH + 'drugs_to_hta.csv', sep=';')
df_reviewer_to_hta = pd.read_csv(SAVE_PATH + 'reviewer_to_file.csv', sep=';')
df_price = pd.read_csv(SAVE_PATH + 'price_new.csv', sep=';')

df_drugs_to_hta = pd.read_csv(SAVE_PATH + 'drugs_to_hta.csv', sep=';')
df_reviewer_to_hta = pd.read_csv(SAVE_PATH + 'reviewer_to_file.csv', sep=';')

df_nt_rec = pd.read_csv(SAVE_PATH + 'nt_radet.csv', sep=';')
df_nt_follow = pd.read_csv(SAVE_PATH + 'nt_follow.csv', sep=';')
df_nt_deal = pd.read_csv(SAVE_PATH + 'nt_deal.csv', sep=';')

df_ema = pd.read_csv(SAVE_PATH + 'EMA.csv', sep=';')
# Insert to db
# add TLV to agencvy
if INIT:
    dh.insert_agency('TLV')
# add companies
dh.insert_companies(df_comp)
# add reviewers
dh.insert_reviewers(df_reviewers, 'TLV')
# add imdications
dh.insert_indications(df_indications)
# add active drug
dh.insert_active_drug(df_active_drugs)
# add products
dh.insert_product(df_drugs)

dh.insert_company_has_product(df_comp_drugs)

dh.insert_product_has_indication(df_indication_to_drug)
# add hta
dh.insert_hta(df_hta, 'TLV')

dh.insert_hta_has_product(df_drugs_to_hta)
dh.insert_hta_has_reviewer(df_reviewer_to_hta)


dh.insert_nt_council_rec(df_nt_rec)
dh.insert_nt_council_follow_up(df_nt_follow)
dh.insert_nt_council_deal(df_nt_deal)

dh.insert_EMA(df_ema)
