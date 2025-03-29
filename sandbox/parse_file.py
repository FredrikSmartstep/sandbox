# -*- coding: utf-8 -*-

import fitz
import pandas as pd
import re
from scraping_methods import get_text, get_text_2, get_text_3, get_decision_summary, \
get_clean_block_list, get_blocks_in_between, extract_sentences_with_word, get_next, get_row_info_from_table

import logging
logger = logging.getLogger(__name__)

def parse_file(file, NT_council_basis=False):

    def get_all_tables(doc):
        table_list = []
        table_caps = []
        for page in doc:
            tabs = page.find_tables()
            if tabs.tables:
                for table in tabs:
                    bbox = table.bbox
                    bb2 = tuple([bbox[0], bbox[1]-10, bbox[2], bbox[1]])
                    b_text = page.get_textbox(bb2)
                    table_list.append(table)
                    table_caps.append(b_text)
        return table_list, table_caps

    def get_table_data(pa):
        nonlocal drug, active_drug_name, ATC_code,forms, company, application_type, decision_date,\
             comparator, number_of_patients, annual_turnover, three_part_found, severity

        tabs = pa.find_tables()
        if tabs.tables:
            df = pd.DataFrame(tabs[0].extract())
            if not df.empty:
                drug = get_row_info_from_table(df,'[Pp]rod|Varu', drug)
                #drug = get_row_info_from_table(df,'Varu', drug)
                active_drug_name = get_row_info_from_table(df,'Akt', active_drug_name)
                ATC_code = get_row_info_from_table(df,'ATC', ATC_code)
                forms = get_row_info_from_table(df,'Bered', forms)
                company = get_row_info_from_table(df,'Företag[^ets]', company)
                application_type = get_row_info_from_table(df,'Typ', application_type)
                decision_date = get_row_info_from_table(df,'Sis', decision_date)
                severity = get_row_info_from_table(df,'^Sjukdomens sv.*|^SJUKDOMENS SV.*', severity)
                comparator = get_row_info_from_table(df,'Rele', comparator)
                #if type(comparator)==str:
                #    print(comparator)
                number_of_patients = get_row_info_from_table(df,'Anta|Patientgrupp', number_of_patients)
                annual_turnover = get_row_info_from_table(df,'Terapi', annual_turnover)
                three_part_found = get_row_info_from_table(df,'Tre|SIDO', three_part_found)
                if type(three_part_found)==str:
                    three_part_found = bool(re.search(r'löper|teckn', three_part_found, re.IGNORECASE))
        else:
            logger.info('No table in page')


    doc = fitz.open(file) #  bed2402-22_kymriah_67-2022 bes210923_ontozry_underlag bes220519_evrenzo_3698-2021_underlag bes190926_idacio_underlag
    print('parsing: ' + file)
    df_info_tot = pd.DataFrame(columns={'drug_name':str,'ATC':str,'company':str,'indication':str,
                                       'decision_date':str,'comparators':str,'application_type':str,
                                       'experts':str, 'diarie_nr':str})
    #--------------------------------------------
    # Page 0
    #--------------------------------------------
    page = doc[0]
    try:
        words = page.get_text('words', sort=True)
        blocks = page.get_text('blocks')
        df_words = pd.DataFrame(words)
        df_blocks = pd.DataFrame(blocks)
        block_list = list(df_blocks.iloc[:,4])
        word_list = list(df_words.iloc[:,4])

        if NT_council_basis:
            # check if not a NT/council bsis
            sub = 'subvention' in word_list
            ny = 'Nyansökan' in word_list
            fin = 'FINOSE' in word_list
            en = 'assessment' in word_list
            tech = 'Medicinteknisk' in word_list
            if sub or fin or en or ny or tech:
                logger.info('fuck up')
                return pd.DataFrame(), pd.DataFrame()

        decision_letter = 'Underlag' not in word_list
        # Get indication
        indication = get_blocks_in_between(r'[Ii]ndikation', r'Förslag', block_list, get_last=False)
        if not indication:
            indication = get_text_3('Utvärderad indikation', block_list, pat = r"{} (.*)")
        if not indication:
            indication = get_text('^Utv.*', df_blocks)
        if not indication: 
            indication = get_next('Utvärderad', block_list)
        if not indication:
            indication = get_text('[I|i]ndikation', df_blocks)
            #indication = get_text_3('indikation|Indikation', block_list, pat = r"{} (.*)")
        # Get diarie nr
        word_list = list(df_words.iloc[:,4])
        diarie_nr = word_list[word_list.index('Diarienummer:') + 1] if 'Diarienummer:' in word_list else None
        if not diarie_nr:
            diarie_nr = word_list[word_list.index('Dnr') + 1] if 'Dnr' in word_list else None
        #if not diarie_nr:
        #    diarie_nr = get_next('beteckning', block_list)
    except Exception as e:
        logging.critical(msg='Could not read document', exc_info=True)
        return pd.DataFrame(), pd.DataFrame()
    if decision_letter:
        return pd.DataFrame(), pd.DataFrame()
    # ------------------------------------------------
    # Pages 1-5
    # ------------------------------------------------
    drug = None
    active_drug_name = None
    ATC_code = None
    forms = None
    company = None
    application_type = None
    decision_date = None
    comparator = None
    number_of_patients = None
    annual_turnover = None
    three_part_found = None
    severity = None
    # First, let's try to see if there are tables on pages 1 to 5 and extract info from them
    for i in range(1,5):
        get_table_data(doc[i])
    # get diarie number, company, review committee, board of experts 
    # These should be available for all modi of the report

    # Put together word data
    df_words = pd.DataFrame(words)
    df_blocks = pd.DataFrame(blocks)
    for i in range(1,4):
        page = doc[i]
        words = page.get_text('words', sort=True)
        blocks = page.get_text('blocks')
        df_wo = pd.DataFrame(words)
        df_bl = pd.DataFrame(blocks)
        df_words = pd.concat([df_words, df_wo])
        df_blocks = pd.concat([df_blocks, df_bl])
    block_list = list(df_blocks.iloc[:,4])
    word_list = list(df_words.iloc[:,4])
    if not diarie_nr:
        diarie_nr = word_list[word_list.index('Diarienummer:') + 1] if 'Diarienummer:' in word_list else None
    
    if not drug:
        drug = get_text('^Prod.*|^PROD.*', df_blocks)

    if not drug:
        try:
            index = [idx for idx, s in enumerate(block_list) if 'Produkt' in s][0]
            drug = block_list[index+1].replace('\n',' ')
        except:
            drug = '' 
            #logger.info('Cannot find drug')
    # Split up drug into components if possible
    pattern = r"(.*)\(" 
    if re.match(pattern, drug):
        drug_name = re.findall(pattern, drug)[0].strip()
    else: # if active drug (morphine) is missing
        pattern = r"([^\,]*)\, "
        if re.match(pattern, drug):
            drug_name = re.findall(pattern, drug)[0].strip()
        else:
            drug_name = drug
    drug_name = re.sub(r'Varumärke[ |\n]*','', drug_name)
    if not active_drug_name:
        pattern = r"\((.*)\)" 
        if re.match(pattern, drug):
            active_drug_name = re.match(pattern, drug)[0].strip()
        else:
            active_drug_name = None
    if not ATC_code:
        pattern = r"([A-Z]\d\d[A-Z][A-Z]\d\d)" 
        if re.match(pattern, drug):
            ATC_code = re.findall(pattern, drug)[0].strip()
        else:
            ATC_code = ''

    if not company:
        print('company not found')
        company = get_text('^Företag(?!et)', df_blocks) # only works pre 23
    #drug_name = get_text_2('Varumärke', df_blocks) # only works pre 23
    if not active_drug_name:
        active_drug_name = get_text('^Akti.*', df_blocks) # only works pre 23
    if not ATC_code:
        ATC_code = get_text('^ATC.*', df_blocks) # only works pre 23
    # if not severity:
    #     severity = get_text('^Sjukdomens sv.*|^SJUKDOMENS SV.*', df_blocks) # adjustment needed
    # if not severity:
    #     try:
    #         index = [idx for idx, s in enumerate(block_list) if 'Sjukdomens' in s][0]
    #         severity = block_list[index+1]
    #     except:
    #         severity=''
    #         #logger.info('cannot find severity') 
    if not application_type:
        application_type = get_text('^Typ.*', df_blocks) # only works pre 23
    if not decision_date:
        decision_date = get_text('^Sis.*|^SIS.*', df_blocks)
    if not comparator:
        print('comparator not found')
        comparator = get_text('^Rele.*|^RELE.*', df_blocks) # adjustment needed
    if comparator[0:2]=='jä':
        try:
            index = [idx for idx, s in enumerate(block_list) if 'Relevant' in s][0]
            comparator = block_list[index+1]
        except:
            comparator = ''
            #logger.info('cannot find relevant comp') 
    if not number_of_patients:
        number_of_patients = get_text('^Anta*', df_blocks) # only works pre 23
    if not number_of_patients:
        pattern = r"(\d+\s\d+|\d+)" 
        number_of_patients = re.findall(pattern, number_of_patients)
    
    if three_part_found is None:
        three_part_found = get_text('^Trepa.*', df_blocks)
    
    # May be non-existing
    df_work = pd.DataFrame()
    try:
        index = [idx for idx, s in enumerate(block_list) if 'Arbetsgrupp' in s][0]
        work_group = re.split(',| och ', block_list[index][block_list[index].startswith('Arbetsgrupp:') and len('Arbetsgrupp:'):])
        pattern = r"\((.*)\)" 
        titles = []
        for w in work_group:
            if re.findall(pattern, w):
                title = re.findall(pattern, w)[0]
            else:
                title = ''
            titles.append(title)
        #titles = [re.findall(pattern, w)[0] for w in work_group if re.findall(pattern, w)]
        pattern = r"([^\()]*)" 
        names = [re.findall(pattern, w)[0].strip() for w in work_group]
        df_work = pd.DataFrame({'name':names, 'title':titles, 'diarie_nr': diarie_nr})
        # index = [idx for idx, s in enumerate(block_list) if 'Arbetsgrupp' in s][0]
        # work_group = re.split(', | och ', block_list[index][block_list[index].startswith('Arbetsgrupp:') and len('Arbetsgrupp:'):])
        # pattern = r"\((.*)\)" 
        # titles = []
        # for w in work_group:
        #     if re.findall(pattern, w):
        #         title = re.findall(pattern, w)[0]
        #     else:
        #         title = ''
        # titles.append(title)
        # pattern = r"(.*)\(" 
        # names = [re.findall(pattern, w)[0].strip() for w in work_group]
        # df_work = pd.DataFrame({'name':names, 'title':titles, 'diarie_nr': diarie_nr})
        #index = [idx for idx, s in enumerate(block_list) if (('Kliniska' in s) or ('Klinisk' in s))][0]
        #logger.info(block_list[index])
    except:
        df_work = pd.DataFrame()
        #logger.info('cannot find workgroup here') 
    
    try:
        index = [idx for idx, s in enumerate(block_list) if bool(re.search(r'Kliniska? exp',s,re.IGNORECASE))][0]
        #pattern = r"([^.!?]*)"
        #expert = re.findall(pattern, block_list[index])[0]
        expert = block_list[index].replace('\n','').replace('-','')
        #expert_group = re.split(', | och ', expert).strip()#block_list[index][block_list[index].startswith('Kliniska experter:') and len('Kliniska experter:'):])
    except:
        expert = ''
        logger.info('cannot find expert here') 
    
    if not company:
        try:
            company = word_list[word_list.index('Företag:') + 1]
        except Exception as e:
            company = ''
            #logger.info('cannot find company here') 
    if not diarie_nr:
        try:
            diarie_nr = word_list[word_list.index('Diarienummer:') + 1]
        except Exception as e:
            diarie_nr = ''
            logger.info('cannot find diarienummer') 
    
    
    # parse application type
    resubmission = None 
    changed_decision = None
    new_indication = None
    new_form = None
    new_strength = None
    temporary = None
    removed = None
    new_price = None

    if application_type:
        pattern = r'[ingår sedan|finns]'
        resubmission = bool(re.search(pattern, application_type, re.IGNORECASE))
        pattern = r'ändring'
        changed_decision = bool(re.search(pattern, application_type, re.IGNORECASE))
        pattern = r'ny[a]? indikation'
        new_indication = bool(re.search(pattern, application_type, re.IGNORECASE))
        pattern = r'ny[a]? bered'
        new_form = bool(re.search(pattern, application_type, re.IGNORECASE))
        pattern = r'ny[a]? styrk'
        new_strength = bool(re.search(pattern, application_type, re.IGNORECASE))  
        pattern = r'pris'
        new_price = bool(re.search(pattern, application_type, re.IGNORECASE))
        pattern = r'utgå'
        removed = bool(re.search(pattern, application_type, re.IGNORECASE))

    # -------------------------------------------
    # The rest of the document
    # -------------------------------------------
    costs_total_comp = None 
    QALY_comp = None 
    ICER_comp = None 
    costs_total_HTA = None 
    QALY_HTA = None 
    ICER_HTA = None
    delta_cost_comp = None

    tables, captions = get_all_tables(doc)
    for i,c in enumerate(captions):
        # TODO: There may be more than one scenario/subinidcation. Needs to be handled
        if bool(re.search(r'företag', c, re.I)) & bool(re.search(r'grund', c, re.I)):           
            df = pd.DataFrame(tables[i].extract())
            if not df.empty:
                #comparator_used_comp = 
                costs_total_comp = get_row_info_from_table(df,'Kostnader, totalt|Totala kostnader', costs_total_comp)
                QALY_comp = get_row_info_from_table(df,'QALYs', QALY_comp)
                ICER_comp = get_row_info_from_table(df,'Kostnad[er]* per', ICER_comp)
                delta_cost_comp = get_row_info_from_table(df,'[Ss]killnad', costs_total_comp)
                
        if bool(re.search(r'TLV', c, re.I)) & bool(re.search(r'grund', c, re.I)):           
            df = pd.DataFrame(tables[i].extract())
            if not df.empty:
                costs_total_HTA = get_row_info_from_table(df,'Kostnader, totalt|Totala kostnader', costs_total_HTA)
                QALY_HTA = get_row_info_from_table(df,'QALYs', QALY_HTA)
                ICER_HTA = get_row_info_from_table(df,'Kostnad[er]* per', ICER_HTA)
        
        if bool(re.search(r'.*företag', c, re.I)) & bool(re.search(r'.*kostnads', c, re.I)):           
            df = pd.DataFrame(tables[i].extract())
            if not df.empty:
                # Forst try to find it in row then in column, since the tables are constructed differently
                delta_cost_comp = get_row_info_from_table(df,'[Ss]killnad', costs_total_comp)
                
    b_clean_list = get_clean_block_list(doc)
    b_dirty_list = get_clean_block_list(doc, True)
    total_text = chr(12).join([c.replace('\n','').replace('-','') for c in b_clean_list])
    decision_summary = get_blocks_in_between(r'TLV:s bedömning|TLV:s centrala|TLV:s sammanfattning|TLV:s slutsatser', r'Innehållsf|\.{5}[ ]?\d', b_dirty_list, get_last=False)
    if not decision_summary: # post -23 and pre-19?
        decision_summary = get_blocks_in_between(r'Samlad bedömning|Sammanfattning', r'^\d |^\d\d|Se nedan hur man överklagar', b_clean_list, get_last=False)
    # Three-part negotiation? 
    if three_part_found=='':
        three_part_found = bool(len(re.findall('trepart', decision_summary)))
    # Cost-effectiveness or cost minimization?
    # Look for chapter called Hälsoekonomi samt Resultat
    CEA = None
    CBA = None
    CM = None
    
    type_of_analysis = 'CEA'
    HE_text = get_blocks_in_between(r'Hälsoekonomi', r'\d[ ]+Resultat', b_clean_list, get_last=False)

    CEA_pattern = r'kostnadseffektiv|kostnadsnytto|kostnaden per [vunnet ]*[kvalitetsjusterat levnadsår|QALY]'
    CM_pattern = r'kostnadsmi|kostnadsjämförelse'
    
    if bool(re.search(CEA_pattern, decision_summary)):
        CEA = True
    if bool(re.search(CM_pattern, decision_summary)):
        CM = True
    
    if (not CEA) & (not CM): # Let's check the first sentence in chapter HE
        if bool(re.search(CEA_pattern, HE_text)):
            CEA = True
        if bool(re.search(CM_pattern, HE_text)):
            CM = True
    if CM:
        type_of_analysis = 'CM' 
    if CEA:
        type_of_analysis = 'CEA' 
    if bool(CEA) & bool(CM):
        type_of_analysis = 'Check'

    if not severity:
        severity_texts = extract_sentences_with_word(decision_summary, 'svårighetsgrad')
        if not severity_texts:
            severity_texts = extract_sentences_with_word(total_text, 'svårighetsgrad')
        if len(severity_texts)>0:
            severity_texts = [c for c in severity_texts if not re.match(r'En högre kostnad per QALY|^[\x0c| ]*\d',c, re.I)]
            severity = chr(12).join([c.replace('\n|\x0c','') for c in severity_texts])

    qaly_texts = extract_sentences_with_word(decision_summary, 'QALY')
    if not qaly_texts:
        qaly_texts = extract_sentences_with_word(decision_summary, 'kvalitetsjusterat')
    qaly_text = chr(12).join([c.replace('\n','') for c in qaly_texts])
    qaly_texts_comp = [t for t in qaly_texts if bool(re.search(r'företag',t,re.IGNORECASE))]
    qaly_text_comp = chr(12).join([c.replace('\n','') for c in qaly_texts_comp])
    if not ICER_comp:
        print('comp ICER not found')
        try:
            ICER_comp = re.findall(r'\d+[\,]?\d+', qaly_text_comp)[0]
        except:
            print('No ICER comp')
    qaly_texts_TLV = [t for t in qaly_texts if bool(re.search(r'TLV',t,re.IGNORECASE))]
    qaly_text_TLV = chr(12).join([c.replace('\n','') for c in qaly_texts_TLV])
    if not ICER_HTA:
        try:
            ICER_HTA = re.findall(r'\d+[\,]?\d+', qaly_text_TLV)[0]
        except:
            try:
                ICER_HTA = re.findall(r'\d+[\,]?\d+', qaly_text)[0]
            except:
                print('No ICER HTA')
    
    indirect_comp = bool(len(re.findall('indirekta jämföre|indirekt jämför', decision_summary, re.IGNORECASE)))

    biosim = bool(len(re.findall('biosim', decision_summary, re.IGNORECASE)))

    df_information = pd.DataFrame([{'drug_name':drug_name,'ATC':ATC_code,'active substance':active_drug_name,'company':company,
                                    'indication':indication,'severity':severity,'three_part_deal':three_part_found,
                                        'forms': forms,'strengths':None, 'annual_turnover':annual_turnover,
                                        'decision_date':decision_date,'comparators':comparator,'application_type':application_type,
                                        'experts':expert, 'diarie_nr':diarie_nr, 'decision_summary':decision_summary,
                                        'type_of_analysis':type_of_analysis, 'CEA': CEA, 'CM': CM,
                                        'application_text':'',
                                        'indirect_comp':indirect_comp,
                                        'costs_total_comp':costs_total_comp,'QALY_comp': QALY_comp,
                                        'ICER_comp':ICER_comp,'costs_total_HTA':costs_total_HTA,
                                        'QALY_HTA':QALY_HTA,'ICER_HTA':ICER_HTA,'delta_cost_comp':delta_cost_comp,
                                        'biosim': biosim,
                                        'resubmission':resubmission ,'changed_decision':changed_decision,
                                        'new_indication':new_indication,'new_form':new_form, 'new_price':new_price,
                                        'new_strength':new_strength,'temporary':temporary,'removed':removed}])

    df_info_tot = pd.concat([df_info_tot, df_information], ignore_index=True)
    return df_information, df_work