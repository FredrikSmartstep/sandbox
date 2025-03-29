import re
import pandas as pd

def get_text(letters, df_blocks):
    try:
        text = df_blocks[df_blocks.iloc[:,4].str.contains(letters)].iloc[:,4].values[0]
        pattern = r"\n(.*)" 
        return re.findall(pattern, text, flags=re.DOTALL)[0].strip() 
    except:
        #print('Cannot find: ' + letters)
        return ''
    
def get_text_2(letters, df_blocks):
    try:
        text = df_blocks[df_blocks.iloc[:,4].str.contains(letters)].iloc[:,4].values[0]
        text = text.replace("\n", "")
        #print(text)
        pattern = r"{} (.*)".format(letters) 
        return re.findall(pattern, text, flags=re.DOTALL)[0].strip() 
    except:
        #print('Cannot find: ' + letters)
        return ''
    
def get_decision_summary(page):
    words = page.get_text('words', sort=True)
    text = page.get_text('text')
    df_words = pd.DataFrame(words)
    word_list = list(df_words.iloc[:,4])
    # Check if decision summary
    decision_summary = ''
    pattern = r"(TLV.*)"
    if bool(re.search(r'TLV:s|TLV:S', word_list[0])):
        decision_summary = re.findall(pattern, text, flags=re.DOTALL)[0]
    return decision_summary

def get_clean_block_list(doc):
    df_blocks_tot = pd.DataFrame()
    for p in doc:
        blocks = p.get_text('blocks')
        df_blocks = pd.DataFrame(blocks)
        df_blocks_tot = pd.concat([df_blocks_tot,df_blocks], ignore_index=True)
    return [b for b in list(df_blocks_tot.iloc[:,4]) if len(b.strip())>3 and 'Dnr' not in b]

def get_blocks_in_between(start, end_pattern, b_clean_list, get_last=True):
    ind = [i for i,s in enumerate(b_clean_list) if bool(re.search(start, s, re.I))]
    tot_text = ''
    if ind:
        if get_last:
            start_pos = ind[-1]
        else:
            start_pos = ind[0]
        j = start_pos + 1
        tot_text = ''
        while (not bool(re.search(end_pattern, b_clean_list[j], re.I))):
            tot_text = ''.join([tot_text, b_clean_list[j]])
            j+=1
    return tot_text

def extract_sentences_with_word(text, word):
    """
    Extracts sentences containing a specified word from a given text using regular expressions.
    
    Args:
    text (str): The text from which to extract sentences.
    word (str): The word to look for within the sentences.
    
    Returns:
    list: A list of sentences containing the word.
    """
    # Regular expression pattern to split text into sentences
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)
    
    # Prepare the word pattern for matching, ensuring it is treated as a whole wor
    # Actually we may want partials matched as well
    word_pattern = re.escape(word)# r'\b' + re.escape(word) + r'\b'
    
    # Find sentences containing the word (case insensitive)
    matching_sentences = [sentence for sentence in sentences if re.search(word_pattern, sentence, re.IGNORECASE)]
    
    return matching_sentences
def get_text(letters, df_blocks):
    try:
        text = df_blocks[df_blocks.iloc[:,4].str.contains(letters)].iloc[:,4].values[0]
        pattern = r"\n(.*)" 
        return re.findall(pattern, text, flags=re.DOTALL)[0].strip() 
    except:
        #print('Cannot find: ' + letters)
        return ''
   
def get_text_2(letters, df_blocks):
    try:
        text = df_blocks[df_blocks.iloc[:,4].str.contains(letters)].iloc[:,4].values[0]
        text = text.replace("\n", "")
        pattern = r"{} (.*)".format(letters) 
        return re.findall(pattern, text, flags=re.DOTALL)[0].strip() 
    except:
        #print('Cannot find: ' + letters)
        return ''

def get_text_3(letters, block_list, pat = r"{} (.*)"):
    try:
        index = [idx for idx, s in enumerate(block_list) if letters in s]
        #print('Index: ' + str(index))
        text = block_list[index[0]]
        #text = text.replace("\n", "")
        #print(text)
        pattern = pat.format(letters) 
        return re.findall(pattern, text, flags=re.DOTALL)[0].strip() 
    except:
        print('Cannot find: ' + letters)
        return ''
    
def get_text_4(letters, df_blocks):
    try:
        text = df_blocks[df_blocks.iloc[:,4].str.contains(letters)].iloc[:,4].values[0]
        pattern = r"\n([\s\S]*)" 
        return re.findall(pattern, text, flags=re.DOTALL)[0].strip() 
    except:
        #print('Cannot find: ' + letters)
        return ''

def get_next(word, block_list, pattern=r'(.*)'):
    index = [idx for idx, s in enumerate(block_list) if word in s]
    if index:
        if re.findall(pattern, block_list[index[0]+1], flags=re.DOTALL):
            return re.findall(pattern, block_list[index[0]+1], flags=re.DOTALL)[0].strip() 
    return ''
    
def get_date(word, block_list):
    index = [idx for idx, s in enumerate(block_list) if word in s]
    if index and index[0]<(len(block_list)-1): 
        #print(block_list[index[0]+1])
        pattern = r"([^\n]*)" 
        return re.findall(pattern, block_list[index[0]+1], flags=re.DOTALL)[0].strip() 
    else:
        return ''
    
def get_line(letters, df_blocks):
    try:
        text = df_blocks[df_blocks.iloc[:,4].str.contains(letters)].iloc[:,4].values[0]
        #print(text.replace("\n", ""))
        pattern = r"([^\n]*)\n" 
        # problem is that there may be leading /n:s before 
        matches = re.findall(pattern, text, flags=re.DOTALL)
        #print(len(matches))
        return get_next(letters, matches)
    except:
        #print('Cannot find: ' + letters)
        return ''

def get_next_line(letters, block_list):
    try:
        index = [idx for idx, s in enumerate(block_list) if letters in s]
        if index: 
            text = block_list[index[0]+1]
            pattern = r"([^\n]*)" 
            return re.findall(pattern, text, flags=re.DOTALL)[0].strip() 
        else:
            return ''
    except:
        #print('Cannot find: ' + letters)
        return ''

def get_drug_from_table(tab):
    drug_name = ''
    for table in tab.tables:
                df_tab = table.to_pandas()
                if 'Namn' in df_tab.columns:
                    df_tab = df_tab.dropna(subset=['Namn'], ignore_index=True)
                    if not df_tab.empty:
                        drug_name = df_tab['Namn'][0] 
    return drug_name

def get_info_from_table(tab, col_name):
    form = ''
    for table in tab.tables:
                df_tab = table.to_pandas()
                if col_name in df_tab.columns:
                    df_tab = df_tab.dropna(subset=[col_name], ignore_index=True)
                    if not df_tab.empty:
                        form = list(df_tab[col_name]) 
                        form = '|'.join(form)
    return form

def get_row_info_from_table(df, pattern, var):
    if not var:
        pos = df[0].str.contains(pattern, regex=True, flags=re.IGNORECASE, na=False)
        if (not df[pos].dropna(axis=1).empty) & (len(df[pos].dropna(axis=1).columns)>1):#sum(pos)>0:
            return df[pos].dropna(axis=1).iloc[0,-1]#df.loc[pos][1].dropna().iloc[0]
    return var

def get_clean_block_list(doc, include_TOC=False):
    df_blocks_tot = pd.DataFrame()
    for p in doc:
        df_words = pd.DataFrame(p.get_text('words'))
        if not df_words.empty:
            words = list(df_words.iloc[:,4])
            not_TOC = [1 for w in words if bool(re.search(r'Innehållsförteckning', w, re.I))]
            if len(not_TOC)==0 or include_TOC:
                blocks = p.get_text('blocks')
                df_blocks = pd.DataFrame(blocks)
                df_blocks_tot = pd.concat([df_blocks_tot,df_blocks], ignore_index=True)
            #else:
            #    print('skipping TOC')
    return [b for b in list(df_blocks_tot.iloc[:,4]) if len(b.strip())>3 and 'Dnr' not in b]

def get_blocks_in_between(start, end_pattern, b_clean_list, get_last=True):
    ind = [i for i,s in enumerate(b_clean_list) if bool(re.search(start, s, re.I))]
    tot_text = ''
    if ind:
        if get_last:
            start_pos = ind[-1]
        else:
            start_pos = ind[0]
        j = start_pos + 1
        tot_text = b_clean_list[start_pos]
        while (not bool(re.search(end_pattern, b_clean_list[j], re.I))):
            tot_text = ''.join([tot_text, b_clean_list[j]])
            j+=1
            if j==len(b_clean_list): # to handle the case that the first text is found but not the latter
                return ''
    return tot_text

def get_decision_summary(page):
    words = page.get_text('words', sort=True)
    text = page.get_text('text')
    df_words = pd.DataFrame(words)
    word_list = list(df_words.iloc[:,4])
    # Check if decision summary
    decision_summary = ''
    pattern = r"(TLV.*)"
    if bool(re.search(r'TLV:s|TLV:S', word_list[0])):
        decision_summary = re.findall(pattern, text, flags=re.DOTALL)[0]
    return decision_summary

def extract_sentences_with_word(text, word):
    """
    Extracts sentences containing a specified word from a given text using regular expressions.
    
    Args:
    text (str): The text from which to extract sentences.
    word (str): The word to look for within the sentences.
    
    Returns:
    list: A list of sentences containing the word.
    """
    # Regular expression pattern to split text into sentences
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)
    
    # Prepare the word pattern for matching, ensuring it is treated as a whole wor
    # Actually we may want partials matched as well
    word_pattern = re.escape(word)# r'\b' + re.escape(word) + r'\b'
    
    # Find sentences containing the word (case insensitive)
    matching_sentences = [sentence for sentence in sentences if re.search(word_pattern, sentence, re.IGNORECASE)]
    
    return matching_sentences