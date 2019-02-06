import os
import re
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

def get_stem_table(url):
    lists = []
    html = requests.get(url)
    soup = BeautifulSoup(html.content, "lxml")
    table = soup.find('table', {'class':'stemTable'})
    rows = table.find_all('tr')[1:]
    l = []
    for tr in rows:
        td = tr.find_all('td')
        # Only import stem names not in a subgroup
        if (tr.find('td', {'class':'sg'}) == None and tr.find('td', {'class':'sg2'}) == None):
            row = [tr.text for tr in td]
            l.append(row)
    df = pd.DataFrame(l, columns=['stem', 'definition', 'examples'], index=None)
    return df

def clean_stem_table(df):
    df.drop(['examples'], axis=1, inplace=True)
    # Further remove stem names in a subgroup
    df = df[df['definition'].str.contains('\\(see.*\\)')==False]
    df = df[df['definition'].str.contains('See.*')==False]
    # Further clean up information within the brackets and after ;
    df = df.replace(' \\(.*\\)','', regex=True).replace('\\(.*\\)','', regex=True)
    df = df.replace('.*;(.*)','', regex=True)
    # Split (explode) pandas dataframe string entry to separate rows
    lst_col = 'stem'
    x = df.assign(**{lst_col:df[lst_col].str.split(',')})
    df1 = pd.DataFrame({
        col:np.repeat(x[col].values, x[lst_col].str.len())
        for col in x.columns.difference([lst_col])
    }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]
    return df1

def regex_pattern(stem_str):
    str_list = stem_str.split('-')
    # add word boundary
    if len(str_list) == 2:
        pos = len(str_list)-str_list.index('')*2
        str_list.insert(pos, '\\b')
    # replace '-' with '.*'
    pat_str = ''.join([i if len(i) > 0 else '.*' for i in str_list])
    return pat_str

def regex_file(url):
    df = get_stem_table(url)
    df = clean_stem_table(df)
    df.stem = df.stem.str.replace(' ','')
    df['regex'] = df.stem.apply(regex_pattern)
    return df

def rxgen_class(regex_df, df, gen_colname):
    to_repl = regex_df.regex.values.tolist()
    vals = regex_df.definition.values.tolist()
    df['class'] = df[gen_colname].replace(to_repl, vals, regex=True)
    return df

if __name__ == "__main__":
    url = 'https://druginfo.nlm.nih.gov/drugportal/jsp/drugportal/DrugNameGenericStems.jsp'
    regex_df = regex_file(url)
