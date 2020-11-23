import glob
import pandas as pd

def read_csvfile(path):
    df = []
    count = 0
    for file in sorted(glob.iglob(path)):
        if count == 0:
            df = pd.read_csv(file, encoding = "ISO-8859-1", header = 0)
        else:
            df = df.append(pd.read_csv(file, encoding = "ISO-8859-1", header = 0))
        count+=1
    return df

def replace_column(df, column, val, new_val):
    df[column] = df[column].str.replace(val, new_val)

def trim_columns(df):
    for column in df:
        df[column] = df[column].str.strip()
    return df

def set_str(df, column):
    print(df[column])
    df[column] = df[column].astype(str) 

def set_int_str(df, column):
    df[column] = df[column].fillna(0)
    df[column] = df[column].astype(int).astype(str) 

def clean_logistic(df):
    
    replace_column(df, 'origen', chr(202), ' ')
    replace_column(df, 'destino', chr(202), ' ')
    set_int_str(df, 'placa_rastra')
    trim_columns(df)

def getWarehouses(df, index_start='origen', index_end='destino'):
    hq = (pd.concat([pd.DataFrame([x.strip(' ') for x in sorted(set(map(str,df[index_start])))]),
                    pd.DataFrame([x.strip(' ') for x in sorted(set(map(str,df[index_end])))])])
        .drop_duplicates()
        .rename(columns={0: 'Warehouses'})
        .sort_values(by='Warehouses')
        .reset_index(drop=True))
    return hq

def getRemisionesCases(df1, df2, df3):
    def partialDF(df, name):
        df = (pd.DataFrame([x.strip(' ') for x in sorted(set(map(str,df['remision'])))])
                 .rename(columns={0: 'remision'})
                 .set_index('remision'))
        df[name] = True
        return df
    
    def getCase(row):
        if (row['cocacola'] and row['logistic_water']) or (row['cocacola'] and row['logistic_soda']):
            return 'Case 1'
        elif (row['cocacola'] and ~row['logistic_water']) or (row['cocacola'] and ~row['logistic_soda']) :
            return 'Case 2'
        else:
            return 'Case 3'
    
    df1unique = partialDF(df1, 'cocacola')
    df2unique = partialDF(df2, 'logistic_water')
    df3unique = partialDF(df3, 'logistic_soda')
    
    dfList = [df1unique, df2unique, df3unique]
    df = pd.concat(dfList, axis=1, sort=False).fillna(False)
    df['case'] = df.apply(lambda x: getCase(x), axis=1)
                  
    return df
    
