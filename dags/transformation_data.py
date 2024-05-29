import pandas as pd
from read_data import read_data

def transformation():
    df = read_data()
    df.dropna(inplace=True)
    df['Year'] = df['Year'].astype(str).str.replace(r'\.0$', '', regex=True)
    df['Duration (min)'] = df['Duration (min)'].astype(int)
    df['Votes'] = df['Votes'].str.replace(',', '').astype(int)
    df['Metascore'] = df['Metascore'].astype(int)
    # df["Certificate"] = df["Certificate"].astype(str).str.replace(r'\W', '', regex=True)
    df.columns = df.columns.str.replace(r"\s+","",regex=True).str.lower()
    return df

print(transformation())
