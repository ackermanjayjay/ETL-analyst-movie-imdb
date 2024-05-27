import pandas as pd

def read_data():
    data = pd.read_csv("dags/data/imdb-movies-dataset.csv")
    return data



