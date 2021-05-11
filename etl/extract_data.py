import pandas as pd


def extract_data(url, name):
    data = pd.read_csv(url)
    data.name = name
    return data
