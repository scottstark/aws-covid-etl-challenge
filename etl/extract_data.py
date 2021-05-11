import pandas as pd


def extract_data(url):
    data = pd.read_csv(url)
    return data
