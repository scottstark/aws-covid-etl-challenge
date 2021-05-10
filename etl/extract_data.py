import pandas as pd
from m_logger import get_logger

logger = get_logger(__name__)

REGION = "us-east-1"


def extract_data(url):
    try:
        data = pd.read_csv(url)
    except Exception as error:
        logger.error("Error downloading data: - {}".format(error))
        raise
    return data


def extract_ny_data(url):
    try:
        ny_data = pd.read_csv(url)
        if not check_columns(ny_data, ["date", "cases", "deaths"]):
            return "NY Times file format error: incorrect columns"

    except Exception as error:
        logger.error("Error downloading NY Times data: - {}".format(error))
        raise

    if len(ny_data) == 0:
        logger.error("Error downloading NY Times data: empty list")
        raise

    return ny_data


def extract_jh_data(url):
    try:
        jh_data = pd.read_csv(url)
        if not check_columns(jh_data, ["Date", "Recovered", "Country/Region"]):
            return "Johns Hopkins file format error: incorrect columns"
    except Exception as error:
        logger.error("Error downloading Johns Hopkins data!- {}".format(error))
        raise

    if len(jh_data) == 0:
        logger.error("Error downloading Johns Hopkins data: empty list")
        raise

    return jh_data


def check_columns(data, col_list):
    for col_name in col_list:
        if col_name not in data.columns:
            return True
    return True
