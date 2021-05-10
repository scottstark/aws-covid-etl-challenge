import boto3
import pandas as pd
from m_logger import get_logger

logger = get_logger(__name__)

REGION = "us-east-1"


def extract_ny_data():
    try:
        ssm = boto3.client("ssm", REGION)
        parameter = ssm.get_parameter(Name='nytimes-covid-url', WithDecryption=False)
        ny_url = (parameter['Parameter']['Value'])
    except Exception as error:
        logger.error("Error getting NY Times URL from Parameter Store - {}".format(error))
        raise

    if len(ny_url) == 0:
        logger.error("Error getting NY Times URL: empty value")
        raise

    try:
        ny_data = pd.read_csv(ny_url)
    except Exception as error:
        logger.error("Error downloading NY Times data: - {}".format(error))
        raise

    if len(ny_data) == 0:
        logger.error("Error downloading NY Times data: empty list")
        raise

    return ny_data


def extract_jh_data():
    try:
        ssm = boto3.client("ssm", REGION)
        parameter = ssm.get_parameter(Name='johns-hopkins-covid-url', WithDecryption=False)
        jh_url = (parameter['Parameter']['Value'])
    except Exception as error:
        logger.error("Error getting Johns Hopkins URL from Parameter Store - {}".format(error))
        raise

    if len(jh_url) == 0:
        logger.error("Error getting Johns Hopkins URL: empty value")

    try:
        jh_data = pd.read_csv(jh_url)
    except Exception as error:
        logger.error("Error downloading Johns Hopkins data!- {}".format(error))
        raise

    if len(jh_data) == 0:
        logger.error("Error downloading Johns Hopkins data: empty list")
        raise

    return jh_data
