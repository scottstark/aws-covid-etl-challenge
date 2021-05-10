import json
import boto3
from notification import send_notification
from transform_data import transform_data
from extract_data import extract_data
from load_data import load_data
from m_logger import get_logger

logger = get_logger(__name__)

REGION = "us-east-1"


def lambda_handler(event, context):
    logger.info("COVID ETL process begin")

    ny_url = get_param_store_val("nytimes-covid-url")
    jh_url = get_param_store_val("johns-hopkins-covid-url")

    if len(ny_url) == 0 or len(jh_url) == 0:
        logger.error("Error retrieving URL values from parameter store: empty value(s)")
        raise

    logger.info("Extracting data")
    ny_data = extract_data(ny_url)
    jh_data = extract_data(jh_url)

    logger.info("Transforming data")
    df_transformed = transform_data(ny_data, jh_data)

    logger.info("Loading stats into Postgres")
    rows, row = load_data(df_transformed)

    msg = "Hi there! COVID ETL process completed with " + str(f"{rows:,}") \
          + " rows processed. \n\n In the US, as of " + row[0].strftime("%m/%d/%Y") \
          + " there have been " + str(f"{row[1]:,}") + " cases, " + str(f"{row[2]:,}") \
          + " deaths and " + str(f"{row[3]:,}") + " recoveries."

    logger.info("Sending notification")
    send_notification(msg)

    logger.info("COVID ETL process end")

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "rows": rows
        })
    }


def get_param_store_val(name):
    try:
        ssm = boto3.client("ssm", REGION)
        parameter = ssm.get_parameter(Name=name, WithDecryption=False)
        value = (parameter['Parameter']['Value'])
        return value
    except Exception as error:
        logger.error("Error getting " + name + " from Parameter Store - {}".format(error))
        raise


if __name__ == "__main__":
    print(lambda_handler(0, 0))
