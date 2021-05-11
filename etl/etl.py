import logging
import json
import boto3
from notification import send_notification
from transform_data import transform_data
from extract_data import extract_data
from load_data import load_data

REGION = "us-east-1"


def lambda_handler(event, context):
    try:
        logger = get_logger(__name__)
        logger.info("COVID ETL process begin")

        ny_url, jh_url = get_param_store_val("nytimes-covid-url", "johns-hopkins-covid-url")

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
    except Exception as e:
        logger.error(e)
        send_notification(str(e))


def get_param_store_val(param1, param2):
    ssm = boto3.client("ssm", REGION)
    parameter = ssm.get_parameter(Name=param1, WithDecryption=False)
    val1 = (parameter['Parameter']['Value'])
    parameter = ssm.get_parameter(Name=param2, WithDecryption=False)
    val2 = (parameter['Parameter']['Value'])
    if len(val1) == 0 or len(val2) == 0:
        raise ValueError("An error occurred parameter empty when calling the GetParameter operation")
    return val1, val2


def get_logger(mod_name):
    logger = logging.getLogger(mod_name)
    if len(logging.getLogger().handlers) > 0:
        """ The Lambda environment pre-configures a handler logging to stderr.
            If a handler is already configured, basicConfig` does not execute.
            Thus we set the level directly. """
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)
    return logger


if __name__ == "__main__":
    print(lambda_handler(0, 0))
