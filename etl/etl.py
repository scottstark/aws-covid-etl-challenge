import json

from notification import send_notification
from transform_data import transform_data
from extract_data import extract_ny_data, extract_jh_data
from load_data import load_data
from m_logger import get_logger

logger = get_logger(__name__)


def lambda_handler(event, context):
    logger.info("COVID ETL process begin")

    logger.info("Extracting data")
    df_ny = extract_ny_data()
    df_jh = extract_jh_data()

    logger.info("Transforming data")
    df_transformed = transform_data(df_ny, df_jh)

    logger.info("Loading stats into Postgres")
    rows, row = load_data(df_transformed)

    msg = "Hi there! COVID ETL process completed with " + str(f"{rows:,}") \
          + " rows processed. \n\n In the US, as of " + row[0].strftime("%m/%d/%Y") \
          + " there have been " + str(f"{row[1]:,}") + " cases, " + str(f"{row[2]:,}") \
          + " deaths and " + str(f"{row[3]:,}") + " recoveries."

    logger.info("Sending notification")
    send_notification(msg)

    logger.info("COVID ETL processing pipeline end")

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "rows": rows
        })
    }


if __name__ == "__main__":
    print(lambda_handler(0, 0))
