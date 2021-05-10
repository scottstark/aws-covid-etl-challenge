import json
from datetime import datetime
import boto3
import psycopg2
from m_logger import get_logger

logger = get_logger(__name__)

REGION = "us-east-1"

SQL_CREATE_TABLE = """create table if not exists daily_covid_stat
                    (
                        stat_date date not null
                            constraint daily_covid_stat_pkey
                                primary key,
                        cases integer,
                        deaths integer,
                        recoveries integer,
                        created_date timestamp default CURRENT_TIMESTAMP not null,
                        update_date timestamp default CURRENT_TIMESTAMP not null
                    );"""

SQL_INSERT_STAT = """INSERT INTO daily_covid_stat(stat_date, cases, deaths, recoveries, update_date, created_date)
             VALUES(%(stat_date)s, %(cases)s, %(deaths)s, %(recoveries)s, %(update_date)s, %(created_date)s)
             ON CONFLICT (stat_date)
             DO UPDATE SET cases = %(cases)s, deaths = %(deaths)s, recoveries = %(recoveries)s, 
             update_date = %(update_date)s;"""

SQL_EXISTS = """SELECT to_regclass('daily_covid_stat')"""

SQL_SELECT_LATEST = """SELECT stat_date, cases, deaths, recoveries FROM daily_covid_stat
                        WHERE stat_date = (
                        SELECT MAX (stat_date)
                        FROM daily_covid_stat
                     );"""


def load_data(df_transformed):
    try:
        sm = boto3.client("secretsmanager", REGION)
        get_secret_value_response = sm.get_secret_value(SecretId="postgres-creds")
        secret_string = get_secret_value_response['SecretString']
    except Exception as error:
        logger.error("Error retrieving db credentials from Secrets Manager - {}".format(error))
        raise

    if len(secret_string) == 0:
        logger.error("Error db credentials from Secrets Manager: empty SecretString")
        raise

    conn = None
    row = None
    rows = 0

    try:
        secrets = json.loads(secret_string)
        conn = psycopg2.connect(
            host=secrets["host"],
            database=secrets["dbname"],
            user=secrets["username"],
            password=secrets["password"])

        # Check if daily_covid_stat table already exists
        cur = conn.cursor()
        cur.execute(SQL_EXISTS)
        exists = cur.fetchall()

        # If table does not exist then create one
        if exists[0][0] is None:
            cur.execute(SQL_CREATE_TABLE)
            conn.commit()

        # select the latest stat date
        cur.execute(SQL_SELECT_LATEST)
        row = cur.fetchone()
        if row is not None:
            latest = datetime.strftime(row[0], "%Y-%m-%d")
            df_transformed = df_transformed[df_transformed["date"] > latest]

        # Inserting each row
        for row in df_transformed.itertuples(index=False):
            cdt = datetime.now()
            cur.execute(SQL_INSERT_STAT, (dict(stat_date=row.date,
                                               cases=row.cases,
                                               deaths=row.deaths,
                                               recoveries=row.recoveries,
                                               created_date=cdt,
                                               update_date=cdt)))
            rows = rows + cur.rowcount

        # commit the changes to the database
        conn.commit()

        # select the latest data in db
        if rows > 0:
            cur.execute(SQL_SELECT_LATEST)
            row = cur.fetchone()

        # close communication with the database
        cur.close()
    except Exception as error:
        logger.exception("Load Data Error - {}".format(error))
    finally:
        if conn is not None:
            conn.close()

    return rows, row
