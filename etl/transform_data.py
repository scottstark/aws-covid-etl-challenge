import pandas as pd
from m_logger import get_logger

logger = get_logger(__name__)


def transform_data(df_ny, df_jh):
    try:
        # apply US only filter to Johns Hopkins data
        df_jh = df_jh[df_jh['Country/Region'] == 'US'][['Date', 'Recovered']]
        df_jh['Date'] = pd.to_datetime(df_jh['Date'], format='%Y-%m-%d')
        df_jh['Recovered'] = df_jh['Recovered'].astype('int64')

        df_ny['date'] = pd.to_datetime(df_ny['date'], format='%Y-%m-%d')
        df_ny['cases'] = df_ny['cases'].astype('int64')
        df_ny['deaths'] = df_ny['deaths'].astype('int64')

        # merge the data frames on date/Date
        df_transformed = pd.merge(df_ny, df_jh, left_on="date", right_on="Date")

        # drop the unused columns
        df_transformed.drop(columns=["Date"], inplace=True)

        # rename columns
        df_transformed.columns = ["date", "cases", "deaths", "recoveries"]
        return df_transformed
    except Exception as error:
        logger.error("Error transforming data: - {}".format(error))
        raise
