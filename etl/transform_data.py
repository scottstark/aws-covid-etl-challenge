import pandas as pd


def transform_data(ny_data, jh_data):
    validate_data(ny_data, jh_data)

    # apply US only filter to Johns Hopkins data
    jh_data = jh_data[jh_data['Country/Region'] == 'US'][['Date', 'Recovered']]
    jh_data['Date'] = pd.to_datetime(jh_data['Date'], format='%Y-%m-%d')
    jh_data['Recovered'] = jh_data['Recovered'].astype('int64')

    ny_data['date'] = pd.to_datetime(ny_data['date'], format='%Y-%m-%d')
    ny_data['cases'] = ny_data['cases'].astype('int64')
    ny_data['deaths'] = ny_data['deaths'].astype('int64')

    # merge the data frames on date/Date
    df_transformed = pd.merge(ny_data, jh_data, left_on="date", right_on="Date")

    # drop the unused columns
    df_transformed.drop(columns=["Date"], inplace=True)

    # rename columns
    df_transformed.columns = ["date", "cases", "deaths", "recoveries"]
    return df_transformed


def validate_data(ny_data, jh_data):
    if len(ny_data) == 0:
        raise ValueError("NY Times empty data file")

    if len(jh_data) == 0:
        raise ValueError("Johns Hopkins empty data file")

    if not check_columns(ny_data, ["date", "cases", "deaths"]):
        raise ValueError("NY Times file format error: incorrect columns")

    if not check_columns(jh_data, ["Date", "Recovered", "Country/Region"]):
        raise ValueError("Johns Hopkins file format error: incorrect columns")


def check_columns(data, col_list):
    for col_name in col_list:
        if col_name not in data.columns:
            return True
    return True
