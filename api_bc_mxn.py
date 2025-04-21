# Libraries
import pandas as pd
from datetime import datetime
from sie_banxico import SIEBanxico

start_date = end_date = "2025-04-16"
#start_date = end_date = datetime.today().strftime('%Y-%m-%d')

def token():

    api_token = '5548dff99f5ceb3ea678009baaf1a8a49087cbe9e6976d2ce20f416e599b465a'

    return api_token

def series_udi3y():

    series = ['SF61593','SF61594','SF61592']

    return series

def rename_columns():

    columns = {"fecha":"database","dato":"data","titulo":"description"}
    return columns

def conditions_desc_udibonos(code):
    if code == "Government Securities   Weekly auctions results Allotted amount         3 year Udibonos":
        return "Amount (Udi3y)"
    elif code == "Government Securities   Weekly auctions results Average placement price  3 year Udibonos":
        return "Yield (Udi3y)"
    elif code == "Government Securities   Weekly auctions results Days to maturity        3 year Udibonos":
        return "Days to maturity (Udi3y)"
    else:
        return "Unknown"

def request_udibonos3y():

    api = SIEBanxico(token = token(), id_series = series_udi3y(), language = 'en')

    api.get_lastdata()

    api.get_timeseries_range(init_date=start_date, end_date=end_date)

    data =api.get_timeseries_range(init_date=start_date, end_date=end_date)

    df = pd.json_normalize(data['bmx']['series'],record_path='datos',meta=['idSerie', 'titulo'])

    df['product'] = 'UDIBonos'
    df.drop(columns='idSerie',inplace=True)

    df['titulo'] = df['titulo'].apply(conditions_desc_udibonos)

    df.rename(columns=rename_columns(),inplace=True)

    return df
