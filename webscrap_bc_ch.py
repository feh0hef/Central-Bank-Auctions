# Libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def request_chile_auction_info():
    """Scrapes the latest auction results table from the Hacienda Chile website"""

    url = 'https://www.hacienda.cl/areas-de-trabajo/finanzas-internacionales/oficina-de-la-deuda-publica/colocaciones-bcch-soma/resultados-ultima-licitacion'

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  
        response.encoding = 'utf-8'
    except requests.exceptions.RequestException as e:
        print(f"Connection error: {e}")
        return pd.DataFrame()

   
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')

    if table is None:
        print("Error: Table not found.")
        return pd.DataFrame()

    
    headers = [th.text.strip() for th in table.find_all('th')]

   
    rows = []
    for tr in table.find_all('tr')[1:]:
        cells = [td.text.strip() for td in tr.find_all('td')]
        if cells:
            rows.append(cells)


    df_chile = pd.DataFrame(rows, columns=headers)
    return df_chile


def main():
    """Main function to run Chile auction scraper and save data"""

    df_chile = request_chile_auction_info()

    if not df_chile.empty:
        today_str = datetime.today().strftime("%Y-%m-%d")
        file_name = f"resultados_licitacion_chile_{today_str}.csv"
        df_chile.to_csv(file_name, index=False, encoding="utf-8-sig")
        print(f"Data saved to '{file_name}'")
    else:
        print("No data retrieved.")


# Run the script
if __name__ == "__main__":
    main()