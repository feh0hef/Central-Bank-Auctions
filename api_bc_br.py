#Libraries
import requests
import numpy as np
import psycopg2 as pg
import pandas as pd
import pprint
import json
from datetime import date, datetime

#Confg Main
format_file ="json"
EndDate = StartDate = datetime.today().strftime('%Y-%m-%d')

def conditions_gov_bondsBR(x):

    if x == "210100": return "LFT"
    elif x == "760197" or x == "760197": return "NTN-B"
    elif x == "100000": return "LTN"
    elif x == "950199" or x == "950198": return "NTN-F"


#Conections with API

def request_SCS_info(StartDate = StartDate, EndDate = EndDate, format_file = format_file):
    """Resquest Data of SCS in Central Bank Brazil API"""

    link = f"https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesContratosSwap(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,edital=@edital,tipoPublico=@tipoPublico,dataVencimento=@dataVencimento,tipoOferta=@tipoOferta)?@dataMovimentoInicio='{StartDate}'&@dataMovimentoFim='{EndDate}'&$top=100&$format={format_file}&$select=dataMovimento,prazo,quantidadeOfertada,quantidadeAceita,datavencimento,cotacao,taxaLinear"

    req = requests.get(link)

    try:
        
        test_conx = str(req)

        if test_conx == "<Response [200]>":

            consult = json.loads(req.text)

            pprint.pprint(consult)

        dataframe_scs = pd.DataFrame(consult['value'])

        dataframe_scs["dataMovimento"] = pd.to_datetime(dataframe_scs["dataMovimento"]).dt.date

        dataframe_scs["datavencimento"] = pd.to_datetime(dataframe_scs["datavencimento"]).dt.date
       
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}\nTry again later.")
        raise

    return dataframe_scs


def request_Gov_BondsBR_info(StartDate = StartDate, EndDate = EndDate, format_file = format_file):
    """Resquest Data of Gov Bonds in Central Bank Brazil API"""

    link = f"https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?@dataMovimentoInicio='{StartDate}'&@dataMovimentoFim='{EndDate}'&$top=100&$format={format_file}&$select=dataMovimento,prazo,quantidadeOfertada,quantidadeAceita,codigoTitulo,dataVencimento,cotacaoCorte,taxaCorte,financeiro"

    req = requests.get(link)

    try:
        
        test_conx = str(req)

        if test_conx == "<Response [200]>":

            consult = json.loads(req.text)

            pprint.pprint(consult)

            dataframe_gov_bondsBR = pd.DataFrame(consult['value'])
        
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}\nTry again later.")
        raise

    if dataframe_gov_bondsBR.empty:
        print("Error with data\nTry again later.")

    else:
        dataframe_gov_bondsBR["Titulo"] = dataframe_gov_bondsBR["codigoTitulo"].apply(conditions_gov_bondsBR)

        dataframe_gov_bondsBR.drop(columns="codigoTitulo")

        dataframe_gov_bondsBR["dataMovimento"] = pd.to_datetime(dataframe_gov_bondsBR["dataMovimento"]).dt.date

        dataframe_gov_bondsBR["dataVencimento"] = pd.to_datetime(dataframe_gov_bondsBR["dataVencimento"]).dt.date

    return dataframe_gov_bondsBR
