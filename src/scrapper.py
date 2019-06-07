import requests
from bs4 import BeautifulSoup
import pandas as pd


def recover(city):
    """ Recover cities by IBGE code
        -- city: IBGE code
    """
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'User-Agent': 'google-colab',
        'Accept': 'text/html, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
    }

    data = {
        'MIME Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'id2': city
    }

    url = "http://indicadoressifilis.aids.gov.br/tabelas.php"
    response = requests.post(url, headers=headers, data=data)

    html_data = response.text
    print(type(html_data))

    # Create a BeautifulSoup object from the HTML: soup
    soup = BeautifulSoup(html_data, features="html.parser")

    print('Searching for table 10.A')

    # Search for Tabela 10.A - Casos de sífilis congênita segundo diagnóstico
    # final por ano de diagnóstico. Brasil, 1998-2018.
    tables = soup.findAll("table")
    for table in tables:
        table_label = table.previous_element
        if "Tabela 10.A" in table_label:
            # Find all columns that describe the years
            tag = "headertab"
            columns_year = [c.text for c in table.find(id=tag).findAll('th')]
            columns_year.pop(0)

            df = pd.read_html(str(table))[0]
            df['ibge'] = city

            final_df = df.pivot(index='ibge', columns='Diagnóstico Final',
                                values=columns_year)

            return final_df

    return None


def recover_all():
    """ Recover all cities
    """

    codigos = pd.read_csv("data/codigo_municipio.csv", header=None)[0].values
    df = pd.DataFrame()

    for codigo in codigos:
        df = df.append(recover(codigo))

    df.to_csv('data/final.csv')
