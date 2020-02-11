import requests
from bs4 import BeautifulSoup
import pandas as pd

import multiprocessing as mp
from os import listdir
from os.path import isfile, join

def recover(code, filename=None):
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
        'id2': code
    }

    url = "http://indicadoressifilis.aids.gov.br/tabelas.php"
    response = requests.post(url, headers=headers, data=data)

    html_data = response.text
    # print(html_data)

    # Create a BeautifulSoup object from the HTML: soup
    soup = BeautifulSoup(html_data, features="html.parser")

    tables = soup.findAll("table")
    final_df = pd.DataFrame()
    for table in tables:
        table_label = table.previous_element

        # Find all columns that describe the years
        tag = "headertab"
        columns_year = [c.text for c in table.find(id=tag).findAll('th')]
        columns_year.pop(0)

        # Save table to dataframe
        df = pd.read_html(str(table), decimal=',', thousands='.',
                          na_values=['-'], keep_default_na=False)[0]

        # Get columns referring to years
        cols = df.columns[df.columns.str.startswith('20')]
        cols2 = df.columns[df.columns.str.startswith('19')]
        columns = list(set(cols).union(set(cols2)))
        
        # Melt dataframe to group all years to one column 
        melted = pd.melt(df, id_vars=[df.columns[0]], value_vars=columns,
                         var_name='Ano', value_name='Valor')
        melted['Tabela'] = ''.join(table_label.split('.')[0:2])
        melted.rename(columns={melted.columns[0]: "Indicador"}, inplace=True)

        # Append table data
        final_df = final_df.append(melted, ignore_index=True)

    if filename is None:
        filename = code

    print("Saving {}.csv".format(filename))

    # Recover numeric columns
    cols = final_df.columns[final_df.columns.str.startswith('20')]
    cols2 = final_df.columns[final_df.columns.str.startswith('19')]

    # Tranform string to numeric
    final_df[cols] = final_df[cols].apply(pd.to_numeric)
    final_df[cols2] = final_df[cols2].apply(pd.to_numeric)

    # Save CSV
    final_df.to_csv('results/{}.csv'.format(filename), sep=';',
                    decimal=',')

    return True


def recover_cities():
    """ Recover all cities
    """

    codes = pd.read_csv("data/codigo_municipio.csv", header=None)[0].values
    df = pd.DataFrame()

    # Parallelize downloads
    pool = mp.Pool(4)

    results = pool.map(recover, codes)


def recover_regions():
    """ Recover data from regions, states and country.
    """
    pairs = pd.read_csv("data/codigos.csv", header=None, index_col=None).values

    codes = [(pair[0], pair[1]) for pair in pairs]
    pool = mp.Pool(4)

    results = pool.starmap(recover, codes)


def recover_all():
    recover_cities()
    recover_regions()
