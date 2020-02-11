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
        df = pd.read_html(str(table))[0]
        df['Table'] = ''.join(table_label.split('.')[0:2])
        df.rename(columns={df.columns[0]: "Value"}, inplace=True)
        final_df = final_df.append(df, ignore_index=True)

    if filename is None:
        filename = code
    print("Saving {}.csv".format(filename))
    final_df.to_csv('results/{}.csv'.format(filename), sep=';')

    return True


def recover_cities():
    """ Recover all cities
    """

    codes = pd.read_csv("data/codigo_municipio.csv", header=None)[0].values
    df = pd.DataFrame()

    # Get codes that were not downloaded yet
    files = [int(f.split('.')[0]) for f in listdir('results/')
             if isfile(join('results/', f))]
    codes = list(set(codes) - set(files))

    # Create chunks to parallelize search
    # chunks = create_chunks(codes, num_rows=1)
    pool = mp.Pool(4)

    results = pool.map(recover, codes)


def recover_regions():
    pairs = pd.read_csv("data/codigos.csv", header=None, index_col=None).values

    codes = [(pair[0], pair[1]) for pair in pairs]

    pool = mp.Pool(4)

    results = pool.starmap(recover, codes)


def recover_all():
    recover_regions()
