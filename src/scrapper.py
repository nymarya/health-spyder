import requests
from bs4 import BeautifulSoup
import pandas as pd

import multiprocessing as mp


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
    response = requests.get(url, headers=headers, data=data)

    html_data = response.text
    # print(html_data)

    # Create a BeautifulSoup object from the HTML: soup
    soup = BeautifulSoup(html_data, features="html.parser")

    tables = soup.findAll("table")
    for table in tables:
        table_label = table.previous_element
        if "Tabela 12.A" in table_label:
            # Find all columns that describe the years
            tag = "headertab"
            columns_year = [c.text for c in table.find(id=tag).findAll('th')]
            columns_year.pop(0)

            df = pd.read_html(str(table))[0]
            print(df)
            df['ibge'] = city

            final_df = df.pivot(index='ibge', columns='Diagnóstico Final',
                                values=columns_year)

            final_df.to_csv('results/{}.csv'.format(city), sep=';')

    return True


def recover_all():
    """ Recover all cities
    """

    codes = pd.read_csv("data/codigo_municipio.csv", header=None)[0].values
    df = pd.DataFrame()

    # Create chunks to parallelize search
    # chunks = create_chunks(codes, num_rows=1)
    # pool = mp.Pool(4)

    # results = [pool.apply_async(recover, args=(x,)) for x in chunks]
    # print([result.get() for result in results])
    for city in codes[:5]:
        recover(city)

    # df.to_csv('results/final.csv')


def create_chunks(dataframe, num_rows=1000):
 return [dataframe[i:i+num_rows] for i in range(0,len(dataframe),num_rows)]

# def execute():
#  cities = City.objects.all().values('ibge')
#  chunks = create_chunks(cities, num_rows=700)
#  pool = mp.Pool(len(chunks))

#  results = [pool.apply_async(recover, args=(x,)) for x in chunks]
#  print([result.get() for result in results])

