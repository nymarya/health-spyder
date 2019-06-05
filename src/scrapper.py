import requests
from bs4 import BeautifulSoup

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
    'id2': 240010
}

response = requests.post("http://indicadoressifilis.aids.gov.br/tabelas.php",
                       headers=headers, data={})

html_data = response.text
print(type(html_data))

# Create a BeautifulSoup object from the HTML: soup
soup = BeautifulSoup(html_data, features="html.parser")

# # Prettify the BeautifulSoup object: pretty_soup
# pretty_soup = soup.prettify()

# # Print the response
# print(pretty_soup)

print('Searching for table 10.A')
# Search for Tabela 10.A - Casos de sífilis congênita segundo diagnóstico final por ano de diagnóstico. Brasil, 1998-2018.
tables = soup.findAll("table")
for table in tables:
    table_label = table.previous_element
    if table_label.contains("Tabela 10.A"):
        print(table)
