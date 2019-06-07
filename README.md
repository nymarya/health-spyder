## About

This is a web scrapper that aims to recover data about [syphilis data](http://indicadoressifilis.aids.gov.br).

## Usage

First, you must install the dependencies:

```
pip install -r requirements.txt
```

Then, you can just run it:

```
python src/main.py
```

The `csv` file will be available at the `data` folder. Using `pandas`, you can read it as below:

```python
df = pd.read_csv('data/final.csv', header=[0,1])
```

