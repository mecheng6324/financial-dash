"""Seed ticker files for all supported markets"""
import requests
import pandas as pd
from io import StringIO

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def fetch_sp500():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    r = requests.get(url, headers=headers)
    tables = pd.read_html(StringIO(r.text))
    return tables[0]['Symbol'].tolist()

def fetch_dow():
    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
    r = requests.get(url, headers=headers)
    tables = pd.read_html(StringIO(r.text), match="Symbol")
    return tables[0]['Symbol'].tolist()

def fetch_nasdaq100():
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    r = requests.get(url, headers=headers)
    tables = pd.read_html(StringIO(r.text))
    # Table 5 has Ticker/Company columns with 101 rows
    return tables[5]['Ticker'].tolist()

def fetch_ftse100():
    url = "https://en.wikipedia.org/wiki/FTSE_100_Index"
    r = requests.get(url, headers=headers)
    tables = pd.read_html(StringIO(r.text))
    # Table 6 has Company/Ticker columns with 100 rows
    return tables[6]['Ticker'].tolist()

def save(filename, tickers):
    # Clean ticker symbols
    tickers = [str(t).strip() for t in tickers if str(t).strip()]
    with open(filename, 'w') as f:
        f.write('\n'.join(tickers))
    print(f"  -> Saved {len(tickers)} tickers to {filename}")

if __name__ == "__main__":
    markets = [
        ("S&P 500", fetch_sp500, "sp500.txt"),
        ("Dow Jones", fetch_dow, "dow.txt"),
        ("Nasdaq-100", fetch_nasdaq100, "nasdaq.txt"),
        ("FTSE 100", fetch_ftse100, "ftse100.txt"),
    ]
    
    for name, fetcher, filename in markets:
        print(f"Fetching {name}...")
        try:
            tickers = fetcher()
            save(filename, tickers)
        except Exception as e:
            print(f"  Failed: {e}")
    
    print("Done!")
