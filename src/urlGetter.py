from bs4 import BeautifulSoup
import pandas as pd
import requests
from io import StringIO
import re
import sys

def get_title(html):
    pattern = "<title.*?>.*?</title.*?>"
    match_results = re.search(pattern, html, re.IGNORECASE)
    if match_results != None:
        title = match_results.group()
        title = re.sub("<.*?>", "", title) # Remove HTML tags
        title = title.removesuffix(" Stock Price and Quote")
        ticker,sep,tail = title.partition(" -")
        return ticker
    return None

def makeSession():
    session = requests.session()
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
    }
    sesh = session.headers.update(headers)
    return sesh

def soupGetInfo(url):
    print("## GETTING INFO~~ MAKING CONNECTION ##")
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
    }
    page = requests.get(url,allow_redirects=True, headers=headers)
    
    if page.status_code == 429:
        sys.exit()
    soup = BeautifulSoup(page.text, 'html.parser')
    table = soup.find('table', 'js-snapshot-table snapshot-table2 screener_snapshot-table-body')  # Adjust based on the page's table id
    try:
        dfs = None
        # Attempt to read the table
        if table:  # Check if the table exists
            dfs = pd.read_html(StringIO(str(table)))[0]  # Get the first table
            if dfs[0][0] != "Index" :
                return None
        return dfs
    except ValueError as e:
        # Catch the error if no table could be read
        return None
    self.endheaders

def getFinvizURL(ticker):
    return "https://finviz.com/quote.ashx?t=" + ticker



