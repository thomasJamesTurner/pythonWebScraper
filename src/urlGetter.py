from bs4 import BeautifulSoup
import pandas as pd
import requests
from io import StringIO
import re

def get_title(html):
    pattern = "<title.*?>.*?</title.*?>"
    match_results = re.search(pattern, html, re.IGNORECASE)
    title = match_results.group()
    title = re.sub("<.*?>", "", title) # Remove HTML tags
    title = title.removesuffix(" Stock Price and Quote")
    return title

def soupGetInfo(url):
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
    }
    page = requests.get(url,allow_redirects=True, headers=headers)
    subjectName = get_title(page.text)
    soup = BeautifulSoup(page.text, 'html.parser')
    table = soup.find('table', 'js-snapshot-table snapshot-table2 screener_snapshot-table-body')  # Adjust based on the page's table id
    try:
        dfs = None
        # Attempt to read the table
        if table:  # Check if the table exists
            dfs = pd.read_html(StringIO(str(table)))[0]  # Get the first table
            if dfs[0][0] != "Index" :
                return None
        return subjectName ,dfs
    except ValueError as e:
        # Catch the error if no table could be read
        return None
    self.endheaders

def getFinvizURL(ticker):
    return "https://finviz.com/quote.ashx?t=" + ticker


