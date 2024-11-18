from urllib.request import urlopen
import pandas as pd
from io import StringIO
import requests
import re

def get_title(html):
    pattern = "<title.*?>.*?</title.*?>"
    match_results = re.search(pattern, html, re.IGNORECASE)
    title = match_results.group()
    title = re.sub("<.*?>", "", title) # Remove HTML tags
    return title

def get_info(html):
    data = re.findall("<td[^>]*>.*?<b[^>]*>(.*?)</b>.*?</td>",html)
    text = ""
    for item in data:
        text += re.sub("<.*?>", "", item)
        text += "\n"
    return text

url = "https://finviz.com/quote.ashx?t=NEXT&ty=c&ta=1&p=d"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
page = requests.get(url,allow_redirects=True, headers=headers)
html_io = StringIO(page.text)
dfs = pd.read_html(html_io)




print(dfs[7])
#print(get_title(html))
#print(get_info(html))


