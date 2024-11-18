import pandas as pd
from io import StringIO
import requests


url = "https://finviz.com/quote.ashx?t=NEXT&ty=c&ta=1&p=d"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
page = requests.get(url,allow_redirects=True, headers=headers)
html_io = StringIO(page.text)
dfs = pd.read_html(html_io)
df = dfs[7]
df = df.transpose()



print(dfs[7])


