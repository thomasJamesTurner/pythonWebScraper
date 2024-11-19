import pandas as pd
from io import StringIO
import requests
import hashlib
import mysql.connector
import os

def getInfo(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    page = requests.get(url,allow_redirects=True, headers=headers)
    html_io = StringIO(page.text)
    dfs = pd.read_html(html_io)
    if "https://finviz.com/quote" in url:
        return dfs[7]

def DBconnect(filename):
    dirname = os.path.dirname(__file__)
    os.chdir('../_sensitive_/')
    f = open(filename, "r")
    details = (f.read()).split()
    
    os.chdir(dirname)
    mydb = mysql.connector.connect(
        host="localhost",
        user= details[1],
        password=details[3],
        database="stock_information"
    )
    return mydb

def insertComapnyData(db,subjectName):
    hashObj = hashlib.sha256()
    hashObj.update(subjectName.encode('utf-8'))
    hexHash= hashObj.hexdigest()
    id = int(hexHash,16) % 100000
    cursor = db.cursor()
    sql = "INSERT INTO company (companyID, companyName, `index`, income, marketcap) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(sql,(id , subjectName,"NASDAQ",1.50,900000000000))
    db.commit()
    print(cursor.rowcount, "record inserted.")

url = "https://finviz.com/quote.ashx?t=NEXT&ty=c&ta=1&p=d"


db = DBconnect("pass.txt")
insertComapnyData(db,"testingInc")
#print(dfs[7])


