import pandas as pd
from io import StringIO
import requests
import hashlib
import mysql.connector
import pymysql
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup

def get_title(html):
    pattern = "<title.*?>.*?</title.*?>"
    match_results = re.search(pattern, html, re.IGNORECASE)
    title = match_results.group()
    title = re.sub("<.*?>", "", title) # Remove HTML tags
    title = title.removesuffix(" Stock Price and Quote")
    return title

def getInfo(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    page = requests.get(url,allow_redirects=True, headers=headers)
    subjectName = get_title(page.text)
    html_io = StringIO(page.text)
    dfs = pd.read_html(html_io)
    if "https://finviz.com/quote" in url:
        return (subjectName,dfs[7])

def soupGetInfo(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    page = requests.get(url,allow_redirects=True, headers=headers)
    subjectName = get_title(page.text)
    soup = BeautifulSoup(page.text, 'html.parser')
    table = soup.find('table', 'js-snapshot-table snapshot-table2 screener_snapshot-table-body')  # Adjust based on the page's table id
    dfs = pd.read_html(StringIO(str(table)))[0]
    return (subjectName,dfs)

# Helper functions to parse data
def parse_currency(value):
    if value[-1] in ['B', 'M']:
        factor = 1e9 if value[-1] == 'B' else 1e6
        return float(value[:-1].replace(',', '').strip()) * factor
    else:
        return float(value.replace(',', '').strip())

def parse_float(value):
    if value == '-':
        return None
    return float(value.replace(',', '').strip()) if value else None

def parse_percentage(value):
    return parse_float(value.replace('%', ''))

def parse_date(value):
    try:
        return datetime.strptime(value, '%b %d, %Y')
    except ValueError:
        return None

def parseData(data):
    
    parsed_data = {}

    parsed_data['index'] = data[1][0]

    income_str = data[1][2]  
    parsed_data['income'] = parse_currency(income_str)
   
    market_cap_str = data[1][1]  
    parsed_data['marketcap'] = parse_currency(market_cap_str)
   
    sales_str = data[1][3]
    parsed_data['sales'] = parse_currency(sales_str)

    employees_str = data[1][9] 
    parsed_data['employees'] = int(employees_str.replace(',', ''))
 
    shares_outstanding_str = data[9][0] 
    parsed_data['shsOutstand'] = parse_currency(shares_outstanding_str)
 
    shares_float_str = data[9][1] 
    parsed_data['ShsFloat'] = parse_currency(shares_float_str)
 
    insider_own_str = data[7][0] 
    parsed_data['insiderOwn'] = parse_percentage(insider_own_str)

    insider_trans_str = data[7][1]  
    parsed_data['insiderTrans'] = parse_percentage(insider_trans_str)

    inst_own_str = data[7][2]  
    parsed_data['instOwn'] = parse_percentage(inst_own_str)

    short_float_str = data[9][3] 
    parsed_data['shortFloat'] = parse_percentage(short_float_str)

    pe_str = data[3][0]  
    parsed_data['P/E'] = parse_float(pe_str)

    peg_str = data[3][2] 
    parsed_data['PEG'] = parse_float(peg_str)

    perf_week_str = data[11][0]  
    parsed_data['perfWeek'] = parse_percentage(perf_week_str)
 
    perf_month_str = data[11][1]  
    parsed_data['perfMonth'] = parse_percentage(perf_month_str)

    perf_quarter_str = data[11][2] 
    parsed_data['perfQuarter'] = parse_percentage(perf_quarter_str)

    eps_str = data[5][0] 
    parsed_data['EPS'] = parse_float(eps_str)

    eps_next_qtr_str = data[5][2]  
    parsed_data['EPSNextQuarter'] = parse_float(eps_next_qtr_str)

    eps_next_year_str = data[5][4]  
    parsed_data['EPSNextYear'] = parse_percentage(eps_next_year_str)

    book_sh_str = data[1][4]  
    parsed_data['book/sh'] = parse_float(book_sh_str)

    cash_sh_str = data[1][5] 
    parsed_data['cash/sh'] = parse_float(cash_sh_str)

    dividend_est_str = data[1][6]
    if dividend_est_str == '-':
          parsed_data['dividendEst'] = None
    else:
        parsed_data['dividendEst'] = parse_float(re.search(r'\(([\d\.]+)%\)', dividend_est_str).group(1))

    dividend_ex_date_str = data[1][7]  
    parsed_data['dividendExDate'] = parse_date(dividend_ex_date_str)

    sma20_str = data[1][12]  
    parsed_data['SMA20'] = parse_percentage(sma20_str)

    debt_eq_str = data[3][9] 
    parsed_data['debt/eq'] = parse_float(debt_eq_str)

    lt_debt_eq_str = data[3][10]  
    parsed_data['ltDebt/eq'] = parse_float(lt_debt_eq_str)

    roa_str = data[7][4]  
    parsed_data['ROA'] = parse_percentage(roa_str)

    roe_str = data[7][5] 
    parsed_data['ROE'] = parse_percentage(roe_str)

    roi_str = data[7][6]  
    parsed_data['ROI'] = parse_percentage(roi_str)

    gross_margin_str = data[7][7] 
    parsed_data['grossMargin'] = parse_percentage(gross_margin_str)

    oper_margin_str = data[7][8]  
    parsed_data['operMargin'] = parse_percentage(oper_margin_str)
   
    profit_margin_str = data[7][9] 
    parsed_data['profitMargin'] = parse_percentage(profit_margin_str)

    return parsed_data


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


def insertComapnyData(db,subjectName,data):
    data = list(data.values())
    data.insert(0,subjectName)
    print(subjectName + '\n')

    hashObj = hashlib.sha256()
    hashObj.update(subjectName.encode('utf-8'))
    hexHash= hashObj.hexdigest()
    id = int(hexHash,16) % 100000
    data.insert(0,id)

    cursor = db.cursor()
    sql = (open("company_insert.txt", "r")).read()
    print("SQL Query: ", sql)

    final_data = data
    print("\n\n DATA: ", final_data)
    cursor.execute(sql, final_data)
    db.commit()
    print(cursor.rowcount, "record inserted.")

urleon = "https://finviz.com/quote.ashx?t=NEXT"
urlaligent = "https://finviz.com/quote.ashx?t=A"

db = DBconnect("pass.txt")
companyData = soupGetInfo("https://finviz.com/quote.ashx?t=FBRX&p=d")
print(companyData)
print(parseData(companyData[1]))
insertComapnyData(db,companyData[0],parseData(companyData[1]))
#print(dfs[7])


