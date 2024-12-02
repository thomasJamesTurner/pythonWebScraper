import hashlib
import yfinance as yf
import urlGetter
import mysql.connector
from mysql.connector import IntegrityError
import os
import re
import time
import random
import json
import datetime
from datetime import datetime

# Helper functions to parse data


def getLineIndex(filename, searchTerm, separator):
    if(searchTerm == None):
        return 0
    searchTerm += separator
    with open(filename, 'r') as file:
        for line_num, line in enumerate(file, start=1):
            index = line.find(searchTerm)
            if index != -1:
                print(f"Found {searchTerm} on line {line_num}, index {index}")
                return line_num -1
            
    print("couldnt find search_term in file")
    return 0

def getLastLine(filename):
    with open(filename, 'r') as file:
        # Read all lines and get the last one
        lines = file.readlines()
        last_line = lines[-1] if lines else None  # Ensure file isn't empty
        print(last_line)
        return last_line

def getID(value, mod):
    hashObj = hashlib.sha256()
    hashObj.update(value.encode('utf-8'))
    hexHash= hashObj.hexdigest()
    ID = int(hexHash,16) % mod
    return ID

def parse_currency(value):
    # If the value is '-', return None
    if value == '-':
        return None
    if type(value) == float:
        print(value)
    # Remove commas and strip spaces for consistency
    value = value.replace(',', '').strip()

    # Check if the last character is 'B' or 'M' to handle billions and millions
    if value[-1] in ['B', 'M']:
        factor = 1e9 if value[-1] == 'B' else 1e6
        return float(value[:-1]) * factor

    # If no 'B' or 'M', return the numeric value
    return float(value)

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
    
    
    if(data[1][0] == '-' or data[1][0] == None):
        parsed_data['index'] = None
        return None
    else:
        parsed_data['index'] = data[1][0]

    income_str = data[1][2]  
    parsed_data['income'] = parse_currency(income_str)
   
    market_cap_str = data[1][1]  
    parsed_data['marketcap'] = parse_currency(market_cap_str)
   
    sales_str = data[1][3]
    parsed_data['sales'] = parse_currency(sales_str)

    employees_str = data[1][9] 
    if(employees_str == '-' or employees_str == None):
        parsed_data['employees'] = None
    else:
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


def insertStock(db, ticker, exchange, industry):
    data = [None] *4
    data[0] = ticker

    if exchange != None:
        exchangeID = getID(exchange,1000)
        data[1] = exchangeID

    companyID = getID(ticker,10000000)
    data[2] = companyID

    if industry != None:
        data[3] = industry

    
    print(data)
    cursor = db.cursor()
    sql = (open("sql/stock_insert.txt", "r")).read()
    try:
        cursor.execute(sql, data)
        db.commit()
        print(cursor.rowcount, "record inserted.")
    except IntegrityError as e:
        print("cannot record data error s%", e)
        return None
    
def insertAllStock(filename, separator = "", max_retries=5):
    sesh = urlGetter.makeSession()
    db = DBconnect("pass.txt")
    f = open(filename,"r")
    last_ticker = getLastLine("log.txt")
    i = getLineIndex(filename,last_ticker , separator)
    eof = False
    bigFile = f.readlines()
    
    while not eof:
        retries = 0
        try:
            if separator != "":
                line, sep , tail = (bigFile[i].strip()).partition(separator)
            else:
                line = bigFile[i].strip()
        except Exception as e:
            print(f"Error processing line {i}: {str(e)}")
            break  # Exit the loop if the line processing fails
        print(line)
        
        if line != "":   
            # Log the ticker being processed
            with open("log.txt", "w") as log:
                log.write(line)
            
            # Retry logic for handling failed data fetching
            while retries < max_retries:
                try:
                    dat = yf.Ticker(line, session=sesh)
                    if dat.info and 'industry' in dat.info:
                        ind = str(dat.info.get('industry'))
                        print(f"Industry for {line}: {ind}")
                        print(line)
                        insertStock(db, line, dat.info.get('exchange'), ind)
                    break  # Break the retry loop if the request is successful
                except Exception as e:
                    print(f"Exception type: {type(e).__name__} for ticker {line}")
                    if isinstance(e, json.JSONDecodeError):
                        print(f"Error decoding JSON for {line}: {e}")
                    else:
                        print(f"Unexpected error for {line}: {e}")
                    
                    retries += 1
                    if retries < max_retries:
                        # Exponential backoff: wait for 2^retries seconds before retrying
                        sleep_time = random.randrange(2 ** retries, 2 ** (retries + 1))  # Exponential backoff
                        print(f"Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                    else:
                        print(f"Max retries reached for {line}. Skipping.")
                        break  # Skip to the next ticker after max retries
                
        else:
            eof = True
        i += 1

    print("####### COMPLETE #######")



def updateAllCompanies():
    db = DBconnect("pass.txt")
    cursor = db.cursor()
    sql = (open("update_company.txt", "r")).read()
    cursor.execute(sql)
    db.commit()



def insertComapnyData(db,subjectName,data):
    data = list(data.values())
    data.insert(0,subjectName)
    print(subjectName + '\n')

    id = getID(subjectName, 10000000)
    data.insert(0,id)

    cursor = db.cursor()
    sql = (open("sql/company_insert.txt", "r")).read()
    

    final_data = data
    print("\n\n DATA: ", final_data)
    try:
        cursor.execute(sql, final_data)
        db.commit()
        print(cursor.rowcount, "record inserted.")
    except IntegrityError as e:
        print("cannot record data error %s",e)
        return None

def insertAllCompanies(filename, separator = "", max_retries=5):
    sesh = urlGetter.makeSession()
    db = DBconnect("pass.txt")
    f = open(filename,"r")
    last_ticker = getLastLine("log.txt")
    if(last_ticker != None):
        i = getLineIndex(filename,last_ticker , separator)
    else:
        print("NO TICKER FOUND IN LOG")
        i = 0
    eof = False
    bigFile = f.readlines()
    
    while not eof:
        retries = 0
        try:
            if separator != "":
                line, sep , tail = (bigFile[i].strip()).partition(separator)
            else:
                line = bigFile[i].strip()
        except Exception as e:
            print(f"Error processing line {i}: {str(e)}")
            break  # Exit the loop if the line processing fails
        print(line)
        
        if line != "":   
            # Log the ticker being processed
            with open("log.txt", "w") as log:
                log.write(line)
            
            # Retry logic for handling failed data fetching
            while retries < max_retries:
                try:
                    dat = yf.Ticker(line, session=sesh)
                    if dat.info and 'industry' in dat.info:
                        ind = str(dat.info.get('industry'))
                        print(f"Industry for {line}: {ind}")
                        print(line)
                        data = parseData(urlGetter.soupGetInfo(urlGetter.getFinvizURL(line)))
                        insertComapnyData(db,line,data)
                    break  # Break the retry loop if the request is successful
                except Exception as e:
                    print(f"Exception type: {type(e).__name__} for ticker {line}")
                    if isinstance(e, json.JSONDecodeError):
                        print(f"Error decoding JSON for {line}: {e}")
                    else:
                        print(f"Unexpected error for {line}: {e}")
                    
                    retries += 1
                    if retries < max_retries:
                        # Exponential backoff: wait for 2^retries seconds before retrying
                        sleep_time = random.randrange(2 ** retries, 2 ** (retries + 1))  # Exponential backoff
                        print(f"Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                    else:
                        print(f"Max retries reached for {line}. Skipping.")
                        break  # Skip to the next ticker after max retries
                
        else:
            eof = True
        i += 1

    print("####### COMPLETE #######")



def insertStockData(db,data):
    print(data)
    cursor = db.cursor()
    sql = (open("sql/stockData_insert.txt", "r")).read()
    try:
        cursor.execute(sql, data)
        db.commit()
        print(cursor.rowcount, "record inserted.")
    except IntegrityError as e:
        print("cannot record data error s%", e)
        return None
    
def insertAllStockData(filename, separator = "", max_retries=5):
    sesh = urlGetter.makeSession()
    db = DBconnect("pass.txt")
    f = open(filename,"r")
    last_ticker = getLastLine("log.txt")
    i = getLineIndex(filename,last_ticker , separator)
    eof = False
    bigFile = f.readlines()
    
    while not eof:
        retries = 0
        try:
            if separator != "":
                line, sep , tail = (bigFile[i].strip()).partition(separator)
            else:
                line = bigFile[i].strip()
        except Exception as e:
            print(f"Error processing line {i}: {str(e)}")
            break  # Exit the loop if the line processing fails
        print(line)
        
        if line != "":   
            # Log the ticker being processed
            with open("log.txt", "w") as log:
                log.write(line)
            
            # Retry logic for handling failed data fetching
            while retries < max_retries:
                try:
                    dat = yf.download(tickers=line, session=sesh,start="2024-09-30", end="2024-11-27", interval="1d")
                    print(dat)
                    if not dat.empty:
                        
                        for iterable,row in dat.iterrows():
                            date = row.name.date()
                            print(date)
                            data = [getID(line,10000000),date, row.iloc[4],row.iloc[1] , row.iloc[2], row.iloc[3], row.iloc[5], row.iloc[0]]
                            print(row.name)
                            insertStockData(db,data)
                    else:
                        print("\n\nno data found for {line}")
                    break  # Break the retry loop if the request is successful
                except Exception as e:
                    print(f"Exception type: {type(e).__name__} for ticker {line}")
                    if isinstance(e, json.JSONDecodeError):
                        print(f"Error decoding JSON for {line}: {e}")
                    else:
                        print(f"Unexpected error for {line}: {e}")
                    
                    retries += 1
                    if retries < max_retries:
                        # Exponential backoff: wait for 2^retries seconds before retrying
                        sleep_time = random.randrange(2 ** retries, 2 ** (retries + 1))  # Exponential backoff
                        print(f"Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                    else:
                        print(f"Max retries reached for {line}. Skipping.")
                        break  # Skip to the next ticker after max retries
                
        else:
            eof = True
        i += 1

    print("####### COMPLETE #######")


def updateStockData(filename, separator = "", max_retries=5):
    db = DBconnect("pass.txt")
    cursor = db.cursor()
    query = "SELECT MAX(dateCollected) AS most_recent_date FROM stockData;"
    cursor.execute(query)
    startDate = str((cursor.fetchone())[0].date())
    
    print(startDate)

    endDate = str((datetime.today()).date())
    print(endDate)
    sesh = urlGetter.makeSession()
    f = open(filename,"r")
    last_ticker = getLastLine("log.txt")
    i = getLineIndex(filename,last_ticker , separator)
    eof = False
    bigFile = f.readlines()
    
    while not eof:
        retries = 0
        try:
            if separator != "":
                line, sep , tail = (bigFile[i].strip()).partition(separator)
            else:
                line = bigFile[i].strip()
        except Exception as e:
            print(f"Error processing line {i}: {str(e)}")
            break  # Exit the loop if the line processing fails
        
        if line != "":   
            # Log the ticker being processed
            log = open("log.txt", "w")
            log.write(line)
            
            # Retry logic for handling failed data fetching
            while retries < max_retries:
                try:
                    dat = yf.download(tickers=line, session=sesh,start=startDate, end=endDate, interval="1d")
                    print(dat)
                    if not dat.empty:
                        
                        for iterable,row in dat.iterrows():
                            date = row.name.date()
                            print("### ROW ###")
                            print(date)
                            data = [getID(line,10000000),date, row.iloc[4],row.iloc[1] , row.iloc[2], row.iloc[3], row.iloc[5], row.iloc[0]]
                            
                            
                            insertStockData(db,data)
                    else:
                        print("\n\nno data found for {line}")
                    break  # Break the retry loop if the request is successful
                except Exception as e:
                    print(f"Exception type: {type(e).__name__} for ticker {line}")
                    if isinstance(e, json.JSONDecodeError):
                        print(f"Error decoding JSON for {line}: {e}")
                    else:
                        print(f"Unexpected error for {line}: {e}")
                    
                    retries += 1
                    if retries < max_retries:
                        # Exponential backoff: wait for 2^retries seconds before retrying
                        sleep_time = random.randrange(2 ** retries, 2 ** (retries + 1))  # Exponential backoff
                        print(f"Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                    else:
                        print(f"Max retries reached for {line}. Skipping.")
                        break  # Skip to the next ticker after max retries
                
        else:
            eof = True
        i += 1
    log.truncate(0)
    log.close()
    print("####### COMPLETE #######")


