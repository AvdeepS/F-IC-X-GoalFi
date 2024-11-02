import yfinance as yf
from datetime import datetime
import numpy as np
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor 
import logging

df = pd.read_csv(r'C:\Users\avdee\OneDrive\Desktop\Repositories\F-IC-X-GoalFi\data\interim\GoalFi_MF.csv')

#initial data cleaning
drop_col_list = ['_id','ISIN']
df.drop(columns=drop_col_list, axis=1, inplace=True)
df_eq = df.loc[df['holding_type']=='Equity Listed']
df_eq.dropna(subset=['Sector', 'Ticker','Industry'], inplace=True)
df_eq.reset_index(drop=True, inplace=True)
amc_fund_dict = df_eq.groupby('AMC')['Fund'].unique().apply(list).to_dict()

#converting to numeric
df['Investment_Value']=pd.to_numeric(df['Investment_Value'])
df['Quantity']=pd.to_numeric(df['Quantity'])

df_fno = df.loc[df['holding_type'].isin(['Futures', 'Options'])]

#function to trim fno names in order to find net position
def trim_string(string):
    #remove any occurrences of months, years, and date formats (e.g., "June 2024", "26/10/2023")
    string = re.sub(r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\b \d{4}', '', string, flags=re.IGNORECASE)
    string = re.sub(r'\d{1,2}/\d{1,2}/\d{4}', '', string)
    
    string = re.sub(r'\bFuture\b', '', string, flags=re.IGNORECASE)
    
    # Retain everything up to "Limited" or "Ltd" and discards the rest
    cleaned_string = re.sub(r'\b(Limited|Ltd)\b.*', r'\1', string, flags=re.IGNORECASE)
    
    return cleaned_string.strip()

df_fno['Name'] = df_fno['Name'].apply(trim_string)

net_df = pd.merge(df_eq, df_fno, on=['AMC', 'Fund', 'Name', 'Date'], 
                     how='outer', suffixes=('_stock', '_fno'))

net_df.drop(columns=['Ticker_fno', 'Sector_fno', 'Industry_fno','holding_type_fno','holding_type_stock'], axis=1, inplace=True)

#fills the missing data for ticker,sector and industry
net_df['Ticker_stock'] = net_df.groupby('Name')['Ticker_stock'].transform(lambda x: x.ffill().bfill())
net_df['Sector_stock'] = net_df.groupby('Name')['Sector_stock'].transform(lambda x: x.ffill().bfill())
net_df['Industry_stock'] = net_df.groupby('Name')['Industry_stock'].transform(lambda x: x.ffill().bfill())

net_df.fillna(0, inplace=True)
net_df['Net_Position'] = net_df['Quantity_stock'] + net_df['Quantity_fno']
net_df['% Hedged'] = np.where(
    net_df['Quantity_stock'] == 0,
    np.nan,  # Set to NaN if Quantity_stock is 0
    (abs(net_df['Quantity_fno'] / net_df['Quantity_stock']) * 100).apply(lambda x: f"{x:.2f}%")
)



#market cap classifications
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

invalid_tickers = net_df['Ticker_stock'].isna() | (net_df['Ticker_stock'] == '')
if invalid_tickers.any():
    logging.warning(f"Invalid ticker entries found: {net_df[invalid_tickers]['Ticker_stock'].unique()}")

valid_tickers_df = net_df[~invalid_tickers]
unique_tickers = valid_tickers_df['Ticker_stock'].unique()

def get_company_market_cap(ticker):
    try:
        if ticker:
            stock = yf.Ticker(f"{ticker}.NS")
            market_cap = stock.info.get('marketCap')
            if market_cap is None:
                raise ValueError("Market cap not found in NSE")
            return market_cap
        else:
            print(f"Invalid ticker: {ticker}")
            return None
    except Exception as ns_exception:
        print(f"Error fetching market cap for {ticker}.NS: {ns_exception}")
        
        try:
            stock = yf.Ticker(f"{ticker}.BO")
            market_cap = stock.info.get('marketCap')
            if market_cap is None:
                raise ValueError("Market cap not found in BSE")
            return market_cap
        except Exception as bo_exception:
            print(f"Error fetching market cap for {ticker}.BO: {bo_exception}")
            return None


def classify_market_cap(market_cap):
    small_cap_threshold = 5000 * 1e7  # Small cap threshold
    mid_cap_threshold = 20000 * 1e7    # Mid cap threshold

    if market_cap is None:
        return "Unknown"
    elif market_cap < small_cap_threshold:
        return "Small Cap"
    elif small_cap_threshold <= market_cap < mid_cap_threshold:
        return "Mid Cap"
    else:
        return "Large Cap"

def fetch_and_classify_market_cap(ticker):
    market_cap = get_company_market_cap(ticker)
    classification = classify_market_cap(market_cap)
    return ticker, market_cap, classification

with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(fetch_and_classify_market_cap, unique_tickers))

results_df = pd.DataFrame(results, columns=['Ticker_stock', 'Market_Cap', 'Classification'])
net_df = net_df.merge(results_df, on='Ticker_stock', how='left')