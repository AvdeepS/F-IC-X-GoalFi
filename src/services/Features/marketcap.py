import yfinance as yf
import logging
from concurrent.futures import ThreadPoolExecutor
from net_position import net_df
import pandas as pd

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