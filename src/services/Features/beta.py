import yfinance as yf
import pandas as pd
import numpy as np

db = pd.read_csv('net_df_1.csv')
def calculate_beta(stock_symbol, index_symbol="^NSEI", period="2y"):
    stock_data = yf.download(stock_symbol, period=period)
    index_data = yf.download(index_symbol, period=period)
    
    stock_data['Returns'] = stock_data['Adj Close'].pct_change()
    index_data['Returns'] = index_data['Adj Close'].pct_change()
    
    stock_data = stock_data.dropna()
    index_data = index_data.dropna()
    
    returns_df = pd.DataFrame({
        "Stock Returns": stock_data['Returns'],
        "Index Returns": index_data['Returns']
    }).dropna()
    
    # Calculate beta
    covariance_matrix = np.cov(returns_df['Stock Returns'], returns_df['Index Returns'])
    covariance = covariance_matrix[0, 1]
    variance = covariance_matrix[1, 1]
    beta = covariance / variance
    
    return beta

def get_stock_symbol(ticker):
    stock_symbol = f"{ticker}.NS"
    data = yf.download(stock_symbol, period="1d")
    
    if data.empty:
        stock_symbol = f"{ticker}.BO"
        data = yf.download(stock_symbol, period="1d")
    
    return stock_symbol if not data.empty else None

db['Beta'] = None 

for ticker in db['Ticker_stock'].unique():
    stock_symbol = get_stock_symbol(ticker)
    if stock_symbol:
        beta = calculate_beta(stock_symbol)
        db.loc[db['Ticker_stock'] == ticker, 'Beta'] = beta
    else:
        print(f"No data available for {ticker} with .NS or .BO suffix")

print(db)