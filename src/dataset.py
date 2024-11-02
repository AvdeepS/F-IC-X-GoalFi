import json
import csv

input_file_path = r'GoalFi.Mutual_Funds.json'
output_file_path = r'GoalFi_MF.csv'

def check_nan(value):
    if isinstance(value, dict):
        if '$numberDouble' in value:
            return None if value['$numberDouble'] == 'NaN' else value['$numberDouble']
        if '$numberLong' in value:
            return value['$numberLong']  # Handle $numberLong case
    return value

def flatten_holdings(fund):
    fund_data = []
    holdings = fund.get('holdings', [])
    
    if not isinstance(holdings, list):
        return fund_data
    
    for holding in holdings:
        holding_data = holding.get('data', {})
        year_month = holding.get('year_month', '').replace('_', '-')

        if not isinstance(holding_data, dict):
            continue

        for holding_name, holding_details in holding_data.items():
            if not isinstance(holding_details, dict):
                continue

            record_template = {
                '_id': fund.get('_id', {}).get('$oid', '') if isinstance(fund.get('_id', {}), dict) else '',
                'AMC': fund.get('fund_name', '') if isinstance(fund.get('fund_name', ''), str) else '',
                'Fund': holding_name,
                'Date': year_month,
                'ISIN': None,
                'Quantity': None,
                'Investment_Value': None,
                'Name': None,
                'Ticker': None,
                'Sector': None,
                'Industry': None
            }

            for category in [
                ('equity_listed', 'Equity Listed'),
                ('equity_unlisted', 'Equity Unlisted'),
                ('equity_foreign', 'Equity Foreign'),
                ('reits', 'REITs'),
                ('dericatives_futures', 'Futures'),
                ('dericatives_options', 'Options'),
                ('currency_futures', 'Currency Futures'),
                ('debt_listed', 'Listed Debt'),
                ('debt_unlisted', 'Unlisted Debt'),
                ('commercial_paper', 'Commercial Paper'),
                ('certificate_deposit', 'CDs'),
                ('t_bills', 'T Bills'),
                ('mutual_fund_unit', 'MF Units'),
                ('corp_debt_market_devp_fund', 'Corp Debt Market Devp Fund'),
                ('margin', 'Margin'),
                ('repo', 'Repo'),
                ('current_assets', 'Current Assets')
            ]:
                if holding_details.get(category[0]):
                    for item in holding_details[category[0]]:
                        if isinstance(item, dict):
                            record = record_template.copy()
                            record['ISIN'] = check_nan(item.get('ISIN', ''))
                            record['Quantity'] = check_nan(item.get('QUANTITY', ''))
                            record['Investment_Value'] = check_nan(item.get('MARKET CAP', ''))  # Updated to handle $numberLong
                            record['Name'] = check_nan(item.get('NAME', ''))
                            record['Ticker'] = check_nan(item.get('TICKER', ''))
                            record['Sector'] = check_nan(item.get('SECTOR', ''))
                            record['Industry'] = check_nan(item.get('INDUSTRY', ''))
                            record['holding_type'] = category[1]
                            fund_data.append(record)

    return fund_data

try:
    with open(input_file_path, 'r') as json_file:
        data = json.load(json_file)

    if not isinstance(data, list):
        raise ValueError("JSON data is not a list.")

    flattened_data = []
    for fund in data:
        flattened_data.extend(flatten_holdings(fund))

    if flattened_data:
        with open(output_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
            fieldnames = ['_id', 'AMC', 'Fund', 'Date', 'ISIN', 'Quantity', 'Investment_Value', 'Name', 'Ticker', 'Sector', 'Industry', 'holding_type']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for record in flattened_data:
                writer.writerow(record)

        print(f"Data successfully written to {output_file_path} in table format.")
    else:
        print("No data to write to CSV.")

except FileNotFoundError:
    print(f"Error: File not found at {input_file_path}")
except json.JSONDecodeError:
    print("Error: JSON file is not properly formatted")
except ValueError as ve:
    print(f"Error: {ve}")
except Exception as e:
    print(f"Error writing to CSV: {e}")