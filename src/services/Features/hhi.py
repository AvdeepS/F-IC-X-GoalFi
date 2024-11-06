import pandas as pd

df = pd.read_csv('GoalFi_MF.csv', dtype={'Column5': str, 'Column6': str}, low_memory=False)

df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
df['Investment_Value'] = pd.to_numeric(df['Investment_Value'], errors='coerce')

df_cleaned = df.dropna(subset=['Investment_Value', 'AMC', 'Fund', 'Date']).copy()

df_cleaned['Date'] = pd.to_datetime(df_cleaned['Date'], errors='coerce')

df_cleaned = df_cleaned.dropna(subset=['Date'])

df_cleaned['Month_Year'] = df_cleaned['Date'].dt.to_period('M')

monthly_totals = df_cleaned.groupby(['Fund', 'Month_Year'])['Investment_Value'].sum().reset_index()
monthly_totals.rename(columns={'Investment_Value': 'Total_Investment_Value'}, inplace=True)

df_merged = pd.merge(df_cleaned, monthly_totals, on=['Fund', 'Month_Year'])
df_merged['Market_Share'] = df_merged['Investment_Value'] / df_merged['Total_Investment_Value']

df_merged['Market_Share_Squared'] = df_merged['Market_Share'] ** 2
hhi = df_merged.groupby(['Fund', 'Month_Year'])['Market_Share_Squared'].sum().reset_index()
hhi.rename(columns={'Market_Share_Squared': 'HHI'}, inplace=True)

output_name = 'hhi1.csv'
hhi.to_csv(output_name, index=False)
