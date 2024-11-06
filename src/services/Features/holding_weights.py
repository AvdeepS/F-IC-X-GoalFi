import pandas as pd

df = pd.read_csv('GoalFi_MF.csv')


df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
df['Investment_Value'] = pd.to_numeric(df['Investment_Value'], errors='coerce')

df_cleaned = df.dropna(subset=['Investment_Value', 'AMC', 'Fund', 'Date'])

total_investment_value = df_cleaned.groupby(['AMC', 'Fund', 'Date'])['Investment_Value'].sum().reset_index()
total_investment_value.rename(columns={'Investment_Value': 'Total_Investment_Value'}, inplace=True)

df_cleaned = df_cleaned.merge(total_investment_value, on=['AMC', 'Fund', 'Date'], how='left')

df_cleaned['Holding_Weight'] = (df_cleaned['Investment_Value'] / df_cleaned['Total_Investment_Value']) * 100

df_cleaned['Holding_Weight_Squared'] = df_cleaned['Holding_Weight']**2

hhi_index = df_cleaned.groupby(['AMC', 'Fund', 'Date'])['Holding_Weight_Squared'].sum().reset_index()
hhi_index.rename(columns={'Holding Weight Squared' : 'HHI_Index'}, inplace=True)
df_cleaned = df_cleaned.merge(hhi_index, on=['AMC', 'Fund', 'Date'], how='left')
df_final = df_cleaned[['AMC', 'Fund', 'Date', 'Name','Investment_Value', 'Total_Investment_Value', 'Holding_Weight', 'HHI_Index']]
print(df_final.head())
output_name = 'HHI.csv' 
df_final.to_csv(output_name, index=False)
