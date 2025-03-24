import pandas as pd

csv_file = 'C:/Users/jkarinef/Downloads/20250324125750.csv'
output_path = 'C:/Users/jkarinef/Beca25/extracted_files/total_vendas_comercio.csv'

df= pd.read_csv(csv_file)

# Extract header and data from the first and second rows
raw_header = df.iloc[0, 0].split(';')
raw_data = df.iloc[1, 0].split(';')

# Build a cleaned DataFrame
df_cleaned = pd.DataFrame([raw_data[1:]], columns=raw_header[1:])  # Skip the first "Brasil" entry

# Transpose the DataFrame
df_transposed = df_cleaned.T.reset_index()

# Rename columns
df_transposed.columns = ['data_mes', 'total_vendas_mes']

# Transform the column to string
df_transposed['total_vendas_mes'] = df_transposed['total_vendas_mes'].astype(str)

# Remove '.' from the string
df_transposed['total_vendas_mes'] = df_transposed['total_vendas_mes'].str.replace('.', '', regex=False)

# Convert it back to a float (or double)
df_transposed['total_vendas_mes'] = pd.to_numeric(df_transposed['total_vendas_mes'], errors='coerce')

#save to csv
df_transposed.to_csv(output_path, index = False)