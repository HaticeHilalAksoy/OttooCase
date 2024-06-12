import pandas as pd
import dask.dataframe as dd
import re
import orjson

# Stage1: Reading Files
file1 = './BasePriceFile_20240607.csv'
file2 = './BasePriceFile_20240608.csv'

# Defining column data types
dtype_spec = {
    'punctuated_part_number': str,
    'current_suggested_retail': float,
    'wi_availability': int,
    'ny_availability': int,
    'tx_availability': int,
    'nv_availability': int,
    'nc_availability': int,
    'national_availability': int,
    'part_status': str,
    'part_description': str,
    'brand_name': str,
    # 'upc_code': str
}

df1 = pd.read_csv(file1, dtype=dtype_spec, low_memory=False)
df2 = pd.read_csv(file2, dtype=dtype_spec, low_memory=False)

# Stage2: Clearing Column Names
def clean_column_names(df):
    df.columns = [re.sub(r'\W+', '_', col.lower().strip()) for col in df.columns]
    return df

df1 = clean_column_names(df1)
df2 = clean_column_names(df2)

# Stage3: Clearing Data 
def clean_data(df, id_column):
    df = df.dropna(subset=[id_column])  
    df = df.drop_duplicates(subset=[id_column])  
    return df

id_column = 'punctuated_part_number'
df1 = clean_data(df1, id_column)
df2 = clean_data(df2, id_column)

# Stage4:Clearing  Availability Data
def enrichment_method(df, quantity_columns):
    for col in quantity_columns:
        df[col] = df[col].astype(str).str.replace('+', '', regex=False)
    return df

quantity_columns = [
    'wi_availability', 'ny_availability', 'tx_availability', 
    'nv_availability', 'nc_availability', 'national_availability'
]
df1 = enrichment_method(df1, quantity_columns)
df2 = enrichment_method(df2, quantity_columns)

# Stage 5: Saving Data in Parquet Format
df1.to_parquet('cleaned_base_price_file_20240607.parquet', engine='pyarrow')
df2.to_parquet('cleaned_base_price_file_20240608.parquet', engine='pyarrow')

# Stage6: Reading Parquet Files Using Dask
df1_parquet = dd.read_parquet('cleaned_base_price_file_20240607.parquet').set_index('punctuated_part_number')
df2_parquet = dd.read_parquet('cleaned_base_price_file_20240608.parquet').set_index('punctuated_part_number')

# Combining DataFrames
merged_df = dd.merge(df1_parquet, df2_parquet, left_index=True, right_index=True, suffixes=('_file1', '_file2'), how='outer', indicator=True)

# Defining the Conditions for Finding Differences
conditions = [
    merged_df['part_status_file1'] != merged_df['part_status_file2'],
    merged_df['current_suggested_retail_file1'] != merged_df['current_suggested_retail_file2']
]

for col in quantity_columns:
    conditions.append(merged_df[f'{col}_file1'] != merged_df[f'{col}_file2'])

changed = merged_df.loc[conditions[0] | conditions[1] | dd.concat(conditions[2:], axis=1).any(axis=1)]

# Adding the Differences
differences = {}
for index, row in changed.compute().iterrows():
    part_number = index
    difference = {
        'punctuated_part_number': part_number,
        'part_description': {
            'file1': row['part_description_file1'],
            'file2': row['part_description_file2']
        },
        'brand_name': {
            'file1': row['brand_name_file1'],
            'file2': row['brand_name_file2']
        },
        '_merge': row['_merge']
    }
    change_detected = False

    if row['part_status_file1'] != row['part_status_file2']:
        if pd.notnull(row['part_status_file1']) or pd.notnull(row['part_status_file2']):
            difference['part_status'] = {
                'file1': row['part_status_file1'],
                'file2': row['part_status_file2']
            }
            change_detected = True

    if row['current_suggested_retail_file1'] != row['current_suggested_retail_file2']:
        if pd.notnull(row['current_suggested_retail_file1']) or pd.notnull(row['current_suggested_retail_file2']):
            difference['current_suggested_retail'] = {
                'file1': row['current_suggested_retail_file1'],
                'file2': row['current_suggested_retail_file2']
            }
            change_detected = True

    for col in quantity_columns:
        if row[f'{col}_file1'] != row[f'{col}_file2']:
            if pd.notnull(row[f'{col}_file1']) or pd.notnull(row[f'{col}_file2']):
                difference[col] = {
                    'file1': row[f'{col}_file1'],
                    'file2': row[f'{col}_file2']
                }
                change_detected = True
    
    if change_detected:
        differences[part_number] = difference

# Writing to JSON File
with open('differences.json', 'wb') as f:
    f.write(orjson.dumps(differences, option=orjson.OPT_INDENT_2))

# Adding Differences for Second File Only
differences_file2_only = {}
for index, row in changed.compute().iterrows():
    part_number = index
    difference = {
        'punctuated_part_number': part_number,
        'part_description': row['part_description_file2'],
        'brand_name': row['brand_name_file2'],
        '_merge': row['_merge']
    }
    change_detected = False

    if row['part_status_file1'] != row['part_status_file2']:
        if pd.notnull(row['part_status_file1']) or pd.notnull(row['part_status_file2']):
            difference['part_status'] = row['part_status_file2']
            change_detected = True

    if row['current_suggested_retail_file1'] != row['current_suggested_retail_file2']:
        if pd.notnull(row['current_suggested_retail_file1']) or pd.notnull(row['current_suggested_retail_file2']):
            difference['current_suggested_retail'] = row['current_suggested_retail_file2']
            change_detected = True

    for col in quantity_columns:
        if row[f'{col}_file1'] != row[f'{col}_file2']:
            if pd.notnull(row[f'{col}_file1']) or pd.notnull(row[f'{col}_file2']):
                difference[col] = row[f'{col}_file2']
                change_detected = True
    
    if change_detected:
        differences_file2_only[part_number] = difference

# Writing to JSON File
with open('differences_file2_only.json', 'wb') as f:
    f.write(orjson.dumps(differences_file2_only, option=orjson.OPT_INDENT_2))

# Counting the Differences
part_status_diff = 0
current_suggested_retail_diff = 0
quantity_diffs = {
    'wi_availability': 0,
    'ny_availability': 0,
    'tx_availability': 0,
    'nv_availability': 0,
    'nc_availability': 0,
    'national_availability': 0
}

for diff in differences.values():
    if 'part_status' in diff:
        part_status_diff += 1
    if 'current_suggested_retail' in diff:
        current_suggested_retail_diff += 1
    for col in quantity_diffs.keys():
        if col in diff:
            quantity_diffs[col] += 1

# Total number of changes
num_changes = part_status_diff + current_suggested_retail_diff + sum(quantity_diffs.values())
print(f"Total number of changes: {num_changes}")

print(f"part_status differences: {part_status_diff}")
print(f"current_suggested_retail differences: {current_suggested_retail_diff}")
for col, diff_count in quantity_diffs.items():
    print(f"{col} differences {diff_count}")