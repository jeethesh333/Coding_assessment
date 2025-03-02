import gzip
import json
import pandas as pd
import datetime

def load_gzipped_json(file_path):
    """
    Load a gzipped JSON file and return a DataFrame.
    
    First, attempt to load the file using pd.read_json assuming JSON Lines format.
    If that fails, fall back to manually parsing each line with json.loads.
    If the result is a Series (because the JSON was a single object), convert it to a DataFrame.
    """
    try:
        df = pd.read_json(file_path, compression='gzip', lines=True)
        # If df is a Series, convert it to a DataFrame with one row.
        if isinstance(df, pd.Series):
            df = pd.DataFrame([df])
        return df
    except ValueError as e:
        print(f"pd.read_json failed for {file_path}: {e}")
        # Fallback: manually parse each line
        data = []
        base_name = file_path.split("/")[-1].replace(".gz", "")
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                if line.strip() and not line.startswith(base_name):
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        df = pd.DataFrame(data)
        if isinstance(df, pd.Series):
            df = pd.DataFrame([df])
        return df

# Conversion functions
def convert_oid(value):
    """
    Converts a MongoDB ObjectId represented as a dict (e.g., {'$oid': '...'})
    into its string representation.
    """
    if isinstance(value, dict) and '$oid' in value:
        return value['$oid']
    return value

def convert_date(value):
    """
    Converts a MongoDB date representation (e.g., {'$date': 1609687531000})
    into a Python datetime object.
    """
    if isinstance(value, dict) and '$date' in value:
        return datetime.datetime.fromtimestamp(value['$date'] / 1000)
    return value

def apply_conversions(df):
    """
    Apply conversions to a DataFrame by converting MongoDB ObjectIDs and date fields.
    Checks for known columns (like '_id', 'userId', 'brand_id' for ObjectIDs and a set of date columns)
    and applies the corresponding conversion functions.
    """
    # Convert ObjectID columns if they exist.
    oid_columns = ['_id', 'userId', 'brand_id']
    for col in oid_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_oid)
    
    # List of common date column names.
    date_columns = ['createdDate', 'lastLogin', 'purchaseDate', 'dateScanned', 'finishedDate', 'modifyDate', 'pointsAwardedDate']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_date)
    
    return df

# Define file paths for each dataset
file_paths = {
    'receipts': 'data/input_data/receipts.json.gz',
    'users': 'data/input_data/users.json.gz',
    'brands': 'data/input_data/brands.json.gz'
}

# Load the three main DataFrames using our loader function.
receipts_df = load_gzipped_json(file_paths['receipts'])
users_df = load_gzipped_json(file_paths['users'])
brands_df = load_gzipped_json(file_paths['brands'])

# If receipts have a nested rewardsReceiptItemList column, extract it.
if 'rewardsReceiptItemList' in receipts_df.columns:
    rewardsReceiptItemList_df = receipts_df['rewardsReceiptItemList']
    receipts_df = receipts_df.drop('rewardsReceiptItemList', axis=1)
else:
    rewardsReceiptItemList_df = pd.DataFrame()

# Organize all DataFrames in a dictionary.
dataframes = {
    'receipts': receipts_df,
    'users': users_df,
    'brands': brands_df,
    'rewardsReceiptItemList': rewardsReceiptItemList_df
}

# Apply conversions to each DataFrame.
for name, df in dataframes.items():
    dataframes[name] = apply_conversions(df)

# Print the first 10 rows and shape of each DataFrame for verification.
for name, df in dataframes.items():
    print(f"First 10 rows of {name}:")
    print(df.head(10))
    print("Shape of the DataFrame:", df.shape, "\n")
