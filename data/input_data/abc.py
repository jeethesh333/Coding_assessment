import gzip
import json
import pandas as pd
import datetime


def load_gzipped_json(file_path):
    """
    Load a gzipped JSON file and return a DataFrame.
    
    The function first attempts to load the file using pd.read_json assuming JSON Lines format.
    If that fails, it falls back to manually parsing each line with json.loads.
    It also skips any header lines that start with the file's base name.
    """
    try:
        # Attempt to read the file assuming JSON Lines format
        df = pd.read_json(file_path, compression='gzip', lines=True)
        return df
    except ValueError as e:
        print(f"pd.read_json failed for {file_path}: {e}")
        # Fallback: manually parse each line
        data = []
        # Get the base name (e.g., 'users.json' becomes 'users.json' without '.gz')
        base_name = file_path.split("/")[-1].replace(".gz", "")
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                # Skip empty lines and lines that start with the base name (likely header lines)
                if line.strip() and not line.startswith(base_name):
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return pd.DataFrame(data)
        

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

def convert_cpg(value):
    """
    Converts a MongoDB format cpg field (e.g.,
      {'$id': {'$oid': '601ac114be37ce2ead437550'}, '$ref': 'Cogs'})
    into a simplified dictionary with keys 'cpg_id' and 'cpg_ref'.
    """
    if isinstance(value, dict) and '$id' in value and '$ref' in value:
        # Extract the id string from the nested dictionary.
        if isinstance(value['$id'], dict) and '$oid' in value['$id']:
            cpg_id = value['$id']['$oid']
        else:
            cpg_id = value['$id']
        cpg_ref = value.get('$ref')
        return {'cpg_id': cpg_id, 'cpg_ref': cpg_ref}
    return value


def apply_conversions(df):
    """
    Apply conversions to a DataFrame by converting MongoDB ObjectIDs and dates.
    
    This function checks for known columns that may contain ObjectIDs (like '_id', 'userId', etc.)
    and known date columns, and applies the corresponding conversion functions.
    """
    # Convert ObjectID columns if they exist.
    oid_columns = ['_id', 'userId', 'brand_id']
    for col in oid_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_oid)
    
    # Define a list of common date column names
    date_columns = ['createdDate', 'lastLogin', 'purchaseDate', 'dateScanned', 'finishedDate', 'modifyDate', 'pointsAwardedDate']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_date)

    if 'cpg' in df.columns:
        df['cpg'] = df['cpg'].apply(convert_cpg)
    
    return df




# Define file paths for each dataset
file_paths = {
    'receipts': 'data/input_data/receipts.json.gz',
    'users': 'data/input_data/users.json.gz',
    'brands': 'data/input_data/brands.json.gz'
}




receipts_df = load_gzipped_json('data/input_data/receipts.json.gz')
rewardsReceiptItemList = receipts_df['rewardsReceiptItemList']
rewardsReceiptItemList_df = pd.DataFrame(rewardsReceiptItemList)
receipts_df = receipts_df.drop('rewardsReceiptItemList', axis=1)

users_df = load_gzipped_json('data/input_data/users.json.gz')

brands_df = load_gzipped_json('data/input_data/brands.json.gz')
cpg = brands_df['cpg']
cpg_df = pd.DataFrame(cpg)
brands_df = brands_df.drop('cpg', axis=1)

dataframes = {'receipts':receipts_df, 'users':users_df, 'brands':brands_df, 'rewardsReceiptItemList_df': rewardsReceiptItemList_df, 'cpg_df': cpg_df}


# Apply conversions to each DataFrame.
for name, df in dataframes.items():
    dataframes[name] = apply_conversions(df)

# for name, df in dataframes.items():
#     print(f"First 10 rows of {name}:")
#     print(df.head(10))
#     print("Shape of the DataFrame:", df.shape, "\n")

print(cpg_df['cpg_ref'].value_counts())

print(rewardsReceiptItemList)