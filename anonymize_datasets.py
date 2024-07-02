import pandas as pd
import os
import pandas as pd
from datetime import datetime as dt, timedelta, date
import gc
import re
import hashlib
import logging


# Configure logging
logger = logging.getLogger()  # Get the root logger
logger.setLevel(logging.INFO)

# File Handler - logs to a file
file_handler = logging.FileHandler('anonymize_datasets.log')
file_format = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
file_handler.setFormatter(file_format)
logger.addHandler(file_handler)

# Stream Handler - logs to the console
stream_handler = logging.StreamHandler()
stream_format = logging.Formatter('%(levelname)s: %(message)s')
stream_handler.setFormatter(stream_format)
logger.addHandler(stream_handler)

def uid_anonymization(col: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to anonymize the uid column in a dataframe
    - col: name of the uid column in the dataframe
    - df: dataframe
    returns: dataframe with anonymized uid column
    """
    # Create a filepath for the key file
    key_filepath = f'./key/key_{col}.snappy.parquet'
    # Check if the key file exists
    if os.path.exists(key_filepath):
        # Read the key file into a dataframe
        key_df = pd.read_parquet(key_filepath)
        logger.info(f'key_{col}.snappy.parquet exists')
    else:
        # Create an empty dataframe for the key
        key_df = pd.DataFrame(columns=[col, f'{col}_'])
        logger.info(f'key_{col}.snappy.parquet does not exist, creating a new one')
    
    # Convert the uid column to string if it is not already
    df[col] = df[col].astype(str)

    # Check if 'uid' column exists in the dataframe
    # if 'uid' in df.columns:
        # Check if 'uid_' column exists in the key dataframe
    if f'{col}_' in key_df.columns:
        # Merge the dataframe with the key dataframe on 'uid' column
        df = df.merge(key_df, on=col, how='left')
        # Check if there are uids that do not match with uid_
        unmatched_uids = df[df[f'{col}_'].isnull()][f'{col}'].unique()
        # Generate new hashes for unmatched uids and append to the key dataframe
        if len(unmatched_uids) > 0:  # Check if unmatched_uids is not empty
            new_uids = pd.DataFrame({col: unmatched_uids})
            new_uids[f'{col}_'] = new_uids[f'{col}'].apply(lambda x: hashlib.blake2b(x.encode(), digest_size=5).hexdigest())
            key_df = pd.concat([key_df, new_uids], ignore_index=True)
        # Remove duplicates from the key dataframe
        key_df = key_df.drop_duplicates(subset=[f'{col}', f'{col}_'])
        key_df = key_df.reset_index(drop=True)
        # Save the key dataframe as a snappy.parquet file
        key_df.to_parquet(key_filepath, index=False, compression='snappy')
        logger.info(f'Updated {key_filepath}')
        # remove the uid column
        df = df.drop(columns=[col])
        # rename the uid_ column to uid
        df = df.rename(columns={f'{col}_': col})
    else:
        # Hash the uid column and save to the key dataframe
        df[f'{col}_'] = df[col].apply(lambda x: hashlib.blake2b(x.encode(), digest_size=5).hexdigest())
        key_df = df[[col, f'{col}_']].drop_duplicates()
        key_df = key_df.reset_index(drop=True)
        # Save the key dataframe as a snappy.parquet file
        key_df.to_parquet(key_filepath, index=False, compression='snappy')
        logger.info(f'Saved new key file: {key_filepath}')
        # remove the uid column
        df = df.drop(columns=[col])
        # rename the uid_ column to uid 
        df = df.rename(columns={f'{col}_': col})
    return df

def anonymize_multiple_attri_in_col(list_items: list, x: str, name='ANON_CLIENT')-> str:
    """
    Function anonymize items from an attribute file that is pivoted(possible multiple rows into a single row)
    -
    list_items: unique list of items that needs anonymization from a dataframe
        e.g. list_items = df[df['column1'] == 'some value']['column2'].unique()
    x: dataframe values for a specific column
    name: is the anonymize word chosen to replace personal identifiable value. Multiple unique values
        receive an incremental number e.g. ANON_CLIENT_1, ANON_CLIENT_2, ect.
    returns: specified anonymize values, or returns the original value not selected for anonymization
    """
    list_items = sorted(list_items)
    if x is None:  # return None type
        return x
    if len(list_items) == 1 and list_items[0] == x:
        return f'{name}'
    elif len(list_items) == 1 and re.search(fr'(?i)\b{list_items[0]}\b', x, re.IGNORECASE):
        return re.sub(fr'(?i)\b{re.escape(list_items[0])}\b', name, x)  # r' searches for capital or lowercase letters
    for idx, item in enumerate(list_items, start=1):
        if item == x:
            return f'{name} {idx}'
        elif re.search(fr'(?i)\b{list_items[0]}\b', x, re.IGNORECASE):
            return re.sub(fr'(?i)\b{re.escape(list_items[0])}\b', f'{name} {idx}', x)
    else:
        return x
    
def cdm_anonymization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to anonymize the CDM data
    - df: dataframe
    returns: dataframe with anonymized columns
    """
    anon_cols = ['CustomerCode','brand','customer']
    uids = ['uid','meter','meters']
    value_cols = ['Amount','forecast_gross', 'backcast_net', 'backcast_gross', 'usage_initial_net', 'usage_initial_gross', 'usage_final_net', 'usage_final_gross', 'usage','value','ams_actual_consumption','ams_actual_net','ams_actual_consumption_abs','forecast_abs_errors','backcast_abs_errors']

    for col in value_cols:
        if col.lower() in [column.lower() for column in df.columns]:
            df[col] = df[col] * 1.0125
    for col in anon_cols:
        if col.lower() in [column.lower() for column in df.columns]:
            unique_brands_list = df[col].unique()
            df[col] = df[col].apply(lambda x: anonymize_multiple_attri_in_col(unique_brands_list, x))
    for col in uids:
        if col.lower() in [column.lower() for column in df.columns]:
            df = uid_anonymization(col, df)
    # lowercase all columns
    df.columns = [column.lower() for column in df.columns]

    return df

def anonymize_files(input_directory: str, output_directory: str, client: str, timezone='America/Chicago', client_demo='client_demo') -> list:
    """
    Function to anonymize all CSV files in a directory and save them as snappy.parquet files

    - input_directory: path to the directory containing the CSV files
    - output_directory: path to the directory where the snappy.parquet files will be saved
    returns: list of output file paths
    """
    # Create the output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

        # Check if the directory exists
    if not os.path.isdir(input_directory):
        logger.error(f"The directory {input_directory} does not exist.")
        return
    
    # Get the latest date folder in the output directory (e.g. 2021-01-01)
    latest_date_folder = max(os.listdir(input_directory))
    logger.info(f'Anonymizing files from {latest_date_folder}...')

    list_output_files = []
    # Iterate over each CSV file in the input directory
    for filename in os.listdir(os.path.join(input_directory, latest_date_folder)):
        if filename.endswith('.csv'):
            # Read the CSV file into a dataframe
            logger.info(f'Reading: {filename}...')
            filepath = os.path.join(input_directory, latest_date_folder, filename)
            df = pd.read_csv(filepath)
            # anonymize the dataframe
            logger.info(f'Anonymizing: {filename}...')
            df = cdm_anonymization(df)
            # Add a process_time column to the dataframe
            if 'process_time' not in df.columns:
                df['process_time'] = dt.now().strftime('%Y-%m-%dT%H:%M:%S')
            # Add a timezone column to the dataframe
            if 'timezone' not in df.columns:
                df['timezone'] = timezone
            # Change the file name from client_name to client_demo
            new_filename = filename.replace(client, client_demo).replace('.csv', '.snappy.parquet')
            # Save the dataframe as a snappy.parquet file in the output directory
            output_filepath = os.path.join(output_directory, new_filename)
            logger.info(f'Saving: {new_filename}...')
            logger.info(f'Output path: {output_filepath}')
            df.to_parquet(output_filepath, index=False, compression='snappy')
            logger.info(f'Saved: {new_filename}\n')
            # Clean up memory
            del df
            gc.collect()
            # Add the output file path to the list
            list_output_files.append(output_filepath)            
        elif filename.endswith('.parquet'):
            # Read the parquet file into a dataframe
            logger.info(f'Reading: {filename}...')
            filepath = os.path.join(input_directory, latest_date_folder, filename)
            df = pd.read_parquet(filepath)
            # anonymize the dataframe
            logger.info(f'Anonymizing: {filename}...')
            df = cdm_anonymization(df)
            # Add a process_time column to the dataframe
            if 'process_time' not in df.columns:
                df['process_time'] = dt.now().strftime('%Y-%m-%dT%H:%M:%S')
            # Add a timezone column to the dataframe
            if 'timezone' not in df.columns:
                df['timezone'] = timezone
            # Change the file name from client_name to client_demo
            new_filename = filename.replace(client, client_demo).replace('.csv', '.snappy.parquet')
            # Save the dataframe as a snappy.parquet file in the output directory
            output_filepath = os.path.join(output_directory, new_filename)
            logger.info(f'Saving: {new_filename}...')
            logger.info(f'Output path: {output_filepath}')
            df.to_parquet(output_filepath, index=False, compression='snappy')
            logger.info(f'Saved: {new_filename}\n')
            # Clean up memory
            del df
            gc.collect()
            # Add the output file path to the list
            list_output_files.append(output_filepath)
    logger.info('Done, anonymizing and saving to parquet files!')
    return list_output_files


if __name__ == '__main__':
    # Usage of the function
    input_directory = './input_anon'
    output_directory = f'./output/{date.today()}'
    list_output_files = anonymize_files(
        input_directory, 
        output_directory, 
        client='client_name',
        timezone='America/New_York'
        )