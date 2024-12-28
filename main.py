import os
import zipfile
from typing import List, Optional

import pandas as pd
from icecream import ic


def extract_all_subdirs(zip_path: str, extract_to: str) -> List[str]:
    """
    Extracts all subdirectories within a zipped directory.

    Args:
        zip_path (str): The path to the zipped directory (dirA).
        extract_to (str): The directory to extract the contents of the zip files.

    Returns:
        List[str]: A list of paths to extracted files.
    """
    extracted_files = []
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_to)
        for file in z.namelist():
            full_path = os.path.join(extract_to, file)
            if os.path.isfile(full_path):
                extracted_files.append(full_path)
    return extracted_files


def process_csv_from_zip(zip_path: str) -> Optional[pd.DataFrame]:
    """
    Processes a zipped directory containing a single CSV file and returns the data as a DataFrame.

    Args:
        zip_path (str): The path to the zipped directory containing a CSV file.

    Returns:
        Optional[pandas.DataFrame]: A DataFrame of the CSV file's contents, or None if no CSV found.
    """
    with zipfile.ZipFile(zip_path, "r") as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            return None
        with z.open(csv_files[0]) as csv_file:
            return pd.read_csv(csv_file)


def combine_csvs_from_dir(zip_dir: str) -> pd.DataFrame:
    """
    Combines all CSV data from zipped subdirectories into a single DataFrame.

    Args:
        zip_dir (str): The path to the directory containing zipped subdirectories.

    Returns:
        pandas.DataFrame: A DataFrame combining all CSV data.
    """
    combined_data = []

    for root, _, files in os.walk(zip_dir):
        for file in files:
            if file.endswith(".zip"):
                zip_path = os.path.join(root, file)
                df = process_csv_from_zip(zip_path)
                if df is not None:
                    combined_data.append(df)

    if combined_data:
        return pd.concat(combined_data, ignore_index=True)
    else:
        return pd.DataFrame()  # Return an empty DataFrame if no data was found.


def dipc_raw_to_dataframe(dir_a: str) -> pd.DataFrame:
    """
    Main function to process the zipped directory and return a combined DataFrame.

    Args:
        dir_a (str): The path to the main zipped directory (dirA).

    Returns:
        pandas.DataFrame: The combined DataFrame containing all CSV data.
    """
    temp_dir = "temp_extracted"
    os.makedirs(temp_dir, exist_ok=True)

    try:
        extract_all_subdirs(dir_a, temp_dir)
        return combine_csvs_from_dir(temp_dir)
    finally:
        # Clean up the temporary directory
        for root, _, files in os.walk(temp_dir):
            for file in files:
                os.remove(os.path.join(root, file))
        os.rmdir(temp_dir)


def filter_dataframe(
    dataframe: pd.DataFrame, column: str, filter_text: str
) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the specified column matches the filter text.

    Args:
        dataframe (pd.DataFrame): The input DataFrame to filter.
        column (str): The column name to apply the filter on.
        filter_text (str): The text to filter rows by.

    Returns:
        pd.DataFrame: A new DataFrame containing only the filtered rows.

    Raises:
        ValueError: If the specified column does not exist in the DataFrame.
    """
    if column not in dataframe.columns:
        raise ValueError(f"Column '{column}' not found in the DataFrame.")

    filtered_df = dataframe[dataframe[column].astype(str) == filter_text]
    return filtered_df


def save_dataframe_to_csv(dataframe: pd.DataFrame, output_filename: str) -> None:
    """
    Saves a DataFrame to a CSV file. Creates a new file if it doesn't exist,
    and overwrites the file if it already exists.

    Args:
        dataframe (pd.DataFrame): The DataFrame to save as a CSV file.
        output_filename (str): The name of the output CSV file (including path if needed).

    Returns:
        None
    """
    try:
        dataframe.to_csv(output_filename, index=False)
        print(f"DataFrame successfully saved to '{output_filename}'.")
    except Exception as e:
        print(f"An error occurred while saving the DataFrame: {e}")


# Example Usage
if __name__ == "__main__":
    dir_a_path = "C:\\Users\\ps.public.PS-LT022-813B\\Downloads\\DIPC Energy Results &#8211; Raw_2024-12-23 0005-2024-12-24 0000.zip"
    combined_dataframe = dipc_raw_to_dataframe(dir_a_path)
    ppei_df = filter_dataframe(combined_dataframe, "RESOURCE_NAME", "10PPEI_U01")
    ic(ppei_df)
