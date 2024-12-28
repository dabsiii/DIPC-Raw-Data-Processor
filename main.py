import zipfile
from pathlib import Path
from typing import List, Optional

import pandas as pd
from icecream import ic

def extract_all_subdirs(zip_path: Path, extract_to: Path) -> List[Path]:
    """
    Extracts all subdirectories within a zipped directory.

    Args:
        zip_path (Path): The path to the zipped directory (dirA).
        extract_to (Path): The directory to extract the contents of the zip files.

    Returns:
        List[Path]: A list of paths to extracted files.
    """
    extracted_files = []
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_to)
        for file in z.namelist():
            full_path = extract_to / file
            if full_path.is_file():
                extracted_files.append(full_path)
    return extracted_files

def process_csv_from_zip(zip_path: Path) -> Optional[pd.DataFrame]:
    """
    Processes a zipped directory containing a single CSV file and returns the data as a DataFrame.

    Args:
        zip_path (Path): The path to the zipped directory containing a CSV file.

    Returns:
        Optional[pandas.DataFrame]: A DataFrame of the CSV file's contents, or None if no CSV found.
    """
    with zipfile.ZipFile(zip_path, "r") as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            return None
        with z.open(csv_files[0]) as csv_file:
            return pd.read_csv(csv_file)

def combine_csvs_from_dir(zip_dir: Path) -> pd.DataFrame:
    """
    Combines all CSV data from zipped subdirectories into a single DataFrame.

    Args:
        zip_dir (Path): The path to the directory containing zipped subdirectories.

    Returns:
        pandas.DataFrame: A DataFrame combining all CSV data.
    """
    combined_data = []

    for zip_path in zip_dir.rglob("*.zip"):
        df = process_csv_from_zip(zip_path)
        if df is not None:
            combined_data.append(df)

    if combined_data:
        return pd.concat(combined_data, ignore_index=True)
    else:
        return pd.DataFrame()  # Return an empty DataFrame if no data was found.

def dipc_raw_to_dataframe(dir_a: Path) -> pd.DataFrame:
    """
    Main function to process the zipped directory and return a combined DataFrame.

    Args:
        dir_a (Path): The path to the main zipped directory (dirA).

    Returns:
        pandas.DataFrame: The combined DataFrame containing all CSV data.
    """
    temp_dir = Path("temp_extracted")
    temp_dir.mkdir(exist_ok=True)

    try:
        extract_all_subdirs(dir_a, temp_dir)
        return combine_csvs_from_dir(temp_dir)
    finally:
        # Clean up the temporary directory
        for file in temp_dir.rglob("*"):
            file.unlink()
        temp_dir.rmdir()

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

def save_dataframe_to_csv(dataframe: pd.DataFrame, output_filename: Path) -> None:
    """
    Saves a DataFrame to a CSV file. Creates a new file if it doesn't exist,
    and overwrites the file if it already exists.

    Args:
        dataframe (pd.DataFrame): The DataFrame to save as a CSV file.
        output_filename (Path): The name of the output CSV file (including path if needed).

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
    dir_a_path = Path(
        "C:/Users/ps.public.PS-LT022-813B/Downloads/DIPC Energy Results &#8211; Raw_2024-12-23 0005-2024-12-24 0000.zip"
    )
    combined_dataframe = dipc_raw_to_dataframe(dir_a_path)
    ppei_df = filter_dataframe(combined_dataframe, "RESOURCE_NAME", "10PPEI_U01")
    ic(ppei_df)
