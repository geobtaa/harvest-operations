import pandas as pd

def find_matched_rows(csv_a_path, csv_b_path, output_csv_path):
    """
    Compares two CSV files based on an 'ID' column and writes the matched rows
    from the first file to a new CSV.

    Args:
        csv_a_path (str): The file path for the CSV containing metadata (CSV-A).
        csv_b_path (str): The file path for the CSV containing only IDs (CSV-B).
        output_csv_path (str): The file path for the output CSV (CSV-C).
    """
    try:
        # Read both CSV files into pandas DataFrames.
        # We assume the 'ID' column exists in both files.
        df_a = pd.read_csv(csv_a_path)
        df_b = pd.read_csv(csv_b_path)

        # Ensure the 'ID' column exists in both DataFrames to avoid errors.
        if 'ID' not in df_a.columns:
            print(f"Error: 'ID' column not found in {csv_a_path}")
            return
        if 'ID' not in df_b.columns:
            print(f"Error: 'ID' column not found in {csv_b_path}")
            return

        # Extract the list of IDs from CSV-B for efficient lookup.
        # Using a set provides faster lookups (O(1) on average).
        ids_in_b = set(df_b['ID'])

        # Filter rows in DataFrame A where the 'ID' IS in the set of IDs from B.
        matched_df = df_a[df_a['ID'].isin(ids_in_b)]

        # Check if any matched rows were found.
        if matched_df.empty:
            print("No matched IDs found. No IDs from CSV-A are present in CSV-B.")
            print("Output file will not be created.")
        else:
            # Write the resulting DataFrame of matched rows to a new CSV file.
            # index=False prevents pandas from writing the DataFrame index as a column.
            matched_df.to_csv(output_csv_path, index=False)
            print(f"Successfully created {output_csv_path} with {len(matched_df)} matched rows.")

    except FileNotFoundError as e:
        print(f"Error: The file was not found - {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- How to use the script ---

# 1. Replace these file paths with the actual paths to your CSV files.
#    Make sure the script is in the same directory as your files,
#    or provide the full path.
metadata_csv = 'CSV-A.csv'   # Your main data file
id_list_csv = 'CSV-B.csv'      # The file with the list of IDs to check against
output_csv = 'CSV-C.csv'       # The name of the file to be created

# 2. To run the script, you will need to create placeholder CSV files.
#    For example:
#
#    --- CSV-A.csv ---
#    ID,Name,Data
#    1,Apple,Fruit
#    2,Banana,Fruit
#    3,Carrot,Vegetable
#    4,Broccoli,Vegetable
#
#    --- CSV-B.csv ---
#    ID
#    1
#    3
#
#    Running the script with the above files will produce:
#
#    --- CSV-C.csv ---
#    ID,Name,Data
#    1,Apple,Fruit
#    3,Carrot,Vegetable

# 3. Call the function with your file paths.
find_matched_rows(metadata_csv, id_list_csv, output_csv)
