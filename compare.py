import pandas as pd

def compare_csv_column(file1, file2, column_name):
    try:
        # Load the CSV files
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)

        # Ensure the column exists in both files
        if column_name not in df1.columns or column_name not in df2.columns:
            return "The specified column does not exist in one or both files."

        # Initialize a list to store the indexes of different values
        diff_indexes = []

        # Compare the column values and store indexes of different values
        for index, (value1, value2) in enumerate(zip(df1[column_name], df2[column_name])):
            if value1 != value2:
                diff_indexes.append(index)

        return diff_indexes

    except Exception as e:
        return f"An error occurred: {e}"

# Example usage
result = compare_csv_column('1-safe.csv', '1-processed.csv', 'Display Name')
print(result)
