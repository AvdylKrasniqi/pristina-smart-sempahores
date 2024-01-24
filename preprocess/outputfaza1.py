import pandas as pd

csv_file_path = '7-final.csv'

df = pd.read_csv(csv_file_path)

grouped_display_names = df.groupby('GroupID')['Display Name'].unique()


# Iterating over each group and printing the formatted output with spaces replaced by underscores
for group_id, names in grouped_display_names.items():
    names_with_underscores = [name.replace(" ", "_") for name in names]
    formatted_output = f"{len(names)} {' '.join(names_with_underscores)}"
    print(formatted_output)
