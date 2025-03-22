import csv

def clean_csv(input_file, output_file):
    cleaned_rows = []

    # Read the input file with ISO-8859 encoding
    with open(input_file, 'r', encoding='ISO-8859-1') as file:
        for line in file:
            # Replace tabs with semicolons and strip unnecessary whitespace
            cleaned_line = line.replace('\t', ';').strip()
            
            # Handle multilines by splitting at semicolons while preserving structure
            row = cleaned_line.split(';')
            cleaned_rows.append(row)

    # Write cleaned rows to output file with the same encoding
    with open(output_file, 'w', encoding='ISO-8859-1', newline='') as out_file:
        writer = csv.writer(out_file, delimiter=';')
        writer.writerows(cleaned_rows)

    print(f"Cleaned CSV file saved as {output_file}")

# Usage example
input_path = 'C:/Users/jkarinef/Beca25/extracted_files/pesquisas_eleitorais.csv'
output_path = 'C:/Users/jkarinef/Beca25/extracted_files/pesquisas_eleitorais_limpo.csv'
clean_csv(input_path, output_path)