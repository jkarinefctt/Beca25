import os
import subprocess
import zipfile

def unzip_and_cleanup(zip_file_path, destination_dir):
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)
    
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(destination_dir)
            print(f"Files extracted to {destination_dir}")

            for file_name in zip_ref.namelist():
                extracted_file_path = os.path.join(destination_dir, file_name)
                if zipfile.is_zipfile(extracted_file_path):
                    unzip_and_cleanup(extracted_file_path, destination_dir)

        os.remove(zip_file_path)
        print(f"Deleted zip file: {zip_file_path}")
        
    except FileNotFoundError:
        print(f"The file {zip_file_path} was not found.")
    except zipfile.BadZipFile:
        print(f"The file {zip_file_path} is not a valid zip file.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
zip_file_path = 'C:/Users/jkarinef/Beca25/dataset.zip'
destination_dir = 'C:/Users/jkarinef/Beca25/extracted_files'

unzip_and_cleanup(zip_file_path, destination_dir)

# Encoding Conversion with Backup
folder_path = "C:/Users/jkarinef/Beca25/extracted_files"

for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        input_file = os.path.join(folder_path, filename)
        output_file = os.path.join(folder_path, f"{os.path.splitext(filename)[0]}_utf8.csv")
        
        try:
            # Build and execute the iconv command
            command = f"iconv -f ISO-8859-1 -t UTF-8 \"{input_file}\" -o \"{output_file}\""
            subprocess.run(command, shell=True, check=True)
            print(f"Converted {filename} to UTF-8 and saved as {output_file}")
        except subprocess.CalledProcessError:
            print(f"Failed to convert {filename}. Skipping...")
            continue

        # Only delete the original file after ensuring the output exists
        if os.path.exists(output_file):
            os.remove(input_file)
            print(f"Deleted original file: {input_file}")
        else:
            print(f"Conversion failed, original file retained: {input_file}")

print("All files have been processed.")
