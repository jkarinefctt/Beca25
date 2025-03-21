import zipfile
import os

def unzip_and_cleanup(zip_file_path, destination_dir):
    # Ensure the destination directory exists
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)
    
    try:
        # Open the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Extract all files to the destination directory
            zip_ref.extractall(destination_dir)
            print(f"Files extracted to {destination_dir}")

            # Check each extracted file to see if it's a zip file
            for file_name in zip_ref.namelist():
                extracted_file_path = os.path.join(destination_dir, file_name)
                if zipfile.is_zipfile(extracted_file_path):
                    # Recursively unzip nested zip files into the same directory
                    unzip_and_cleanup(extracted_file_path, destination_dir)
        
        # Delete the zip file after extraction and closure
        os.remove(zip_file_path)
        print(f"Deleted zip file: {zip_file_path}")
        
    except FileNotFoundError:
        print(f"The file {zip_file_path} was not found.")
    except zipfile.BadZipFile:
        print(f"The file {zip_file_path} is not a valid zip file.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
zip_file_path = 'C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/dataset.zip'  # Replace with your zip file path
destination_dir = 'C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Dataset'  # Replace with your desired extraction directory

unzip_and_cleanup(zip_file_path, destination_dir)
