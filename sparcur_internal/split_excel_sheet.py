import time
import os
import pandas as pd

start_time = time.time()



# Define the path to the root directory containing the files
root_directory = "Data/Ussing chamber experiments for distension evoked secretion in human colon"

# Define the path to the new directory where the split files will be saved
new_directory = "new_data/Ussing chamber experiments for distension evoked secretion in human colon"

count = 0
log_file = "error_log.txt"
# Iterate over all the files and subdirectories
for root, dirs, files in os.walk(root_directory):
    for file in files:
        # Check if the file is an Excel file
        if file.endswith(".xlsx"):
          # Get the full file path
          file_path = os.path.join(root, file)
          try:
            count +=1
            # Load the Excel file
            excel_file = pd.ExcelFile(file_path)
            
            # Get the sheet names
            sheet_names = excel_file.sheet_names
            
            # Iterate over each sheet and save it as a separate file
            for sheet_name in sheet_names:
                # Read the sheet into a DataFrame
                df = excel_file.parse(sheet_name)
                
                # Create the new directory path based on the file's location
                new_dir_path = os.path.join(new_directory, os.path.relpath(root, root_directory))
                
                # Create the new directory if it doesn't exist
                os.makedirs(new_dir_path, exist_ok=True)
                
                # Create a new Excel file with the sheet name as the file name
                new_file_name = f"{sheet_name}.xlsx"
                new_file_path = os.path.join(new_dir_path, new_file_name)
                
                # Save the DataFrame as a new Excel file
                df.to_excel(new_file_path, index=False)
            print(f"Count: {count}")
          except Exception as e:
                # Log the error in the log file
                with open(log_file, "a") as f:
                    f.write(f"Error opening file: {file_path}\n")
                    f.write(f"Error message: {str(e)}\n")
                continue

end_time = time.time()
run_time = end_time - start_time
print(f"Runtime: {run_time} seconds")
