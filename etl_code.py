import glob                         # For reading file paths
import pandas as pd                # For reading CSV and JSON, and for DataFrame operations
import xml.etree.ElementTree as ET # For parsing XML files
from datetime import datetime      # For timestamps in logging

log_file = "log_file.txt"                 # For storing logs
target_file = "transformed_data.csv"     # Final transformed data output


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        row = pd.DataFrame([{"name": name, "height": height, "weight": weight}])
        dataframe = pd.concat([dataframe, row], ignore_index=True)
    
    return dataframe

def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    # Extract from all CSV files except the target output file
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    # Extract from all JSON files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    # Extract from all XML files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted_data

def transform(data):
    '''Convert height from inches to meters and round to 2 decimals'''
    data['height'] = round(data.height * 0.0254, 2)
    
    '''Convert weight from pounds to kilograms and round to 2 decimals'''
    data['weight'] = round(data.weight * 0.45359237, 2)
    
    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Example: 2025-Jul-18-14:32:05
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 

print("Transformed Data:")
print(transformed_data)

log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file, transformed_data) 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 
