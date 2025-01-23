import pandas as pd
import boto3
import os
from datetime import datetime

# Define the file path
file_path = os.path.join(os.getcwd(), "CoT-Data", "CoT-Data_Last-ten-years.xlsx")

# Set up Timestream client
timestream_client = boto3.client(
    'timestream-write',
    region_name='eu-north-1', 
    endpoint_url='https://CoT-Data-kikbifjjenx76n.eu-north-1.timestream-influxdb.amazonaws.com'  
)
# Define database and table names
database_name = 'CoT-Data'  
table_name = 'XLS'

# Read the Excel file into a DataFrame
df = pd.read_excel(file_path, sheet_name='Sheet1')

# Function to write data to AWS Timestream
def write_to_timestream(df, database_name, table_name):
    records = []
    for index, row in df.iterrows():
        # Create record for each row
        record = {
            'Dimensions': [
                {'Name': 'market_names', 'Value': str(row['Market Names'])}
            ],
            'MeasureName': 'cot_data',
            'MeasureValues': [
                {'Name': 'Open Interest', 'Value': str(row['Open Interest']), 'Type': 'DOUBLE'},
                {'Name': 'Producer/Merchant/Processor/User Long', 'Value': str(row['Producer/Merchant/Processor/User Long']), 'Type': 'DOUBLE'},
                {'Name': 'Producer/Merchant/Processor/User Short', 'Value': str(row['Producer/Merchant/Processor/User Short']), 'Type': 'DOUBLE'},
                {'Name': 'Swap Dealer Long', 'Value': str(row['Swap_Dealer_Long']), 'Type': 'DOUBLE'},
                {'Name': 'Swap Dealer Short', 'Value': str(row['Swap_Dealer_Short']), 'Type': 'DOUBLE'},
                {'Name': 'Swap Dealer Spread', 'Value': str(row['Swap_Dealer_Spread']), 'Type': 'DOUBLE'},
                {'Name': 'Managed Money Long', 'Value': str(row['Managed_Money_Long']), 'Type': 'DOUBLE'},
                {'Name': 'Managed Money Short', 'Value': str(row['Managed_Money_Short']), 'Type': 'DOUBLE'},
                {'Name': 'Managed Money Spread', 'Value': str(row['Managed_Money_Spread']), 'Type': 'DOUBLE'},
                {'Name': 'Other Reportables Long', 'Value': str(row['Other_Reportables_Long']), 'Type': 'DOUBLE'},
                {'Name': 'Other Reportables Short', 'Value': str(row['Other_Reportables_Short']), 'Type': 'DOUBLE'},
                {'Name': 'Other Reportables Spread', 'Value': str(row['Other_Reportables_Spread']), 'Type': 'DOUBLE'},
                {'Name': 'Total Traders', 'Value': str(row['Total_Traders']), 'Type': 'DOUBLE'}
                # Fügen Sie hier bei Bedarf weitere Felder hinzu
            ],
            'MeasureValueType': 'MULTI',
            'Time': str(int(datetime.utcnow().timestamp() * 1000))  # Verwenden Sie die aktuelle Zeit in Millisekunden
        }
        records.append(record)

        # Write in batches to avoid limits
        if len(records) >= 100:
            timestream_client.write_records(DatabaseName=database_name, TableName=table_name, Records=records)
            records = []  # Reset the records list

    # Write any remaining records
    if records:
        timestream_client.write_records(DatabaseName=database_name, TableName=table_name, Records=records)

# Write DataFrame to Timestream
write_to_timestream(df, database_name, table_name)
print("Data uploaded to AWS Timestream successfully.")
