import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
import os

# Define the file path
file_path = os.path.expanduser("~/Desktop/CoT-Data/CoT-Data_Last-ten-years.xlsx")

# Initialize the InfluxDB client
token = "D7cFFZ7LzmMJn0qO-85EYhjRfhKqydGLSE-gkoR-e1RLivHN_Ud_mj_r5s4kGXKHtUg-n0xI7I3L0-6Uv8ZVhw=="
org = "Student"
bucket = "CoT-Data"

client = InfluxDBClient(url="http://localhost:8086", token=token)

# Read the Excel file into a DataFrame
df = pd.read_excel(file_path, sheet_name='Sheet1')

# Prepare data for InfluxDB
write_api = client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=10_000))

# Iterate through the DataFrame and write data points to InfluxDB
for index, row in df.iterrows():
    point = Point("cot_data") \
        .tag("market_names", row['Market Names']) \
        .field("Open Interest", row['Open Interest']) \
        .field("Producer/Merchant/Processor/User Long", row['Producer/Merchant/Processor/User Long']) \
        .field("Producer/Merchant/Processor/User Short", row['Producer/Merchant/Processor/User Short']) \
        .field("Swap Dealer Long", row['Swap_Dealer_Long']) \
        .field("Swap Dealer Short", row['Swap_Dealer_Short']) \
        .field("Swap Dealer Spread", row['Swap_Dealer_Spread']) \
        .field("Managed Money Long", row['Managed_Money_Long']) \
        .field("Managed Money Short", row['Managed_Money_Short']) \
        .field("Managed Money Spread", row['Managed_Money_Spread']) \
        .field("Other Reportables Long", row['Other_Reportables_Long']) \
        .field("Other Reportables Short", row['Other_Reportables_Short']) \
        .field("Other Reportables Spread", row['Other_Reportables_Spread']) \
        .field("Total Traders", row['Total_Traders']) \
        .field("Traders Prod/Merc Long", row['Traders_Prod_Merc_Long']) \
        .field("Traders Prod/Merc Short", row['Traders_Prod_Merc_Short']) \
        .field("Traders Swap Long", row['Traders_Swap_Long']) \
        .field("Traders Swap Short", row['Traders_Swap_Short']) \
        .field("Traders Swap Spread", row['Traders_Swap_Spread']) \
        .field("Traders M Money Long", row['Traders_M_Money_Long']) \
        .field("Traders M Money Short", row['Traders_M_Money_Short']) \
        .field("Traders M Money Spread", row['Traders_M_Money_Spread']) \
        .field("Traders Other Rept Long", row['Traders_Other_Rept_Long']) \
        .field("Traders Other Rept Short", row['Traders_Other_Rept_Short']) \
        .field("Traders Other Rept Spread", row['Traders_Other_Rept_Spread']) \
        .time(pd.to_datetime(row['Date'], format='%y%m%d'), WritePrecision.NS)
    write_api.write(bucket=bucket, org=org, record=point)

# Ensure all data is written and wait until all pending writes are completed
write_api.__del__()

# Close the client connection
client.__del__()
