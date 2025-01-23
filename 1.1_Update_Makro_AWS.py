import os
import requests
from bs4 import BeautifulSoup
import zipfile
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WriteOptions
from datetime import datetime

# Funktion, um einen temporären Ordner zu erstellen (z. B. in Lambda)
def create_temp_folder(folder_name):
    tmp_path = os.path.join('/tmp', folder_name)
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)
    return tmp_path

# Funktion zum Herunterladen und Speichern einer ZIP-Datei
def download_file(url, folder_path, year):
    response = requests.get(url)
    zip_path = os.path.join(folder_path, f'{year}.zip')
    with open(zip_path, 'wb') as file:
        file.write(response.content)
    return zip_path

# Funktion zum Entpacken und Verarbeiten der ZIP-Datei
def extract_zip(zip_path, folder_path, year):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(folder_path)
        extracted_file_name = [name for name in zip_ref.namelist() if name.endswith('.xls')][0]
        final_file_path = os.path.join(folder_path, f'{year}_CoT-Data.xls')
        if os.path.exists(final_file_path):
            os.remove(final_file_path)
        os.rename(os.path.join(folder_path, extracted_file_name), final_file_path)
        os.remove(zip_path)
        return final_file_path
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file.")
        os.remove(zip_path)
        return None

# Überprüfung, ob die Datei eine gültige Excel-Datei ist
def is_valid_excel(file_path):
    try:
        pd.read_excel(file_path, nrows=1)
        return True
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return False

# Funktion zur Aktualisierung der kombinierten Daten und Vorbereitung für InfluxDB
def process_data_for_influx(file_path):
    columns_to_keep = [
        "Market_and_Exchange_Names",
        "As_of_Date_In_Form_YYMMDD",
        "Open_Interest_All",
        "Prod_Merc_Positions_Long_ALL",
        "Prod_Merc_Positions_Short_ALL",
        "Swap_Positions_Long_All",
        "Swap__Positions_Short_All",
        "Swap__Positions_Spread_All",
        "M_Money_Positions_Long_ALL",
        "M_Money_Positions_Short_ALL",
        "M_Money_Positions_Spread_ALL",
        "Other_Rept_Positions_Long_ALL",
        "Other_Rept_Positions_Short_ALL",
        "Other_Rept_Positions_Spread_ALL",
        "Traders_Tot_All"
    ]
    column_rename_map = {
        "Market_and_Exchange_Names": "Market Names",
        "As_of_Date_In_Form_YYMMDD": "Date",
        "Open_Interest_All": "Open Interest",
        "Prod_Merc_Positions_Long_ALL": "Producer/Merchant/Processor/User Long",
        "Prod_Merc_Positions_Short_ALL": "Producer/Merchant/Processor/User Short",
        "Swap_Positions_Long_All": "Swap Dealer Long",
        "Swap__Positions_Short_All": "Swap Dealer Short",
        "Swap__Positions_Spread_All": "Swap Dealer Spread",
        "M_Money_Positions_Long_ALL": "Managed Money Long",
        "M_Money_Positions_Short_ALL": "Managed Money Short",
        "M_Money_Positions_Spread_ALL": "Managed Money Spread",
        "Other_Rept_Positions_Long_ALL": "Other Reportables Long",
        "Other_Rept_Positions_Short_ALL": "Other Reportables Short",
        "Other_Rept_Positions_Spread_ALL": "Other Reportables Spread",
        "Traders_Tot_All": "Total Traders"
    }
    market_filter = {
        "GOLD - COMMODITY EXCHANGE INC.": "Gold",
        "SILVER - COMMODITY EXCHANGE INC.": "Silver",
        "PLATINUM - NEW YORK MERCANTILE EXCHANGE": "Platinum",
        "PALLADIUM - NEW YORK MERCANTILE EXCHANGE": "Palladium",
        "COPPER- #1 - COMMODITY EXCHANGE INC.": "Copper"
    }

    if os.path.exists(file_path) and is_valid_excel(file_path):
        df = pd.read_excel(file_path, usecols=columns_to_keep)
        df.rename(columns=column_rename_map, inplace=True)
        df = df[df["Market Names"].isin(market_filter.keys())]
        df["Market Names"].replace(market_filter, inplace=True)
        df["Date"] = pd.to_datetime(df["Date"], format="%y%m%d")
        return df
    else:
        print(f"Invalid or missing file: {file_path}")
        return pd.DataFrame()

# Funktion zum Hochladen in InfluxDB
def upload_to_influxdb(df, bucket, org, token, url):
    if df.empty:
        print("No data to upload.")
        return

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=10_000))
    
    for _, row in df.iterrows():
        point = Point("cot_data") \
            .tag("market", row["Market Names"]) \
            .field("open_interest", row["Open Interest"]) \
            .field("producer_long", row["Producer/Merchant/Processor/User Long"]) \
            .field("producer_short", row["Producer/Merchant/Processor/User Short"]) \
            .time(row["Date"].isoformat())
        write_api.write(bucket=bucket, org=org, record=point)
    
    write_api.flush()
    client.close()

# Lambda-Handler-Funktion
def lambda_handler(event, context):
    folder_path = create_temp_folder('CoT-Data')
    year = datetime.utcnow().year
    base_url = 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm'
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    selector = '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(1) > td:nth-child(1) > a:nth-child(2)'
    link = soup.select_one(selector).get('href')
    download_url = 'https://www.cftc.gov' + link
    zip_path = download_file(download_url, folder_path, year)
    final_file_path = extract_zip(zip_path, folder_path, year)

    if final_file_path:
        df = process_data_for_influx(final_file_path)
        bucket = "CoT-Data"
        org = "cot-plotly"
        token = "YOUR_INFLUXDB_TOKEN"
        url = "https://YOUR_INFLUXDB_URL"
        upload_to_influxdb(df, bucket, org, token, url)
        print("Data successfully uploaded to InfluxDB.")
    else:
        print("No valid data file found.")

