import os
import requests
from bs4 import BeautifulSoup
import zipfile
import pandas as pd

# Funktion, um einen Ordner im /tmp-Verzeichnis von Lambda zu erstellen
def create_folder_in_tmp(folder_name):
    tmp_path = os.path.join('/tmp', folder_name)
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)
    return tmp_path

# Funktion zum Herunterladen und Speichern einer ZIP-Datei im /tmp-Verzeichnis
def download_file(url, folder_path, year):
    response = requests.get(url)
    zip_path = os.path.join(folder_path, f'{year}.zip')
    with open(zip_path, 'wb') as file:
        file.write(response.content)
    return zip_path

# Funktion zum Entpacken der ZIP-Datei
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

# Funktion zur Aktualisierung der kombinierten Excel-Datei
def update_combined_xls(folder_path, year, output_file_name):
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
        "Traders_Tot_All",
        "Traders_Prod_Merc_Long_All",
        "Traders_Prod_Merc_Short_All",
        "Traders_Swap_Long_All",
        "Traders_Swap_Short_All",
        "Traders_Swap_Spread_All",
        "Traders_M_Money_Long_All",
        "Traders_M_Money_Short_All",
        "Traders_M_Money_Spread_All",
        "Traders_Other_Rept_Long_All",
        "Traders_Other_Rept_Short_All",
        "Traders_Other_Rept_Spread_All"
    ]
    
    column_rename_map = {
        "Market_and_Exchange_Names": "Market Names",
        "As_of_Date_In_Form_YYMMDD": "Date",
        "Open_Interest_All": "Open Interest",
        "Prod_Merc_Positions_Long_ALL": "Producer/Merchant/Processor/User Long",
        "Prod_Merc_Positions_Short_ALL": "Producer/Merchant/Processor/User Short",
        "Swap_Positions_Long_All": "Swap_Dealer_Long",
        "Swap__Positions_Short_All": "Swap_Dealer_Short",
        "Swap__Positions_Spread_All": "Swap_Dealer_Spread",
        "M_Money_Positions_Long_ALL": "Managed_Money_Long",
        "M_Money_Positions_Short_ALL": "Managed_Money_Short",
        "M_Money_Positions_Spread_ALL": "Managed_Money_Spread",
        "Other_Rept_Positions_Long_ALL": "Other_Reportables_Long",
        "Other_Rept_Positions_Short_ALL": "Other_Reportables_Short",
        "Other_Rept_Positions_Spread_ALL": "Other_Reportables_Spread",
        "Traders_Tot_All": "Total_Traders",
        "Traders_Prod_Merc_Long_All": "Traders_Prod_Merc_Long",
        "Traders_Prod_Merc_Short_All": "Traders_Prod_Merc_Short",
        "Traders_Swap_Long_All": "Traders_Swap_Long",
        "Traders_Swap_Short_All": "Traders_Swap_Short",
        "Traders_Swap_Spread_All": "Traders_Swap_Spread",
        "Traders_M_Money_Long_All": "Traders_M_Money_Long",
        "Traders_M_Money_Short_All": "Traders_M_Money_Short",
        "Traders_M_Money_Spread_All": "Traders_M_Money_Spread",
        "Traders_Other_Rept_Long_All": "Traders_Other_Rept_Long",
        "Traders_Other_Rept_Short_All": "Traders_Other_Rept_Short",
        "Traders_Other_Rept_Spread_All": "Traders_Other_Rept_Spread"
    }

    file_path = os.path.join(folder_path, f'{year}_CoT-Data.xls')
    if os.path.exists(file_path) and is_valid_excel(file_path):
        df_new = pd.read_excel(file_path, usecols=columns_to_keep)
        df_new.rename(columns=column_rename_map, inplace=True)

        market_filter = {
            "GOLD - COMMODITY EXCHANGE INC.": "Gold",
            "SILVER - COMMODITY EXCHANGE INC.": "Silver",
            "PLATINUM - NEW YORK MERCANTILE EXCHANGE": "Platinum",
            "PALLADIUM - NEW YORK MERCANTILE EXCHANGE": "Palladium",
            "COPPER- #1 - COMMODITY EXCHANGE INC.": "Copper"
        }
        
        df_new = df_new[df_new["Market Names"].isin(market_filter.keys())]
        df_new["Market Names"].replace(market_filter, inplace=True)

        output_file_path = os.path.join(folder_path, f'{output_file_name}.xlsx')
        if os.path.exists(output_file_path):
            df_combined = pd.read_excel(output_file_path)
            df_combined = pd.concat([df_combined, df_new], ignore_index=True)
            df_combined.to_excel(output_file_path, index=False, engine='openpyxl')
        else:
            df_new.to_excel(output_file_path, index=False, engine='openpyxl')
    else:
        print(f"File {file_path} is not a valid Excel file or does not exist.")

def lambda_handler(event, context):
    folder_path = create_folder_in_tmp('CoT-Data')
    year = 2024
    base_url = 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm'
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    selector = '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(1) > td:nth-child(1) > a:nth-child(2)'
    link = soup.select_one(selector).get('href')
    download_url = 'https://www.cftc.gov' + link
    zip_path = download_file(download_url, folder_path, year)
    final_file_path = extract_zip(zip_path, folder_path, year)
    
    if final_file_path:
        update_combined_xls(folder_path, year, 'CoT-Data_Last-ten-years')
    else:
        print("No valid file was extracted.")




from influxdb_client import InfluxDBClient, Point, WriteOptions

def upload_to_influxdb(dataframe, bucket, org, token, url):
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=10_000))
    
    for _, row in dataframe.iterrows():
        point = Point("cot_data") \
            .tag("market", row["Market Names"]) \
            .field("open_interest", row["Open Interest"]) \
            .field("producer_long", row["Producer/Merchant/Processor/User Long"]) \
            .field("producer_short", row["Producer/Merchant/Processor/User Short"]) \
            .time(row["Date"])
        write_api.write(bucket=bucket, org=org, record=point)
    
    write_api.flush()
    client.close()

bucket = "CoT-Data"
org = "cot-plotly"
token = "3baLLLDojDOW9jpoBOx1ejzprCzsMHPpBhFADeEZuKJToIP6h_MjU3fsCwgtBIKC9Aaz3ufBNiL-cREirFbXCQ=="
url = "https://eu-central-1-1.aws.cloud2.influxdata.com"  

# Lade die gefilterten und verarbeiteten Daten hoch
upload_to_influxdb(df_new, bucket, org, token, url)

