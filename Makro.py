import os
import requests
from bs4 import BeautifulSoup
import zipfile
import pandas as pd

# Function to create folder on desktop
def create_folder_on_desktop(folder_name):
    desktop_path = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop')
    folder_path = os.path.join(desktop_path, folder_name)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return folder_path

# Function to download and verify zip file
def download_file(url, folder_path, year):
    response = requests.get(url)
    zip_path = os.path.join(folder_path, f'{year}.zip')
    with open(zip_path, 'wb') as file:
        file.write(response.content)
    return zip_path

# Function to extract files from zip if valid
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

# Function to verify if the file is a valid Excel file
def is_valid_excel(file_path):
    try:
        pd.read_excel(file_path, nrows=1)
        return True
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return False

# Function to combine xls files efficiently and rename columns
def combine_xls_files(folder_path, output_file_name):
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
    
    combined_data = []
    for year in range(2014, 2025):
        file_path = os.path.join(folder_path, f'{year}_CoT-Data.xls')
        if os.path.exists(file_path) and is_valid_excel(file_path):
            df = pd.read_excel(file_path, usecols=columns_to_keep)
            combined_data.append(df)
    combined_data = pd.concat(combined_data, ignore_index=True)
    
    # Rename columns
    combined_data.rename(columns={
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
    }, inplace=True)
    
    # Filter and rename market names
    market_filter = {
        "GOLD - COMMODITY EXCHANGE INC.": "Gold",
        "SILVER - COMMODITY EXCHANGE INC.": "Silver",
        "PLATINUM - NEW YORK MERCANTILE EXCHANGE": "Platinum",
        "PALLADIUM - NEW YORK MERCANTILE EXCHANGE": "Palladium",
        "COPPER- #1 - COMMODITY EXCHANGE INC.": "Copper"
    }
    
    combined_data = combined_data[combined_data["Market Names"].isin(market_filter.keys())]
    combined_data["Market Names"].replace(market_filter, inplace=True)
    
    output_file_path = os.path.join(folder_path, f'{output_file_name}.xlsx')
    if os.path.exists(output_file_path):
        os.remove(output_file_path)
    combined_data.to_excel(output_file_path, index=False, engine='openpyxl')

# Main function to orchestrate the process
def main():
    folder_path = create_folder_on_desktop('CoT-Data')
    base_url = 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm'
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    selectors = [
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(1) > td:nth-child(1) > a:nth-child(2)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(1) > td:nth-child(2) > a:nth-child(2)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(1) > td:nth-child(3) > a:nth-child(2)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(2) > td:nth-child(1) > a:nth-child(2)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(2) > td:nth-child(2) > a:nth-child(2)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(2) > td:nth-child(3) > a:nth-child(2)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(3) > td:nth-child(1) > a:nth-child(2)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(3) > td:nth-child(2) > a:nth-child(2)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(3) > td:nth-child(3) > a:nth-child(2)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(4) > td:nth-child(1) > a:nth-child(3)',
        '#content-container > section > div > article > div > div > table:nth-child(2) > tbody > tr:nth-child(4) > td:nth-child(2) > p > a:nth-child(2)'
    ]

    years = range(2024, 2013, -1)

    for selector, year in zip(selectors, years):
        link = soup.select_one(selector).get('href')
        download_url = 'https://www.cftc.gov' + link
        zip_path = download_file(download_url, folder_path, year)
        extract_zip(zip_path, folder_path, year)

    combine_xls_files(folder_path, 'CoT-Data_Last-ten-years')

if __name__ == "__main__":
    main()
