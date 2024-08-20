from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
import pandas as pd
import os
import time
from datetime import date,datetime
from prefect import task



# Define the directory and the filename to keep

current_date = date.today()
date_today = date.today()

def read_existing_data(directory='./data'):
    """
    Reads the first CSV file in the specified directory that includes 'hello' in its filename.
    
    Parameters:
    directory (str): The path to the directory to search. Default is './data'.
    
    Returns:
    DataFrame: A pandas DataFrame containing the data from the CSV file.
    """
    # List all files in the directory
    all_files = os.listdir(directory)
    
    # Find the first file that includes 'hello' in its name and ends with '.csv'
    for file_name in all_files:
        if 'hello' in file_name.lower() and file_name.endswith('.csv'):
            file_path = os.path.join(directory, file_name)
            print(f"Reading file: {file_path}")
            return pd.read_csv(file_path)
    
    # If no matching file is found, return None or raise an error
    print("No matching file found.")
    return None


def cleanup_files():
    """
    Remove all files in the specified directory.
    This function will delete all files in the directory regardless of their names.
    """
    directory = './data'  # Define the directory containing the files

    # List all files in the directory
    all_files = os.listdir(directory)

    # Iterate over the files and delete them
    for file_name in all_files:
        file_path = os.path.join(directory, file_name)
        
        # Check if it is a file and delete it
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"Removed: {file_name}")
    
    print("Cleanup completed.")


# Example usage




def process_file(directory='./data'):
    """
    Check for CSV files in the specified directory with 'hello' in the file name,
    read it into a DataFrame, and find the maximum date from the 'Date' column.

    Parameters:
    directory (str): The path to the directory containing the CSV files.

    Returns:
    datetime.date: The maximum date found in the 'Date' column or None if no file is found or date is not available.
    """
    # List all files in the directory
    all_files = os.listdir(directory)
    
    # Find the first file that includes 'hello' in its name and ends with '.csv'
    file_path = None
    for file_name in all_files:
        if 'hello' in file_name.lower() and file_name.endswith('.csv'):
            file_path = os.path.join(directory, file_name)
            print(f"Reading file: {file_path}")
            break
    
    if file_path and os.path.isfile(file_path):
        # Open the file and read it into a DataFrame
        df = pd.read_csv(file_path)

        # Ensure the 'Date' column exists
        if 'Date' in df.columns:
            # Remove the trailing 'Z' if present and convert to datetime
            df['Date'] = df['Date'].str.rstrip("Z")
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Convert to datetime, ignore errors

            # Drop rows where 'Date' could not be parsed
            df = df.dropna(subset=['Date'])

            # Find the maximum date in the 'Date' column
            max_date = df['Date'].max()
            start_date = max_date.date() if pd.notna(max_date) else None

            if start_date:
                print(f"Start Date is {start_date}")
                print(f"Start date set to the maximum date in the file: {start_date.strftime('%d-%m-%Y')}")
            else:
                print("No valid dates found in the file.")

            return start_date
        else:
            print(f"'Date' column not found in the file '{file_path}'.")
            return None
    else:
        print("No matching file found.")
        return None
# Example usage

#start_date = process_file()

@task
def scrape_reviews(start_date):
    """
    Scrape reviews from Trustpilot and return a DataFrame with the results.

    Parameters:
    start_date (datetime.date): The date from which to start scraping.

    Returns:
    pd.DataFrame: A DataFrame containing the scraped review data.
    """
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    
    # Initialize the WebDriver
    driver = webdriver.Chrome(options=chrome_options)
    url = "https://ca.trustpilot.com/review/www.hellofresh.ca"
    driver.get(url)

    # Initialize lists to hold the scraped data
    date =    []
    content = []
    header =  []
    rate =    []

    page = 0
    current_date = datetime.today().date()
    
    while current_date > start_date:
        page += 1
        print(f"Web scraping for hello_fresh_canada in Page ... {page}")
        button = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//span[normalize-space()='Next page']")))
        driver.execute_script("arguments[0].click();", button)
        time.sleep(2)

        # Scraping the next pages
        date_list = driver.find_elements(By.XPATH, '//div/time[@data-service-review-date-time-ago="true"]')
        content_list = driver.find_elements(By.XPATH, '//p[@class="typography_body-l__KUYFJ typography_appearance-default__AAY17 typography_color-black__5LYEn"]')
        header_list = driver.find_elements(By.XPATH, '//h2[@class="typography_heading-s__f7029 typography_appearance-default__AAY17"]')
        rate_list = driver.find_elements(By.XPATH, '//div/img[contains(@alt,"Rated")]')
        
        len_list = min(len(date_list), len(content_list), len(header_list), len(rate_list))
        for p in range(len_list):
            date.append(date_list[p].get_attribute('datetime'))
            content.append(content_list[p].text)
            header.append(header_list[p].text)
            rate.append(rate_list[p].get_attribute("alt"))

        # Update current_date
        current_date = max(date_list, key=lambda x: x.get_attribute('datetime')).get_attribute('datetime')
        dt_object = datetime.fromisoformat(current_date.rstrip("Z"))
        current_date = dt_object.date()
        print(current_date)
        
    
    driver.quit()
    print("Scraping completed.")
    # Create DataFrame and return
    df = pd.DataFrame({
        'Rate': rate,
        'Content': content,
        'Header': header,
        'Date': date
    })
    # reading existing data
    existing_data = read_existing_data()
    cleanup_files()
    # Convert Date column to datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    new_data = df[df['Date'].dt.date >= start_date]
    if existing_data is not None:
        # If existing data was found, concatenate with the new data
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        # If no existing data was found, use the new data as the combined data
        combined_data = new_data
    
   
    # Generate the output filename with today's date
    output_filename =  f'./data/Hello_Fresh_ca_{date_today.strftime("%Y_%m_%d")}.csv'

    # Save the combined data to a CSV file
    combined_data.to_csv(output_filename, index=False)
    return combined_data

# Example usage




