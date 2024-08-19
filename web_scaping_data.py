from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
from datetime import date,datetime
start_date = "2024-08-11"
start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
current_date = date.today()
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920x1080")

driver = webdriver.Chrome(options=chrome_options)
url = "https://ca.trustpilot.com/review/www.hellofresh.ca"

driver.get(url)
# Scraping the first page
date_list = driver.find_elements(By.XPATH, '//div/time[@data-service-review-date-time-ago="true"]')
content_list = driver.find_elements(By.XPATH, '//p[@class="typography_body-l__KUYFJ typography_appearance-default__AAY17 typography_color-black__5LYEn"]')
header_list = driver.find_elements(By.XPATH, '//h2[@class="typography_heading-s__f7029 typography_appearance-default__AAY17"]')
rate_list = driver.find_elements(By.XPATH, '//div/img[contains(@alt,"Rated")]')

date = []
content = []
header = []
rate = []
for p in range(len(date_list)):
    date.append(date_list[p].get_attribute('datetime'))
    content.append(content_list[p].text)
    header.append(header_list[p].text)
    rate.append(rate_list[p].get_attribute("alt"))

df = pd.DataFrame({
    'Rate': rate,
    'Content': content,
    'Header': header,
    'Date': date
})

page = 0
while current_date > start_date:
    page += 1
    print(f"Page number {page}")
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

    
    current_date = max(date_list, key=lambda x: x.get_attribute('datetime')).get_attribute('datetime')
    dt_object = datetime.fromisoformat(current_date.rstrip("Z"))
    current_date = dt_object.date()
    print(current_date)
    print(min(date_list, key=lambda x: x.get_attribute('datetime')).get_attribute('datetime')) 
    df = pd.DataFrame({
        'Rate': rate,
        'Content': content,
        'Header': header,
        'Date': date
    })

# Convert Date column to datetime format
df['Date'] = pd.to_datetime(df['Date'])
df = df[df['Date'].dt.date >= start_date]

# Group by year and month, then save each group to a separate CSV file
for (year, month), group in df.groupby([df['Date'].dt.year, df['Date'].dt.month]):
    filename = f"Hello_Fresh_ca_{year}_{month}.csv"
    group.to_csv(filename, index=False)
    print(f"Saved {filename}")

driver.quit()
print("Scraping completed.")
