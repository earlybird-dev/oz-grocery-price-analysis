from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By

import pandas as pd
import datetime
import time

SLEEP_TIME = 3

def get_woolies_cat_l1(driver):
    """Scrape main category data"""


    SLEEP_TIME = 3

    url = f'https://www.woolworths.com.au/'
    driver.get(url)
    time.sleep(SLEEP_TIME)

    browse_buttons = driver.find_elements(By.CSS_SELECTOR, "button.wx-header__drawer-button")
    for button in browse_buttons:
        if "Browse" in button.text:
            browse_button = button
            print("found browse button", button.text)
            browse_button.click()
            break
    print("clicked")
    time.sleep(SLEEP_TIME)
    # category_items = driver.find_element(By.CSS_SELECTOR, css_selector)
    # category_items = category_items.text
    # print(category_items)
    browse_menu = driver.find_element(By.CLASS_NAME, "category-list")
    browse_menu_items = browse_menu.find_elements(By.CSS_SELECTOR, "a.ng-star-inserted")

    categories = []
    for item in browse_menu_items:
        cat_dict = {}

        cat_dict["cat_l1_name"] = item.text
        cat_dict["cat_l1_link"] = item.get_attribute('href')

        cat_link_split = cat_dict["cat_l1_link"].split("/")
        cat_link_split = [i for i in cat_link_split if i != ""]
        cat_dict["cat_l1_id"] = cat_link_split[-1]
        
        if ("tobacco" not in cat_dict["cat_l1_id"]) & ("liquor" not in cat_dict["cat_l1_id"]):
            print("cat_dict", cat_dict)
            categories.append(cat_dict)

    categories = pd.DataFrame(categories)
    categories = categories[['cat_l1_id', 'cat_l1_name', 'cat_l1_link']]
    return categories



def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    # Initialising the webdriver
    service = Service()
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless=new")
    # options.add_argument("--no-sandbox")
    # options.add_argument('--disable-dev-shm-usage')
    # options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=service, options=options)

    level_1_cat = get_woolies_cat_l1(driver)

    categories = pd.DataFrame(level_1_cat)
    print(categories)
    return categories

if __name__ == "__main__":
    load_data()