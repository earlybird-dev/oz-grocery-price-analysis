from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By

import pandas as pd
import datetime
import time

SLEEP_TIME = 3

def get_main_categories(driver, css_selector):
    """Scrape main category data"""

    url = f'https://www.coles.com.au/browse/'
    driver.get(url)
    time.sleep(SLEEP_TIME)

    category_items = driver.find_elements(By.CSS_SELECTOR, css_selector)
    categories = []
    for item in category_items:
        cat_dict = {}
        cat_dict["CAT_1"] = item.text
        cat_dict["cat_link"] = item.get_attribute('href')
        if ("tobacco" not in cat_dict["cat_link"].lower()) & ("liquor" not in cat_dict["cat_link"].lower()):
            print("cat_dict", cat_dict)
            categories.append(cat_dict)
    return categories

def get_child_categories(driver, parent_categories, css_selector, level):
    """Scrape product data given a category"""

    categories = []

    for category in parent_categories:
        cat_1 = category["CAT_1"]
        cat_link = category["cat_link"]
        cat_link_cut = cat_link.split("/")[-1]
        print("cat_link", cat_link)
        driver.get(cat_link)
        time.sleep(SLEEP_TIME)
        category_items = driver.find_elements(By.CSS_SELECTOR, css_selector)
        
        for item in category_items:
            cat_dict = {}
            cat_dict["CAT_1"] = cat_1
            cat_dict["CAT_2"] = item.text
            cat_dict["cat_link"] = item.get_attribute('href')
            if cat_link_cut in cat_dict["cat_link"]:
                if ("tobacco" not in cat_dict["cat_link"].lower()) & ("liquor" not in cat_dict["cat_link"].lower()):
                    print("cat_dict", cat_dict)
                    categories.append(cat_dict)
    return categories

def get_level_2_cat(parent_categories):
    """Scrape product data given a category"""

    # Initialising the webdriver
    service = Service()
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless=new")
    # options.add_argument("--no-sandbox")
    # options.add_argument('--disable-dev-shm-usage')
    # options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=service, options=options)

    # Get some sleep bro
    
    categories = []

    for category in parent_categories:
        cat_1 = category["CAT_1"]
        cat_link = category["cat_link"]
        cat_link_cut = cat_link.split("/")[-1]
        print("cat_link", cat_link)
        driver.get(cat_link)
        time.sleep(SLEEP_TIME)
        category_items = driver.find_elements(By.CSS_SELECTOR, 'a.coles-targeting-NavLinkLink')
        
        for item in category_items:
            cat_dict = {}
            cat_dict["CAT_1"] = cat_1
            cat_dict["CAT_2"] = item.text
            cat_dict["cat_link"] = item.get_attribute('href')
            if cat_link_cut in cat_dict["cat_link"]:
                if ("tobacco" not in cat_dict["cat_link"].lower()) & ("liquor" not in cat_dict["cat_link"].lower()):
                    print("cat_dict", cat_dict)
                    categories.append(cat_dict)
    return categories

def get_level_3_cat(parent_categories):
    """Scrape product data given a category"""

    # Initialising the webdriver
    service = Service()
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless=new")
    # options.add_argument("--no-sandbox")
    # options.add_argument('--disable-dev-shm-usage')
    # options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=service, options=options)

    # Get some sleep bro
    
    categories = []

    for category in parent_categories:
        cat_1 = category["CAT_1"]
        cat_2 = category["CAT_2"]
        cat_link = category["cat_link"]
        cat_link_cut = cat_link.split("/")[-1]
        print("cat_link", cat_link)
        driver.get(cat_link)
        time.sleep(SLEEP_TIME)
        category_items = driver.find_elements(By.CSS_SELECTOR, 'a.coles-targeting-NavLinkLink')
        
        for item in category_items:
            cat_dict = {}
            cat_dict["CAT_1"] = cat_1
            cat_dict["CAT_2"] = cat_2
            cat_dict["CAT_3"] = item.text
            cat_dict["cat_link"] = item.get_attribute('href')
            if cat_link_cut in cat_dict["cat_link"]:
                if ("tobacco" not in cat_dict["cat_link"].lower()) & ("liquor" not in cat_dict["cat_link"].lower()):
                    print("cat_dict", cat_dict)
                    categories.append(cat_dict)
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

    level_1_cat = get_main_categories(driver, "a.coles-targeting-ShopCategoriesShopCategoryStyledCategoryContainer")
    print("categories level 1", level_1_cat)

    level_2_cat = get_child_categories(driver, level_1_cat, "a.coles-targeting-NavLinkLink", 2)
    print("categories level 2", level_2_cat)

    categories = pd.DataFrame(level_2_cat)
    return categories

if __name__ == "__main__":
    load_data()