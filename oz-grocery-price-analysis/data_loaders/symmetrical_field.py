from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By

import pandas as pd
import datetime
import time


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

SLEEP_TIME = 3


def get_level_2_cat(parent_categories):
    """Scrape product data given a category"""

    # Initialising the webdriver
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=service, options=options)

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
                if "tobacco" not in cat_dict["cat_link"].lower():
                    print("cat_dict", cat_dict)
                    categories.append(cat_dict)
    return categories

def get_level_3_cat(parent_categories):
    """Scrape product data given a category"""

    # Initialising the webdriver
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=service, options=options)
    
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
                if "tobacco" not in cat_dict["cat_link"].lower():
                    categories.append(cat_dict)
    return categories


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    level_1_cat = get_level_1_cat()
    print("categories level 1", level_1_cat)

    level_2_cat = get_level_2_cat(level_1_cat)
    print("categories level 2", level_2_cat)

    level_3_cat = get_level_3_cat(level_2_cat)
    print("categories level 3", level_3_cat)

    categories = pd.DataFrame(level_3_cat)
    print("categories", categories)

    return categories


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


if __name__ == "__main__":
    load_data()