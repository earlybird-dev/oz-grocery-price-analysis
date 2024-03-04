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


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    # Initialising the webdriver
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=service, options=options)

    CSS_SELECTOR = "a.coles-targeting-ShopCategoriesShopCategoryStyledCategoryContainer"
    
    coles_cat_l1 = get_coles_cat_l1(driver, CSS_SELECTOR)
    print("coles_cat_l1", coles_cat_l1)

    return coles_cat_l1


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'