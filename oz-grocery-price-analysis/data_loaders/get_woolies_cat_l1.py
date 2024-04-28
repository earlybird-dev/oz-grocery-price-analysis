from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path

from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By

import pandas as pd
import datetime
import time
import pytz


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


TZ = pytz.timezone('Australia/Sydney') 


def load_woolies_cat_l1_from_big_query():
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    query = 'SELECT * FROM grocery-price-analysis.scraping_data.woolies_cat_l1'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    try:
        data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
        return pd.DataFrame(data)
    except:
        return None


def get_new_woolies_cat_l1(driver):
    """
    Scrape main category data
    """

    SLEEP_TIME = 10
    now = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')
    
    categories = []

    stop_condition = False
    attempts = 0
    
    while not stop_condition:

        attempts += 1
        if attempts > 2:
            stop_condition = True

        url = f'https://www.woolworths.com.au/'
        driver.get(url)
        time.sleep(SLEEP_TIME)

        browse_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.wx-header__drawer-button')
        for button in browse_buttons:
            if 'Browse' in button.text:
                browse_button = button
                print('found browse button', button.text)
                browse_button.click()
                break
        print('clicked')
        time.sleep(SLEEP_TIME)

        browse_menu = driver.find_element(By.CLASS_NAME, 'category-list')
        browse_menu_items = browse_menu.find_elements(By.CSS_SELECTOR, 'a.ng-star-inserted')

        if len(browse_menu_items) > 0:
            stop_condition = True

            for item in browse_menu_items:
                cat_dict = {}

                cat_dict['updated_at'] = now
                cat_dict['newly_added'] = 1

                cat_dict['cat_l1_name'] = item.text.strip()
                cat_dict['cat_l1_link'] = item.get_attribute('href')

                cat_link_split = cat_dict['cat_l1_link'].split('/')
                cat_link_split = [i for i in cat_link_split if i != '']
                cat_dict['cat_l1_id'] = cat_link_split[-1]
                
                if 'tobacco' not in cat_dict['cat_l1_id']:
                    categories.append(cat_dict)

    categories = pd.DataFrame(categories)
    categories = categories[['updated_at', 'newly_added', 'cat_l1_id', 'cat_l1_name', 'cat_l1_link']]
    return categories


@data_loader
def load_data(*args, **kwargs):
    """
    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    # Initialising the webdriver
    service = Service()
    options = webdriver.ChromeOptions()
    # This blocks images and javascript requests
    chrome_prefs = {
        "profile.default_content_setting_values": {
            "images": 2,
            #"javascript": 2,
        }
    }
    options.experimental_options["prefs"] = chrome_prefs
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')
    driver = webdriver.Chrome(service=service, options=options)

    new_woolies_cat_l1 = get_new_woolies_cat_l1(driver)

    current_woolies_cat_l1 = load_woolies_cat_l1_from_big_query()
    if current_woolies_cat_l1 is not None:
        current_woolies_cat_l1['newly_added'] = 0

    woolies_cat_l1 = pd.concat([new_woolies_cat_l1, current_woolies_cat_l1])
    woolies_cat_l1 = woolies_cat_l1.groupby(by=['cat_l1_id', 'cat_l1_name', 'cat_l1_link'])
    woolies_cat_l1 = woolies_cat_l1[['newly_added', 'updated_at']].max().reset_index()
    woolies_cat_l1= woolies_cat_l1[['updated_at', 'newly_added', 'cat_l1_id', 'cat_l1_name', 'cat_l1_link']]

    print()
    print('woolies_cat_l1')
    print(woolies_cat_l1)
    print()

    print('driver.quit')
    driver.close()
    driver.quit()
    print()
    

    return woolies_cat_l1


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """

    assert output is not None, 'The output is undefined'