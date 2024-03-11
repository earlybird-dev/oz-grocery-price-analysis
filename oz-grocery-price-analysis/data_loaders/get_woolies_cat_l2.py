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

    query = 'SELECT * FROM grocery-price-analysis.raw_data.woolies_cat_l1 WHERE newly_added = 1'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    try:
        data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
        return pd.DataFrame(data)
    except:
        return None


def load_woolies_cat_l2_from_big_query():
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    query = 'SELECT * FROM grocery-price-analysis.raw_data.woolies_cat_l2'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    try:
        data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
        return pd.DataFrame(data)
    except:
        return None


def get_new_woolies_cat_l2(driver, woolies_cat_l1):
    """
    Scrape main category data
    """

    SLEEP_TIME = 3
    now = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')
    categories = []

    for index, cat_l1 in woolies_cat_l1.iterrows():
        cat_l1_id = cat_l1['cat_l1_id']
        cat_l1_link = cat_l1['cat_l1_link']

        driver.get(cat_l1_link)
        time.sleep(SLEEP_TIME)

        category_items = []
        try:
            browse_menu = driver.find_element(By.CLASS_NAME, 'chip-list')
            category_items = browse_menu.find_elements(By.CSS_SELECTOR, 'a.chip')
        except:
            pass
        
        for item in category_items:
            cat_dict = {}

            cat_dict['updated_at'] = now
            cat_dict['newly_added'] = 1

            cat_dict['cat_l1_id'] = cat_l1_id

            cat_dict['cat_l2_name'] = item.find_element(By.CSS_SELECTOR, 'span.chip-text').text.strip()
            cat_dict['cat_l2_link'] = item.get_attribute('href')

            cat_link_split = cat_dict['cat_l2_link'].split('/')
            cat_link_split = [i for i in cat_link_split if i != '']
            cat_dict['cat_l2_id'] = cat_link_split[-1]
            
            if (cat_l1_id in cat_dict['cat_l2_link']) & (cat_l1_id != cat_dict['cat_l2_id']):
                if ('tobacco' not in cat_dict['cat_l2_id']) & ('liquor' not in cat_dict['cat_l2_id']):
                    categories.append(cat_dict)

    categories = pd.DataFrame(categories)
    categories = categories[['updated_at', 'newly_added', 'cat_l1_id', 'cat_l2_id', 'cat_l2_name', 'cat_l2_link']]
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
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')
    driver = webdriver.Chrome(service=service, options=options)

    # Retrieve woolies level 1 categories
    woolies_cat_l1 = load_woolies_cat_l1_from_big_query()
    print('woolies_cat_l1', woolies_cat_l1)

    # Get woolies level 2 categories
    new_woolies_cat_l2 = get_new_woolies_cat_l2(driver, woolies_cat_l1)
    print('new_woolies_cat_l2', new_woolies_cat_l2)

    current_woolies_cat_l2 = load_woolies_cat_l2_from_big_query()
    if current_woolies_cat_l2 is not None:
        current_woolies_cat_l2['newly_added'] = 0

    woolies_cat_l2 = pd.concat([new_woolies_cat_l2, current_woolies_cat_l2])
    woolies_cat_l2 = woolies_cat_l2.groupby(by=['cat_l1_id', 'cat_l2_id', 'cat_l2_name', 'cat_l2_link'])
    woolies_cat_l2 = woolies_cat_l2[['newly_added', 'updated_at']].max().reset_index()
    woolies_cat_l2= woolies_cat_l2[['updated_at', 'newly_added', 'cat_l1_id', 'cat_l2_id', 'cat_l2_name', 'cat_l2_link']]
    
    print()
    print('woolies_cat_l2')
    print(woolies_cat_l2)
    print()

    return woolies_cat_l2


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """

    assert output is not None, 'The output is undefined'