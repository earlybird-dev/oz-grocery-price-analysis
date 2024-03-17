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


def load_coles_cat_l2_from_big_query():
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    query = 'SELECT * FROM grocery-price-analysis.scraping_data.coles_cat_l2 WHERE newly_added = 1'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    try:
        data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
        return pd.DataFrame(data)
    except:
        return None


def load_coles_cat_l3_from_big_query():
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    query = 'SELECT * FROM grocery-price-analysis.scraping_data.coles_cat_l3'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    try:
        data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
        return pd.DataFrame(data)
    except:
        return None


def get_new_coles_cat_l3(driver, coles_cat_l2):
    """
    Scrape main category data
    """

    SLEEP_TIME = 3
    now = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')

    CSS_SELECTOR = 'a.coles-targeting-NavLinkLink'
    categories = []

    for index, cat_l2 in coles_cat_l2.iterrows():

        print()
        print(f'INDEX---------------------------------: {index}')

        cat_l1_id = cat_l2['cat_l1_id']
        cat_l2_id = cat_l2['cat_l2_id']
        cat_l2_link = cat_l2['cat_l2_link']

        driver.get(cat_l2_link)
        time.sleep(SLEEP_TIME)

        category_items = driver.find_elements(By.CSS_SELECTOR, CSS_SELECTOR)
        
        for item in category_items:
            cat_dict = {}

            cat_dict['updated_at'] = now
            cat_dict['newly_added'] = 1

            cat_dict['cat_l1_id'] = cat_l1_id
            cat_dict['cat_l2_id'] = cat_l2_id

            cat_dict['cat_l3_name'] = item.find_element(By.CSS_SELECTOR, 'span.coles-targeting-NavLinkLabel').text.strip()
            cat_dict['cat_l3_link'] = item.get_attribute('href')

            cat_link_split = cat_dict['cat_l3_link'].split('/')
            cat_link_split = [i for i in cat_link_split if i != '']
            cat_dict['cat_l3_id'] = cat_link_split[-1]
            
            if (cat_l2_id in cat_dict['cat_l3_link']) & (cat_l2_id != cat_dict['cat_l3_id']):
                if 'tobacco' not in cat_dict['cat_l3_id']:
                    categories.append(cat_dict)

    categories = pd.DataFrame(categories)
    categories = categories[['updated_at', 'newly_added', 'cat_l1_id', 'cat_l2_id', 'cat_l3_id', 'cat_l3_name', 'cat_l3_link']]
    return categories

@data_loader
def load_data(*args, **kwargs):
    
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

    # Retrieve coles level 2 categories
    coles_cat_l2 = load_coles_cat_l2_from_big_query()
    print('coles_cat_l2', coles_cat_l2)

    # Get coles level 3 categories
    new_coles_cat_l3 = get_new_coles_cat_l3(driver, coles_cat_l2)

    current_coles_cat_l3 = load_coles_cat_l3_from_big_query()
    if current_coles_cat_l3 is not None:
        current_coles_cat_l3['newly_added'] = 0

    coles_cat_l3 = pd.concat([new_coles_cat_l3, current_coles_cat_l3])
    coles_cat_l3 = coles_cat_l3.groupby(by=['cat_l1_id', 'cat_l2_id', 'cat_l3_id', 'cat_l3_name', 'cat_l3_link'])
    coles_cat_l3 = coles_cat_l3[['newly_added', 'updated_at']].max().reset_index()
    coles_cat_l3= coles_cat_l3[['updated_at', 'newly_added', 'cat_l1_id', 'cat_l2_id', 'cat_l3_id', 'cat_l3_name', 'cat_l3_link']]
    
    print()
    print('coles_cat_l3')
    print(coles_cat_l3)
    print()

    return coles_cat_l3
 

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """

    assert output is not None, 'The output is undefined'