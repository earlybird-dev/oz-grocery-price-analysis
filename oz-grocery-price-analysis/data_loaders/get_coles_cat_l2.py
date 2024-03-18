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


def load_coles_cat_l1_from_big_query():
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    query = 'SELECT * FROM grocery-price-analysis.scraping_data.coles_cat_l1 WHERE newly_added = 1'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    try:
        data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
        return pd.DataFrame(data)
    except:
        return None


def load_coles_cat_l2_from_big_query():
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    query = 'SELECT * FROM grocery-price-analysis.scraping_data.coles_cat_l2'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    try:
        data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
        return pd.DataFrame(data)
    except:
        return None


def get_new_coles_cat_l2(driver, coles_cat_l1):
    """Scrape main category data"""

    SLEEP_TIME = 3
    now = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')

    categories = []

    for index, cat_l1 in coles_cat_l1.iterrows():
        
        stop_condition = False
        attempts = 0
        
        while not stop_condition:

            attempts += 1
            if attempts > 2:
                stop_condition = True

            print()
            print(f'INDEX------------------: {index} - ATTEMPT------------------: {attempts}')

            cat_l1_id = cat_l1['cat_l1_id']
            cat_l1_link = cat_l1['cat_l1_link']
            
            if attempts > 1:
                print(cat_l1_link)

            driver.get(cat_l1_link)
            time.sleep(SLEEP_TIME)

            category_items = driver.find_elements(By.CSS_SELECTOR, 'a.coles-targeting-NavLinkLink')
            
            if len(category_items) > 0:
                stop_condition = True

                for item in category_items:
                    cat_dict = {}

                    cat_dict['updated_at'] = now
                    cat_dict['newly_added'] = 1

                    cat_dict['cat_l1_id'] = cat_l1_id

                    cat_dict['cat_l2_name'] = item.find_element(By.CSS_SELECTOR, 'span.coles-targeting-NavLinkLabel').text.strip()
                    cat_dict['cat_l2_link'] = item.get_attribute('href')

                    cat_link_split = cat_dict['cat_l2_link'].split('/')
                    cat_link_split = [i for i in cat_link_split if i != '']
                    cat_dict['cat_l2_id'] = cat_link_split[-1]
                    
                    if (cat_l1_id in cat_dict['cat_l2_link']) & (cat_l1_id != cat_dict['cat_l2_id']):
                        if 'tobacco' not in cat_dict['cat_l2_id']:
                            categories.append(cat_dict)

    categories = pd.DataFrame(categories)
    categories = categories[['updated_at', 'newly_added', 'cat_l1_id', 'cat_l2_id', 'cat_l2_name', 'cat_l2_link']]
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

    # Retrieve coles level 1 categories
    coles_cat_l1 = load_coles_cat_l1_from_big_query()
    print('coles_cat_l1', coles_cat_l1)

    # Get coles level 2 categories
    new_coles_cat_l2 = get_new_coles_cat_l2(driver, coles_cat_l1)
    print('new_coles_cat_l2', new_coles_cat_l2)

    current_coles_cat_l2 = load_coles_cat_l2_from_big_query()
    if current_coles_cat_l2 is not None:
        current_coles_cat_l2['newly_added'] = 0

    coles_cat_l2 = pd.concat([new_coles_cat_l2, current_coles_cat_l2])
    coles_cat_l2 = coles_cat_l2.groupby(by=['cat_l1_id', 'cat_l2_id', 'cat_l2_name', 'cat_l2_link'])
    coles_cat_l2 = coles_cat_l2[['newly_added', 'updated_at']].max().reset_index()
    coles_cat_l2= coles_cat_l2[['updated_at', 'newly_added', 'cat_l1_id', 'cat_l2_id', 'cat_l2_name', 'cat_l2_link']]
    
    print()
    print('coles_cat_l2')
    print(coles_cat_l2)
    print()

    return coles_cat_l2
 

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """

    assert output is not None, 'The output is undefined'