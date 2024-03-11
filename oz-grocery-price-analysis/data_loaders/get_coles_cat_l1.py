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

    query = 'SELECT * FROM grocery-price-analysis.raw_data.coles_cat_l1'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    try:
        data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
        return pd.DataFrame(data)
    except:
        return None


def get_new_coles_cat_l1(driver):
    """
    Scrape main category data
    """

    SLEEP_TIME = 3
    now = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')
    
    url = f'https://www.coles.com.au/browse/'
    driver.get(url)
    time.sleep(SLEEP_TIME)

    CSS_SELECTOR = 'a.coles-targeting-ShopCategoriesShopCategoryStyledCategoryContainer'
    category_items = driver.find_elements(By.CSS_SELECTOR, CSS_SELECTOR)
    categories = []

    for item in category_items:
        cat_dict = {}

        cat_dict['updated_at'] = now
        cat_dict['newly_added'] = 1

        cat_dict["cat_l1_name"] = item.text.strip()
        cat_dict["cat_l1_link"] = item.get_attribute('href')

        cat_link_split = cat_dict['cat_l1_link'].split('/')
        cat_link_split = [i for i in cat_link_split if i != '']
        cat_dict['cat_l1_id'] = cat_link_split[-1]
        
        if ('tobacco' not in cat_dict['cat_l1_id']) & ('liquor' not in cat_dict['cat_l1_id']):
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
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')
    driver = webdriver.Chrome(service=service, options=options)
    
    new_coles_cat_l1 = get_new_coles_cat_l1(driver)

    current_coles_cat_l1 = load_coles_cat_l1_from_big_query()
    if current_coles_cat_l1 is not None:
        current_coles_cat_l1['newly_added'] = 0

    coles_cat_l1 = pd.concat([new_coles_cat_l1, current_coles_cat_l1])
    coles_cat_l1 = coles_cat_l1.groupby(by=['cat_l1_id', 'cat_l1_name', 'cat_l1_link'])
    coles_cat_l1 = coles_cat_l1[['newly_added', 'updated_at']].max().reset_index()
    coles_cat_l1= coles_cat_l1[['updated_at', 'newly_added', 'cat_l1_id', 'cat_l1_name', 'cat_l1_link']]

    print()
    print('coles_cat_l1')
    print(coles_cat_l1)
    print()

    return coles_cat_l1


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """

    assert output is not None, 'The output is undefined'