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

import math


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


TZ = pytz.timezone('Australia/Sydney') 


def load_coles_cat_l3_from_big_query():
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    query = 'SELECT * FROM grocery-price-analysis.scraping_data.coles_cat_l3 WHERE newly_added = 1'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
    return pd.DataFrame(data)


def export_product_data_to_big_query(df):
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    table_id = 'grocery-price-analysis.scraping_data.coles_products'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='append',  # Specify resolution policy if table name already exists
    )


def scrape_data(driver, start_run_time, coles_cat_l3):
    """
    Scrape product data given a category
    """

    now_time = datetime.datetime.now(TZ)
    SLEEP_TIME = 3
    products = []

    for index, cat_l3 in coles_cat_l3.iterrows():
        
        print()
        print(f'INDEX---------------------------------: {index}')

        cat_l1_id = cat_l3['cat_l1_id']
        cat_l2_id = cat_l3['cat_l2_id']
        cat_l3_id = cat_l3['cat_l3_id']
        cat_l3_link = cat_l3['cat_l3_link']
        cat_l3_link_updated_at = cat_l3['updated_at']
        print(cat_l3_link)

        driver.get(cat_l3_link)
        time.sleep(SLEEP_TIME)

        # Get number of product pages
        pagination_items = driver.find_elements(By.CLASS_NAME, 'coles-targeting-PaginationPaginationItem')
        nums = [1]
        for item in pagination_items:
            page_number = item.text.strip()
            page_number = page_number.split()[-1]
            try:
                nums.append(int(page_number))
            except:
                pass
        total_pages = max(nums)

        # Scrape product data from each page
        for i in range(1, total_pages + 1):
            page_url = f'{cat_l3_link}?page={i}'
            driver.get(page_url)
            time.sleep(SLEEP_TIME)

            product_count = 0

            # Find all the product tiles
            product_tiles = driver.find_elements(By.CLASS_NAME, 'coles-targeting-ProductTileProductTileWrapper')
            for product_tile in product_tiles:

                product_dict = {}

                product_dict['start_run_time'] = start_run_time
                product_dict['updated_at'] = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')

                product_dict['cat_l1_id'] = cat_l1_id
                product_dict['cat_l2_id'] = cat_l2_id
                product_dict['cat_l3_id'] = cat_l3_id
                product_dict['cat_l3_link'] = cat_l3_link
                product_dict['cat_l3_link_updated_at'] = cat_l3_link_updated_at

                # Get product tile hat
                try:
                    product_tile_hat = product_tile.find_element(By.CLASS_NAME, 'coles-targeting-ProductTileHat')
                    product_dict['product_tile_hat'] = product_tile_hat.text
                except:
                    product_dict['product_tile_hat'] = ''
                    
                # Get product badge
                try:
                    product_badge = product_tile.find_element(By.CSS_SELECTOR, 'span.product__badge')
                    product_dict['product_badge'] = product_badge.text
                except:
                    product_dict['product_badge'] = ''
                    
                # Get product link
                try:
                    product_link = product_tile.find_element(By.CSS_SELECTOR, 'a.product__link')
                    product_dict['product_link'] = product_link.get_attribute('href')
                    
                except:
                    product_dict['product_link'] = ''

                # Get product img
                try:
                    product_img = product_tile.find_element(By.CSS_SELECTOR, 'a.product__image')                    
                    product_img_link = product_img.find_element(By.TAG_NAME, 'img')
                    product_dict['product_img_link'] = product_img_link.get_attribute('src')

                except:
                    product_dict['product_img_link'] = ''
                    
                # Get product name
                try:
                    product_name = product_tile.find_element(By.CSS_SELECTOR, 'h2.product__title')
                    product_dict['product_name'] = product_name.text
                except:
                    product_dict['product_name'] = ''
                    
                # Get product price
                try:
                    product_price = product_tile.find_elements(By.CSS_SELECTOR, 'span.price__value')
                    product_dict['product_price'] = (' ').join([item.text for item in product_price])
                except:
                    product_dict['product_price'] = ''  

                # Get product price was
                try:
                    product_price_was = product_tile.find_elements(By.CSS_SELECTOR, 'span.price__was')
                    product_dict['product_price_was'] = (' ').join([item.text for item in product_price_was])
                except:
                    product_dict['product_price_was'] = ''
                    
                # Get product badge label
                try:
                    product_badge_label = product_tile.find_elements(By.CSS_SELECTOR, 'section.badge-label')
                    product_dict['product_badge_label'] = (' ').join([item.text for item in product_badge_label])
                except:
                    product_dict['product_badge_label'] = ''
                    
                # Get product price calculation method
                try:
                    product_price_calc_method = product_tile.find_elements(By.CSS_SELECTOR, 'div.price__calculation_method')
                    product_dict['product_price_calc_method'] = (' ').join([item.text for item in product_price_calc_method])
                except:
                    product_dict['product_price_calc_method'] = ''
                    
                # Get product price calculation method desc
                try:
                    product_price_calc_method_desc = product_tile.find_elements(By.CSS_SELECTOR, 'span.price__calculation_method__description')
                    product_dict['product_price_calc_method_desc'] = (' ').join([item.text for item in product_price_calc_method_desc])
                except:
                    product_dict['product_price_calc_method_desc'] = ''
                    
                # Get product availability
                try:
                    product_current_unavailable = product_tile.find_elements(By.CSS_SELECTOR, 'div.coles-targeting-ProductTileCurrentlyUnavailableMessage')
                    product_dict['product_current_unavailable'] = (' ').join([item.text for item in product_current_unavailable])
                except:
                    product_dict['product_current_unavailable'] = ''
                    
                product_count += 1
                products.append(product_dict)

        # if index > 1:
        #     break

    if len(products) > 0:
        product_df = pd.DataFrame(products)
        print()
        print(f'TOTAL ROWS: {len(product_df)}')
        print(f'RUNNING TIME: {datetime.datetime.now(TZ)-now_time}')
        print()
        export_product_data_to_big_query(product_df)


@data_loader   
def load_data(*args, **kwargs):
    """
    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

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

    start_run_time = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')

    # Retieve coles level 3 categories
    coles_cat_l3 = load_coles_cat_l3_from_big_query()

    # Scrape products data
    number_of_categories = len(coles_cat_l3)
    bin_size = 10
    number_of_bin = math.ceil(number_of_categories / bin_size)

    for i in range(number_of_bin):
        start_index = i*bin_size
        end_index = min(((i+1)*bin_size, number_of_categories))
        print()
        print(f'i: {i}, start_index: {start_index}, end_index: {end_index-1}')
        print()
        
        sub_coles_cat_l3 = coles_cat_l3.iloc[start_index:end_index]
        scrape_data(driver, start_run_time, sub_coles_cat_l3)


    return ''


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """

    assert output is not None, 'The output is undefined'
