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


def get_shadow_root(driver, element):
    return driver.execute_script('return arguments[0].shadowRoot', element)


def load_woolies_cat_l3_from_big_query():
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    query = """
        SELECT
            *
        FROM
            grocery-price-analysis.scraping_data.woolies_cat_l3
        WHERE
            newly_added = 1
            AND cat_l3_link NOT LIKE "%everyday-market%"
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
    return pd.DataFrame(data)


def export_product_data_to_big_query(df):
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    table_id = 'grocery-price-analysis.scraping_data.woolies_products'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='append',  # Specify resolution policy if table name already exists
    )


def export_fail_attempts_to_big_query(df):
    """
    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    table_id = 'grocery-price-analysis.scraping_data.woolies_cat_l3_fail_attempts'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='append',  # Specify resolution policy if table name already exists
    )


def scrape_data(driver, start_run_time, woolies_cat_l3):
    """
    Scrape product data given a category
    """

    now_time = datetime.datetime.now(TZ)

    SLEEP_TIME = 3
    products = []
    fail_to_get_product_tiles = []

    for index, cat_l3 in woolies_cat_l3.iterrows():

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
        pagination_items = driver.find_elements(By.CSS_SELECTOR, 'a.paging-pageNumber')
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

            # Find all the product tiles (shadow host)
            stop_condition = False
            attempts = 0
            
            while not stop_condition:
                page_url = f'{cat_l3_link}?pageNumber={i}'

                driver.get(page_url)
                time.sleep(SLEEP_TIME)

                product_count = 0
                product_tiles = driver.find_elements(By.TAG_NAME, 'wc-product-tile') # sometimes return null

                attempts += 1
                if attempts > 10:
                    stop_condition = True
                    fail_to_get_product_tiles.append({'start_run_time': start_run_time, 'time': datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S'), 'cat_l3_link_page_num': page_url})

                if len(product_tiles) > 0:
                    stop_condition = True

                    for product_tile in product_tiles:
                        shadow_root = get_shadow_root(driver, product_tile)

                        product_dict = {}

                        product_dict['start_run_time'] = start_run_time
                        product_dict['updated_at'] = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')

                        product_dict['cat_l1_id'] = cat_l1_id
                        product_dict['cat_l2_id'] = cat_l2_id
                        product_dict['cat_l3_id'] = cat_l3_id
                        product_dict['cat_l3_link'] = cat_l3_link
                        product_dict['cat_l3_link_updated_at'] = cat_l3_link_updated_at

                        # Get product link
                        try:
                            product_tile_image = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-image')
                            product_link = product_tile_image.find_element(By.TAG_NAME, 'a')
                            product_dict['product_link'] = product_link.get_attribute('href')
                        except:
                            product_dict['product_link'] = ''

                        # Get image link
                        try:
                            product_tile_image = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-image')
                            product_img_link = product_tile_image.find_element(By.TAG_NAME, 'img')
                            product_dict['product_img_link'] = product_img_link.get_attribute('src')
                        except:
                            product_dict['product_img_link'] = ''
                            
                        # Get product badge
                        try:
                            product_badge = shadow_root.find_element(By.CSS_SELECTOR, 'img.product-tile-roundel-image')
                            product_dict['product_badge'] = product_badge.get_attribute('alt')
                        except:
                            product_dict['product_badge'] = ''
                            
                        # Get product name
                        try:
                            product_name = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-title-container .title')
                            product_dict['product_name'] = product_name.text
                        except:
                            product_dict['product_name'] = ''
                            
                        # Get product price
                        try:
                            product_price = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-price .primary')
                            product_dict['product_price'] = product_price.text
                        except:
                            product_dict['product_price'] = ''  

                        # Get product price was
                        try:
                            product_price_was = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-price .secondary .was-price')
                            product_dict['product_price_was'] = product_price_was.text
                        except:
                            product_dict['product_price_was'] = ''
                            
                        # Get product badge label
                        try:
                            product_badge_label = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-v2--labels')
                            product_dict['product_badge_label'] = product_badge_label.text
                        except:
                            product_dict['product_badge_label'] = ''
                            
                        # Get product price calculation method
                        try:
                            product_price_calc_method = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-price .secondary .price-per-cup')
                            product_dict['product_price_calc_method'] = product_price_calc_method.text
                        except:
                            product_dict['product_price_calc_method'] = ''
                            
                        # Get product price calculation method desc
                        try:
                            product_title_promo_info = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-promo-info')
                            product_dict['product_title_promo_info'] = product_title_promo_info.text
                        except:
                            product_dict['product_title_promo_info'] = ''
                            
                        # Get product availability
                        try:
                            product_current_unavailable = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-unavailable-tag')
                            product_dict['product_current_unavailable'] = product_current_unavailable.text
                        except:
                            product_dict['product_current_unavailable'] = ''

                        # Get product sponsor text
                        try:
                            product_sponsor = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-title-container .sponsored-text')
                            product_sponsor_text = product_sponsor.text
                            product_dict['product_sponsor'] = product_sponsor_text
                        except:
                            product_dict['product_sponsor'] = ''
                        
                        # Get product sold by
                        try:
                            product_sold_by = shadow_root.find_element(By.CSS_SELECTOR, 'div.shelfProductTile-vendor-information')
                            product_sold_by_text = product_sold_by.text
                            product_dict['product_sold_by'] = product_sold_by_text
                        except:
                            product_dict['product_sold_by'] = ''

                        product_count += 1
                        products.append(product_dict)
                        
                    print("product_count: ", product_count)

        # if index > 1:
        #     break

    if len(products) > 0:
        product_df = pd.DataFrame(products)
        print()
        print(f'TOTAL ROWS: {len(product_df)}')
        print(f'RUNNING TIME: {datetime.datetime.now(TZ)-now_time}')
        print()
        export_product_data_to_big_query(product_df)
    
    if len(fail_to_get_product_tiles) > 0:
        fail_to_get_product_tiles_df = pd.DataFrame(fail_to_get_product_tiles)
        export_fail_attempts_to_big_query(fail_to_get_product_tiles_df)


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

    # Retieve woolies level 3 categories
    woolies_cat_l3 = load_woolies_cat_l3_from_big_query()

    # Scrape products data
    number_of_categories = len(woolies_cat_l3)
    bin_size = 10
    number_of_bin = math.ceil(number_of_categories / bin_size)

    for i in range(number_of_bin):
        start_index = i*bin_size
        end_index = min(((i+1)*bin_size, number_of_categories))
        print()
        print(f'i: {i}, start_index: {start_index}, end_index: {end_index-1}')
        print()

        sub_woolies_cat_l3 = woolies_cat_l3.iloc[start_index:end_index]
        scrape_data(driver, start_run_time, sub_woolies_cat_l3)

    print()
    return 'DONE!!!'


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    
    assert output is not None, 'The output is undefined'
