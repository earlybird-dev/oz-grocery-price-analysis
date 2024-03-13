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

TZ = pytz.timezone("Australia/Sydney") 


def get_shadow_root(driver, element):
    return driver.execute_script('return arguments[0].shadowRoot', element)


def load_data_from_big_query():
    """
    Template for loading data from a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    query = 'SELECT * FROM grocery-price-analysis.raw_data.woolies_cat_l3'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    data = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
    return pd.DataFrame(data)


def export_data_to_big_query(df):
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    table_id = 'grocery-price-analysis.raw_data.woolies_products'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='append',  # Specify resolution policy if table name already exists
    )


def scrape_data(driver, woolies_cat_l3):
    """Scrape product data given a category"""

    now_time = datetime.datetime.now(TZ)
    now = now_time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"STARTED at {now}")

    SLEEP_TIME = 3
    CSS_SELECTOR = 'a.paging-pageNumber'
    products = []

    for index, cat_l3 in woolies_cat_l3.iterrows():

        print()
        print(f"INDEX---------------------------------: {index}")

        cat_l1_id = cat_l3["cat_l1_id"]
        cat_l2_id = cat_l3["cat_l2_id"]
        cat_l3_id = cat_l3["cat_l3_id"]
        cat_l3_link = cat_l3["cat_l3_link"]
        print("cat_l3_link", cat_l3_link)

        driver.get(cat_l3_link)
        time.sleep(SLEEP_TIME)

        # Get number of product pages
        total_pages = 1

        pagination_items = driver.find_elements(By.CSS_SELECTOR, CSS_SELECTOR)
        total_pages = max(1, int(len(pagination_items)))

        print(f"{cat_l1_id}/{cat_l2_id}/{cat_l3_id}")
        print(f"Total pages: {total_pages}")

        # Scrape product data from each page
        for i in range(1, total_pages + 1):
            page_url = f'{cat_l3_link}?pageNumber={i}'
            print(page_url)
            driver.get(page_url)
            time.sleep(SLEEP_TIME)

            print(f"Page: {i}")
            product_count = 0

            # Find all the product tiles (shadow host)
            stop_condition = False
            while not stop_condition:
                product_tiles = driver.find_elements(By.TAG_NAME, 'wc-product-tile') # sometimes return null
                print("len(product_tiles)", len(product_tiles))

                if len(product_tiles) > 0:
                    stop_condition = True

                    for product_tile in product_tiles:
                        shadow_root = get_shadow_root(driver, product_tile)

                        product_dict = {}

                        # Get query time
                        product_dict['updated_at'] = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')
                        product_dict['cat_l1_id'] = cat_l1_id
                        product_dict['cat_l2_id'] = cat_l2_id
                        product_dict['cat_l3_id'] = cat_l3_id
                        product_dict['cat_l3_link'] = cat_l3_link

                        # Get product link
                        try:
                            product_tile_image = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-image')
                            product_link = product_tile_image.find_element(By.TAG_NAME, 'a')
                            product_dict['product_link'] = product_link.get_attribute('href')
                        except:
                            product_dict['product_link'] = None

                        # Get image link
                        try:
                            product_tile_image = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-image')
                            product_image_url = product_tile_image.find_element(By.TAG_NAME, 'img')
                            product_dict['product_image_url'] = product_image_url.get_attribute('src')
                        except:
                            product_dict['product_image_url'] = None
                            
                        # Get product badge
                        try:
                            product_badge = shadow_root.find_element(By.CSS_SELECTOR, 'img.product-tile-roundel-image')
                            product_dict['product_badge'] = product_badge.get_attribute('alt')
                        except:
                            product_dict['product_badge'] = None
                            
                        # Get product name
                        try:
                            product_name = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-title-container .title')
                            product_dict['product_name'] = product_name.text
                        except:
                            product_dict['product_name'] = None
                            
                        # Get product price
                        try:
                            product_price = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-price .primary')
                            product_dict['product_price'] = product_price.text
                        except:
                            product_dict['product_price'] = None  

                        # Get product price was
                        try:
                            product_price_was = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-price .secondary .was-price')
                            product_dict['product_price_was'] = product_price_was.text
                        except:
                            product_dict['product_price_was'] = None
                            
                        # Get product badge label
                        try:
                            product_badge_label = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-v2--labels')
                            product_dict['product_badge_label'] = product_badge_label.text
                        except:
                            product_dict['product_badge_label'] = None
                            
                        # Get product price calculation method
                        try:
                            product_price_calc_method = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-price .secondary .price-per-cup')
                            product_dict['product_price_calc_method'] = product_price_calc_method.text
                        except:
                            product_dict['product_price_calc_method'] = None
                            
                        # Get product price calculation method desc
                        try:
                            product_title_promo_info = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-promo-info')
                            product_dict['product_title_promo_info'] = product_title_promo_info.text
                        except:
                            product_dict['product_title_promo_info'] = None
                            
                        # Get product availability
                        try:
                            product_current_unavailable = shadow_root.find_element(By.CSS_SELECTOR, 'div.product-tile-unavailable-tag')
                            product_dict['product_current_unavailable'] = product_current_unavailable.text
                        except:
                            product_dict['product_current_unavailable'] = None
                            
                        product_count += 1
                        products.append(product_dict)

                    print(f"# of Products: {product_count}")

        print()
        print(f"TOTAL ROWS: {len(products)}")
        print(datetime.datetime.now(TZ)-now_time)

        # if index > 10:
        #     break

    product_df = pd.DataFrame(products)

    print()
    now = datetime.datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')
    print(f"FINISHED at {now}")

    return product_df


@data_loader   
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=service, options=options)

    # Retieve coles level 3 categories
    woolies_cat_l3 = load_data_from_big_query()
    print("woolies_cat_l3", woolies_cat_l3)

    # Scrape products data
    all_products = scrape_data(driver, woolies_cat_l3)

    print()
    print("DONE!!!")
    return all_products


@test
def test_output(output, *args) -> '':
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
