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


def scrape_data(category):
    """Scrape product data given a category"""

    # Initialising the webdriver
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=service, options=options)

    # Get some sleep bro
    sleep_time = 3
    url = f'https://www.coles.com.au/browse/{category}'
    driver.get(url)
    time.sleep(sleep_time)

    # Get number of product pages
    total_pages = 0
    pagination_items = driver.find_elements(By.CLASS_NAME, 'coles-targeting-PaginationPaginationItem')
    for item in pagination_items:
        total_pages = item.text
    total_pages = int(total_pages)
    print(f"Total pages: {total_pages}")

    products = []
    # total_pages = 1
    if total_pages > 0:
        # Scrape product data from each page
        for i in range(1, total_pages + 1):
            page_url = f'https://www.coles.com.au/browse/{category}?page={i}'
            driver.get(page_url)
            time.sleep(sleep_time)

            print(f"Page: {i}")
            product_count = 0

            # Find all the product tiles
            product_tiles = driver.find_elements(By.CLASS_NAME, 'coles-targeting-ProductTileProductTileWrapper')
            for product_tile in product_tiles:

                product_dict = {}

                # Get query time
                product_dict['time'] = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                product_dict['category'] = category

                # Get product tile hat
                try:
                    product_tile_hat = product_tile.find_element(By.CLASS_NAME, 'coles-targeting-ProductTileHat')
                    product_dict['product_tile_hat'] = product_tile_hat.text
                except:
                    product_dict['product_tile_hat'] = None
                    
                # Get product link
                try:
                    product_link = product_tile.find_element(By.CSS_SELECTOR, 'a.product__link')
                    product_dict['product_link'] = product_link.get_attribute('href')
                except:
                    product_dict['product_link'] = None
                    
                # Get product name
                try:
                    product_name = product_tile.find_element(By.CSS_SELECTOR, 'h2.product__title')
                    product_dict['product_name'] = product_name.text
                except:
                    product_dict['product_name'] = None
                    
                # Get product price
                try:
                    product_price = product_tile.find_elements(By.CSS_SELECTOR, 'span.price__value')
                    product_dict['product_price'] = (' ').join([item.text for item in product_price])
                except:
                    product_dict['product_price'] = None  

                # Get product price was
                try:
                    product_price_was = product_tile.find_elements(By.CSS_SELECTOR, 'span.price__was')
                    product_dict['product_price_was'] = (' ').join([item.text for item in product_price_was])
                except:
                    product_dict['product_price_was'] = None
                    
                # Get product badge label
                try:
                    product_badge_label = product_tile.find_elements(By.CSS_SELECTOR, 'section.badge-label')
                    product_dict['product_badge_label'] = (' ').join([item.text for item in product_badge_label])
                except:
                    product_dict['product_badge_label'] = None
                    
                # Get product price calculation method
                try:
                    product_price_calc_method = product_tile.find_elements(By.CSS_SELECTOR, 'div.price__calculation_method')
                    product_dict['product_price_calc_method'] = (' ').join([item.text for item in product_price_calc_method])
                except:
                    product_dict['product_price_calc_method'] = None
                    
                # Get product price calculation method
                try:
                    product_price_calc_method_desc = product_tile.find_elements(By.CSS_SELECTOR, 'span.price__calculation_method__description')
                    product_dict['product_price_calc_method_desc'] = (' ').join([item.text for item in product_price_calc_method_desc])
                except:
                    product_dict['product_price_calc_method_desc'] = None
                    
                # Get product price calculation method
                try:
                    product_current_unavailable = product_tile.find_elements(By.CSS_SELECTOR, 'div.coles-targeting-ProductTileCurrentlyUnavailableMessage')
                    product_dict['product_current_unavailable'] = (' ').join([item.text for item in product_current_unavailable])
                except:
                    product_dict['product_current_unavailable'] = None
                    
                product_count += 1
                products.append(product_dict)

            print(f"# of Products: {product_count}")

    product_df = pd.DataFrame(products)

    return product_df


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    categories = [
        'meat-seafood',
        'bakery',
        'drinks',
        'fruit-vegetables',
        'deli',
        'frozen',
        'baby',
        'dairy-eggs-fridge',
        'pantry',
        'household',
        # 'pet'
        # 'health-beauty',
        # 'liquor',
        ]
    all_products = pd.DataFrame()

    for category in categories:
        print(f"Started: {category}")
        product_df = scrape_data(category)
        all_products = pd.concat([all_products, product_df])
        print(f"Finished: {category}")
        print()
        print("----------")
    print("DONE!!!")
    return all_products


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
