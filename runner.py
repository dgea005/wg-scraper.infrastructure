import logging
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
from scraper import Scraper


# setup
pd.options.display.max_colwidth = 150
log_formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger('scraper')
logger.setLevel(logging.INFO)
handler = logging.FileHandler('database/scraper.log')
logger.addHandler(handler)
handler.setFormatter(log_formatter)

disk_engine = create_engine('sqlite:////home/dg/projects/scrape_gesucht/database/listings.db')

def run_scraper():
    """run full page scraper"""
    urls = ['http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.1.0.0.html',
            'http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.2.0.0.html']
    for url in urls:
        listing_html = Scraper(url).get_html()
        listings = listing_html.get_listings_from_page()
        n_records = listings.shape[0]
        listings.to_sql('data', disk_engine, if_exists='append', index=False)
        logger.info('{} records written to listings.db'.format(n_records))

if __name__ == '__main__':
    run_scraper()
