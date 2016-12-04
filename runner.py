import pandas as pd
from scraper import Scraper

# so links display in HTML tables
pd.options.display.max_colwidth = 150

def run_scraper():
    """run full page scraper"""
    urls = ['http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.1.0.0.html',
            'http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.2.0.0.html']
    new_listings = pd.concat(Scraper(url).get_html().get_listings_from_page() for url in urls)
    print(new_listings)

if __name__ == '__main__':
    run_scraper()