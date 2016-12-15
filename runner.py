import logging
import os
import time
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
from scraper import indexScraper, listingScraper


def run_scraper():
    """run full page scraper"""
    disk_engine = create_engine('sqlite:///database/listings.db')
    urls = ['http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.1.0.0.html',
            'http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.2.0.0.html']
    for url in urls:
        listing_html = indexScraper(url).get_html()
        listings = listing_html.get_listings_from_page()
        n_records = listings.shape[0]
        listings.to_sql('listings_stg', disk_engine, if_exists='append', index=False)
        logging.info('{} records written to listings_stg in listings.db'.format(n_records))

def write_clean_listings():
    """deuplicate data in listings_stg and write to clean version"""
    # get the raw data -- all data scraped to date
    disk_engine = create_engine('sqlite:///database/listings.db')
    raw_listings = pd.read_sql('select * from listings_stg', disk_engine)
    n_listings = raw_listings.listing_id.nunique()
    logging.info('{} unique listing_ids in listings_stg'.format(n_listings))

    # some information for dealing with changes of listing details
    first_scraped_time = raw_listings.groupby('listing_id', as_index=False).agg({'scrape_time': min})
    first_scraped_time = first_scraped_time.rename(columns={'scrape_time': 'first_scrape_time'})
    last_scraped_time = raw_listings.groupby('listing_id', as_index=False).agg({'scrape_time': max})
    last_scraped_time = last_scraped_time.rename(columns={'scrape_time': 'last_scrape_time'})
    clean_listings = raw_listings.drop('scrape_time', axis=1).drop_duplicates()
    clean_listings = clean_listings.merge(first_scraped_time, on='listing_id')
    clean_listings = clean_listings.merge(last_scraped_time, on='listing_id')
    number_of_changes = clean_listings.groupby('listing_id', as_index=False).size()
    number_of_changes = pd.DataFrame(number_of_changes).rename(columns={0:'n_versions'}).reset_index()
    clean_listings = clean_listings.merge(number_of_changes, on='listing_id')

    # write processed dataframe
    logging.info('{} listings to write to listings_clean'.format(clean_listings.shape[0]))
    clean_listings.to_sql('listings_clean', disk_engine, if_exists='replace', index=False)
    logging.info('clean listings written')

def run_listing_url_scraper():
    """go through listing individual urls to get more detail"""
    # retrieve urls from clean table
    disk_engine = create_engine('sqlite:///database/listings.db')
    listing_urls = pd.read_sql('select distinct(link) from listings_clean', disk_engine)
    listing_urls = listing_urls.link.tolist()
    logging.info('pulled {} distinct urls from local db'.format(len(listing_urls)))
    # get the further details
    listing_details = []
    for url in listing_urls:
        listing_details.append(listingScraper(url).get_listing_html().parse_details())
        time.sleep(10)
        logging.info('url: {} scraped; sleeping 10 seconds'.format(url))
    listing_details = pd.concat(listing_details)
    logging.info('retrieved {} link details'.format(listing_details.shape[0]))
    listing_details.to_sql('listing_dim', disk_engine, if_exists='replace', index=False)
    logging.info('{} data written to listings_dim'.format(listing_details.shape))

def run_scheduler():
    """
    running scheduled scraping and other tasks
    """
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='database/scraper.log')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    scheduler = BlockingScheduler()
    #scheduler.add_job(run_scraper, 'interval', seconds=30)
    #scheduler.add_job(write_clean_listings, 'interval', minutes=1)
    scheduler.add_job(run_listing_url_scraper, 'interval', seconds=10)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == '__main__':
    run_scheduler()
