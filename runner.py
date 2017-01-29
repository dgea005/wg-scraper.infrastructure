import logging
import os
import time
import log
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
import scrapers



def scrape_search_index():
    """run full page scraper on first page of search results"""
    disk_engine = create_engine('sqlite:///database/listings.db')
    urls = ['http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.1.0.0.html',
            'http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.2.0.0.html']
    for url in urls:
        listing_html = scrapers.indexScraper(url).get_html()
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


def follow_listing_urls():
    """go through listing individual urls to get more detail"""
    # retrieve urls from clean table
    disk_engine = create_engine('sqlite:///database/listings.db')
    listings = pd.read_sql('select * from listings_clean', disk_engine)
    logging.info('pulled {} distinct urls from local db'.format(listings.shape[0]))

    listings.last_scrape_time = pd.to_datetime(listings.last_scrape_time)
    latest_listing_allowed = listings.last_scrape_time.max() - pd.Timedelta(hours=6)
    listings = listings.loc[listings.last_scrape_time >= latest_listing_allowed]

    listing_urls = listings.link.tolist()
    logging.info('list of {} listing URLs to scrape'.format(len(listing_urls)))

    # should functionalise below and go through in batches

    # get further details
    listing_details = []
    for url in listing_urls:
        scrape_results = scrapers.listingScraper(url).get_listing_html().parse_details()
        listing_details.append(scrape_results)
        time.sleep(10)
        logging.info('url: {} scraped; sleeping 10 seconds'.format(url))
    listing_details = pd.concat(listing_details)
    logging.info('retrieved {} link details'.format(listing_details.shape[0]))
    listing_details.to_sql('listing_dim', disk_engine, if_exists='append', index=False)
    logging.info('{} data written to listings_dim'.format(listing_details.shape))

def run_scheduler():
    """
    running scheduled scraping and other tasks
    """
    log.setup_logger()

    scheduler = BlockingScheduler()
    #scheduler.add_job(scrape_search_index, 'interval', minutes=2)
    #scheduler.add_job(write_clean_listings, 'interval', minutes=2)
    # --- currently have individual url scraper disabled - there is a request limit --- #

    scheduler.add_job(follow_listing_urls, 'interval', minutes=1)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == '__main__':
    run_scheduler()
