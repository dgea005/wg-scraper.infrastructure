import logging
import os
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
from scraper import Scraper


def run_scraper():
    """run full page scraper"""
    disk_engine = create_engine('sqlite:///database/listings.db')
    urls = ['http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.1.0.0.html',
            'http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.2.0.0.html']
    for url in urls:
        listing_html = Scraper(url).get_html()
        listings = listing_html.get_listings_from_page()
        n_records = listings.shape[0]
        listings.to_sql('listings_stg', disk_engine, if_exists='append', index=False)
        logging.info('{} records written to listings_stg in listings.db'.format(n_records))


def run_scheduler():
    """
    running scheduled scraping and other tasks
    """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='database/scraper.log')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    scheduler = BlockingScheduler()
    scheduler.add_job(run_scraper, 'interval', minutes=1)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == '__main__':
    run_scheduler()
