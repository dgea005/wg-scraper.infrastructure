"""functions to send emails with new listings"""
import logging
import os.path
import requests
import jinja2
import sqlalchemy
import pandas as pd
from secrets import key, sandbox, recipient
from user_filters import get_preferences

logger = logging.getLogger(__name__)

# TODO: do the selection of listings to send in a better way
# need to make sure a listing is new
# meetings search filters
# and has not been sent before

# would like this to take a json with the filter specifications

# needs more descriptive logging -- say why parts are filtered out

def get_listings_to_mail():
    """queries to get data"""
    db_connection = sqlalchemy.create_engine('sqlite:///database/listings.db')

    last_scraped_query = 'select max(last_scrape_time) last_scraped from listings_clean'
    latest_scrape_time = pd.read_sql(last_scraped_query, db_connection)
    min_first_scrape = pd.to_datetime(latest_scrape_time.last_scraped[0]) - pd.Timedelta(minutes=5)
    #min_first_scrape = min_first_scrape.strftime('%Y%m%d %H:%M:%S')
    new_listings_query = "select * from listings_clean"
    new_listings_query = "select * from listings_clean where first_scrape_time >= '{}'"
    new_listings_query = new_listings_query.format(min_first_scrape)

    latest_listings = pd.read_sql(new_listings_query, db_connection)
    

    def filter_previously_emailed(listings):
        """if listings have been scraped before filter them out"""
        try:
            n_listings = listings.shape[0]
            listings_sent = pd.read_sql('select * from listings_sent', db_connection)
            listings_sent_ids = listings_sent.listing_id.unique()
            logger.info('{} listings previously sent'.format(len(listings_sent_ids)))
            listings = listings.loc[~listings.listing_id.isin(listings_sent_ids)]
            logger.info('filtered from {} to {} listings to email'.format(n_listings, len(listings)))
            return listings
        except sqlalchemy.exc.OperationalError:
            logger.info('listings_sent does not exist yet')
            return listings

    latest_listings = filter_previously_emailed(latest_listings)
    logger.info('{} listings remain after removing previously sent'.format(latest_listings.shape[0]))

    if latest_listings.shape[0] == 0:
        return latest_listings

    # filter for user preferences
    def filter_for_user_preferences(listings):
        """filter listings to send by preferences as specified in user_filters.py"""
        preferences = get_preferences()
        start_number = listings.shape[0]
        listings.free_from = pd.to_datetime(listings.free_from)
        listings = (listings.
                        pipe(lambda df: df[df['free_from'] >= preferences['min_free_from']]).
                        pipe(lambda df: df[df['free_from'] <= preferences['max_free_from']]).
                        pipe(lambda df: df[df['cost'] <= preferences['max_cost']]).
                        pipe(lambda df: df[df['cost'] >= preferences['min_cost']]).
                        pipe(lambda df: df[((df['length'] >= preferences['min_length'])|df['length'].isnull())])
        )
        filtered_number = listings.shape[0]
        logger.info('filtered from {} to {} number of listings with preferences'.format(start_number, filtered_number))
        return listings

    latest_listings = filter_for_user_preferences(latest_listings)


    # keep record of sent listings so that we don't have duplicates
    sent_listings = latest_listings[['listing_id']].reset_index(drop=True)
    sent_listings.loc['sent_time'] = pd.Timestamp('now').strftime('%Y%m%d %H:%M:%S')
    sent_listings.to_sql('listings_sent', db_connection, if_exists='append', index=False)

    return latest_listings[['cost', 'link', 'free_from', 'length', 'flat_type']]

def create_html_doc(search_table):
    """takes a pandas dataframe of html as input"""
    # Capture our current directory
    THIS_DIR = os.path.dirname(os.path.abspath(__file__))
    # trim_blocks helps control whitespace
    j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(THIS_DIR), trim_blocks=True)
    return j2_env.get_template('email_template.html').render(listings=search_table)

def send_results_mail(html_document):
    """
    send scrape results with mailgun
    """
    request_url = 'https://api.mailgun.net/v2/{0}/messages'.format(sandbox)
    email_data = {'from': 'listings@wg_mg.com',
                  'to': recipient,
                  'subject': 'Latest listings',
                  'html': html_document}
    request = requests.post(request_url, auth=('api', key), data=email_data)
    logger.info('Status: {0}'.format(request.status_code))
    logger.info('Body: {0}'.format(request.text))

def send_email():
    """get latest scrapings and send them"""
    listings = get_listings_to_mail()
    if listings.shape[0] > 0:
        pd.set_option('display.max_colwidth', -1)
        html_doc = create_html_doc(listings.to_html())
        send_results_mail(html_doc)
        pd.set_option('display.max_colwidth', 100)
    else:
        logger.info('no new listings to send')
