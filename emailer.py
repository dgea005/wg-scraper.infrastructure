"""functions to send emails with new listings"""
import logging
import os.path
import requests
import jinja2
import sqlalchemy
import pandas as pd
from secrets import key, sandbox, recipient

logger = logging.getLogger(__name__)

# TODO: do the selection of listings to send in a better way
# need to make sure a listing is new
# meetings search filters
# and has not been sent before

def get_listings_to_mail():
    """queries to get data"""
    db_connection = sqlalchemy.create_engine('sqlite:///database/listings.db')

    min_time_query = 'select max(last_scrape_time) last_scraped from listings_clean'
    latest_scrape_time = pd.read_sql(min_time_query, db_connection)
    min_first_scrape = pd.to_datetime(latest_scrape_time.last_scraped[0]) - pd.Timedelta(minutes=5)

    new_listings_query = "select * from listings_clean where first_scrape_time >= '{}'"
    new_listings_query = new_listings_query.format(min_first_scrape)

    latest_listings = pd.read_sql(new_listings_query, db_connection)
    logger.info('{} listings to mail'.format(latest_listings.shape[0]))

    return latest_listings

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
        html_doc = create_html_doc(listings.to_html())
        send_results_mail(html_doc)
    else:
        logging.info('no new listings to send')
