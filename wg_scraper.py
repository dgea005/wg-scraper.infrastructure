import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from secrets import key, sandbox, recipient
from jinja2 import Environment, FileSystemLoader
import os.path
import numpy as np
from apscheduler.schedulers.background import BackgroundScheduler
import time


## so links display in HTML tables
pd.options.display.max_colwidth = 150


######################################################
#####         functions to scrape urls          ######
######################################################

def get_search_results(url):
    """make request to get html and soup"""
    r = requests.get(url)
    assert r.status_code == 200, 'status code not 200'
    html_doc = r.text
    return BeautifulSoup(html_doc, 'html.parser')


def get_listing_details(listing):
    """get details of one listing from parsed html response"""
    href_link = list(set(filter(None.__ne__, [tag.get('href') for tag in listing.findAll('a')])))
    link = 'http://www.wg-gesucht.de/en/' + href_link[0]
    cost = (listing.find('td', class_='ang_spalte_miete').
                find('span').contents[1].contents[0].
                replace(' ','').replace('\n', '').replace('€',''))
    size = (listing.find('td', class_='ang_spalte_groesse').
                find('span').contents[0].
                replace(' ','').replace('\n', '').replace('m²', ''))
    stadt = (listing.find('td', class_='ang_spalte_stadt').
                find('span').contents[0].
                replace(' ','').replace('\n', ''))
    free_from = (listing.find('td', class_='ang_spalte_freiab').
                    find('span').contents[0])
    free_to = listing.find('td', class_ = 'ang_spalte_freibis').find('span')
    if free_to:
        free_to = free_to.contents[0]
    else:
        free_to = None
    
    
    return {'link': link,'cost': cost,'size': size,'stadt': stadt,
            'free_from': free_from, 'free_to': free_to}


def get_latest_listing_details(url):
    """get listings out of one url, also process dtypes, etc a bit"""
    soup = get_search_results(url)
    
    ## depending on link we are searching in the 1zimmer or flat part of wg-gesucht
    if '8.2.0.0' in url:
        flat_type = 'flat'
    elif '8.1.0.0' in url:
        flat_type = 'studio'
    else:
        flat_type = ''
    
    search_results = pd.DataFrame(get_listing_details(prop)
                      for prop in soup.findAll('tr', class_=re.compile('listenansicht0|listenansicht1')))
    
    search_results = (search_results.
                         assign(cost = search_results['cost'].astype(int)).
                         assign(size = search_results['size'].astype(int)).
                         assign(free_from = pd.to_datetime(search_results.free_from, dayfirst=True)).
                         assign(free_to = pd.to_datetime(search_results.free_to, dayfirst=True)).
                         assign(length = lambda df: (df['free_to'] - df['free_from'])/pd.Timedelta(days=1)).
                         assign(scrape_time = pd.Timestamp('now').replace(second=0, microsecond=0)).
                         assign(flat_type = flat_type)
                     )
    
    return search_results


def get_previous_listings(file):
    """open persisted previous listings file"""
    previous_scrapings = pd.read_csv(file)

    def convert_dtypes(data):
        return (data.
                    assign(free_from = lambda df: pd.to_datetime(df['free_from'])).
                    assign(free_to = lambda df: pd.to_datetime(df['free_to'])).
                    assign(scrape_time = lambda df: pd.to_datetime(df['scrape_time']))
                )

    return convert_dtypes(previous_scrapings)


def filter_old_out(new, old):
    """filter out previously seen listing urls"""
    unseen_links = np.setdiff1d(new.link.values, old.link.values)
    unseen_listings = new[new.link.isin(unseen_links)]
    return unseen_listings


def filter_requirements(listings):
    """filters for price and dates"""
    min_free_from = pd.Timestamp('2016-09-28')
    max_free_from = pd.Timestamp('2016-11-01')
    max_cost = 700
    min_cost = 300
    min_length = 90
    
    return (listings.
                pipe(lambda df: df[df['free_from'] >= min_free_from]).
                pipe(lambda df: df[df['free_from'] <= max_free_from]).
                pipe(lambda df: df[df['cost'] <= max_cost]).
                pipe(lambda df: df[df['cost'] >= min_cost]).
                pipe(lambda df: df[((df['length'] >= min_length)|df['length'].isnull())])
    )



def get_listings_from_url(url_, data_file):
    """
    run scraper on one url; persist all results and 
    return deduplicated filtered listings
    - this has the logic with whether to send results
    """
    current_listings = get_latest_listing_details(url_)
    if os.path.isfile(data_file):
        ## read previous listings
        previous_listings = get_previous_listings(data_file)
        
        ## append ALL listings to data file
        current_listings.to_csv(data_file, index=False, mode='a', header=False)
        
        ## apply url filter
        current_listings = filter_old_out(current_listings, previous_listings)
    else:
        ## else is if the file doesn't exist - should only happen on first run
        ## write first instance to csv
        current_listings.to_csv(data_file, index=False)
    
    ## return listings with desired features
    return filter_requirements(current_listings)


######################################################
#####      functions for emailing results       ######
######################################################


def create_html_doc(search_table):
    # Capture our ctitleurrent directory
    THIS_DIR = os.path.dirname(os.path.abspath(__file__))
    # Create the jinja2 environment.
    # Notice the use of trim_blocks, which greatly helps control whitespace.
    j2_env = Environment(loader=FileSystemLoader(THIS_DIR),
                         trim_blocks=True)
    return (j2_env.
            get_template('email_template.html').
            render(listings=search_table))


def send_results_mail(html_document):
    """
    send scrape results with mailgun
    """
    request_url = 'https://api.mailgun.net/v2/{0}/messages'.format(sandbox)
    request = requests.post(request_url,
                            auth=('api', key),
                            data={'from': 'listings@wg_mg.com',
                                    'to': recipient,
                                    'subject': 'Latest listings',
                                    'html': html_document})
    print('Status: {0}'.format(request.status_code))
    print('Body:   {0}'.format(request.text))



def run_scraper():
    urls = ['http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.1.0.0.html',
            'http://www.wg-gesucht.de/en/wohnungen-in-Berlin.8.2.0.0.html']
    data_file = 'storage.csv'
    new_listings = pd.concat(get_listings_from_url(url, data_file) for url in urls)
    n_new_listings = new_listings.shape[0]
    print(n_new_listings)
    if  n_new_listings >= 1: ## if there are new listings send them
        print('send {} listings'.format(n_new_listings))
        html_doc = create_html_doc(new_listings.to_html(index=False))
        send_results_mail(html_doc)
    else:
        print('no listings to send')


if __name__ == '__main__':
    ### 
    ## can specify to run every x minutes here
    ## would like to specify start and stop time 
    ## periods so it doesn't run all the time


    ## from apscheduler docs
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_scraper, 'interval', minutes=5)
    scheduler.start()
    print('Press Ctrl+c to exit')

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()

