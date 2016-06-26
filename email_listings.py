import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from secrets import key, sandbox, recipient
from jinja2 import Environment, FileSystemLoader
import os


## need to separate this out..
## should have something to intermittently scrape the first listing
## to see if it has changed...

## then for new listings somehow trigger something...

def get_search_results(url):
    """
    return bs4 object of search results
    """
    r = requests.get(url)
    assert r.status_code == 200, 'status code not 200'
    html_doc = r.text
    return BeautifulSoup(html_doc, 'html.parser')


def get_listing_details(listing):
    """
    return details of each listing from search results html document
    """
    href_link = list(set(filter(None.__ne__, [tag.get('href')
                                            for tag in listing.findAll('a')])))
    link = 'http://www.wg-gesucht.de/en/' + href_link[0]
    cost = (listing.find('td', class_='ang_spalte_miete row_click').
                find('span').contents[1].contents[0].replace(' ',''))
    size = (listing.find('td', class_='ang_spalte_groesse row_click').
                find('span').contents[0].
                replace(' ','').replace('\n', ''))
    stadt = (listing.find('td', class_='ang_spalte_stadt row_click').
                find('span').contents[0].
                replace(' ','').replace('\n', ''))
    date_available = (listing.find('td', class_='ang_spalte_freiab row_click').
                    find('span').contents[0])

    return {'link': link,'cost': cost,'size': size,
            'stadt': stadt,'date_available': date_available}


def get_latest_listing_details():
    """
    return formated listings of wg search results
    """
    url = 'http://www.wg-gesucht.de/en/1-zimmer-wohnungen-in-Berlin.8.1.0.0.html?filter=fb0bdd36e1f2253e7bb343bba784e123c46b8103025e2e0a3a'
    soup = get_search_results(url)
    search_results = [get_listing_details(prop)
                            for prop in soup.findAll('tr',
                                class_=re.compile('listenansicht0|listenansicht1'))]
    print('listings have been scraped')
    return pd.DataFrame(search_results)


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


if __name__ == '__main__':
    listings = get_latest_listing_details()
    html_doc = create_html_doc(listings.to_html())
    send_results_mail(html_doc)
    #print(html_doc)
