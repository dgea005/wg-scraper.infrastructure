"""
Class to scrape
"""

import re
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class indexScraper:
    """
    Scrape results from wg-gesucht for one search link
    """
    def __init__(self, url):
        self.url = url
        self.soup = None
        self.search_results = None

    def get_html(self):
        """make request to get html and soup"""
        response = requests.get(self.url)
        logger.info('response from: {url} is {code}'.format(url=self.url, code=response.status_code))
        assert response.status_code == 200, 'status code not 200'
        html_doc = response.text
        self.soup = BeautifulSoup(html_doc, 'html.parser')
        return self

    def get_listings_from_page(self):
        """get listings out of one url, also process dtypes, etc a bit"""

        # depending on link we are searching in the 1zimmer or flat part of wg-gesucht
        if '8.2.0.0' in self.url:
            flat_type = 'flat'
        elif '8.1.0.0' in self.url:
            flat_type = 'studio'
        else:
            flat_type = ''

        listing_identifier_rx = 'listenansicht0|listenansicht1'
        listing_tags = self.soup.findAll('tr', class_=re.compile(listing_identifier_rx))
        self.search_results = pd.DataFrame(self.get_listing_details(prop) for prop in listing_tags)

        ## search result clean up
        self.search_results = (self.search_results.
                               assign(cost=self.search_results['cost'].astype(int)).
                               assign(size=self.search_results['size'].astype(int)).
                               assign(free_from=
                                      pd.to_datetime(self.search_results.free_from, dayfirst=True)).
                               assign(free_to=
                                      pd.to_datetime(self.search_results.free_to, dayfirst=True)).
                               assign(length=lambda df:
                                      (df['free_to'] - df['free_from'])/pd.Timedelta(days=1)).
                               assign(scrape_time=
                                      pd.Timestamp('now').replace(second=0, microsecond=0)).
                               assign(flat_type=flat_type))
        return self.search_results

    @staticmethod
    def get_listing_details(listing):
        """get details of one listing from parsed html response"""
        href = list(set(filter(None.__ne__, [tag.get('href') for tag in listing.findAll('a')])))
        listing_details = dict()
        listing_details['link'] = 'http://www.wg-gesucht.de/en/' + href[0]
        listing_details['listing_id'] = listing_details['link'].split('.')[-2]
        listing_details['cost'] = (listing.find('td', class_='ang_spalte_miete').
                                   find('span').contents[1].contents[0].
                                   replace(' ', '').replace('\n', '').replace('€', ''))
        listing_details['size'] = (listing.find('td', class_='ang_spalte_groesse').
                                   find('span').contents[0].
                                   replace(' ', '').replace('\n', '').replace('m²', ''))
        listing_details['stadt'] = (listing.find('td', class_='ang_spalte_stadt').
                                    find('span').contents[0].
                                    replace(' ', '').replace('\n', ''))
        listing_details['free_from'] = (listing.find('td', class_='ang_spalte_freiab').
                                        find('span').contents[0])
        listing_details['free_to'] = listing.find('td', class_='ang_spalte_freibis').find('span')
        # handle listings that are free indefinitely
        if listing_details['free_to']:
            listing_details['free_to'] = listing_details['free_to'].contents[0]
        else:
            listing_details['free_to'] = None

        return listing_details


class listingScraper:
    """follow the links retrieved by indexScraper to get lister, address details"""

    def __init__(self, listing_url):
        self.listing_url = listing_url
        self.soup = None

    def get_listing_html(self):
        """request specified link and return html/soup"""
        response = requests.get(self.listing_url)
        logger.info('response from: {url} is {code}'.format(url=self.listing_url, code=response.status_code))
        assert response.status_code == 200, 'status code not 200'
        html_doc = response.text
        self.soup = BeautifulSoup(html_doc, 'html.parser')
        return self

    @staticmethod
    def parse_details(listing_url):
        """follow listing url and get more details"""
        listing = listingScraper(listing_url).get_listing_html()
        # --- address --- #
        address_div = listing.soup.find('div', class_='col-xs-12 col-sm-4')
        address_contents = address_div.find('a').contents
        address_pt1 = address_contents[0].replace('\r', '').replace('\n', '').replace('  ', '')
        address_pt2 = address_contents[2].replace('\r', '').replace('\n', '').replace('  ', '')
        # --- contact information --- #
        lister_details = listing.soup.findAll('div', class_='col-sm-12')
        member_since = (lister_details[3].find('div', class_='row col-sm-12').
                        contents[2].replace('\n', '').
                        replace('   ', '').replace('  ', ' '))
        member_name = (lister_details[3].find('div', class_='col-xs-12').
                       contents[1].replace('\n', '').
                       replace('  ', ''))
        return {'address_1': address_pt1,
                'address_2': address_pt2,
                'member_since': member_since,
                'member_name': member_name,
                'link': listing_url}
