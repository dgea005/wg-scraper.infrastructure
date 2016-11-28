"""
Class to scrape
"""

import requests
import re
import pandas as pd
from bs4 import BeautifulSoup


class Scraper:
    def __init__(self, url):
        self.url = url


    def get_search_results(self):
        """make request to get html and soup"""
        response = requests.get(self.url)
        assert response.status_code == 200, 'status code not 200'
        html_doc = response.text
        return BeautifulSoup(html_doc, 'html.parser')

    def get_latest_listing_details(self):
        """get listings out of one url, also process dtypes, etc a bit"""
        soup = self.get_search_results()

        ## depending on link we are searching in the 1zimmer or flat part of wg-gesucht
        if '8.2.0.0' in self.url:
            flat_type = 'flat'
        elif '8.1.0.0' in self.url:
            flat_type = 'studio'
        else:
            flat_type = ''

        reg_str = 'listenansicht0|listenansicht1'
        listing_tags = soup.findAll('tr', class_=re.compile(reg_str))
        search_results = pd.DataFrame(get_listing_details(prop) for prop in listing_tags)

        ## search result clean up
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
        


