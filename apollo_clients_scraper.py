import random
import time
from bs4 import BeautifulSoup
import requests
from pprint import pprint
from pymongo import MongoClient
import os
import stem.process
import re
from selenium import webdriver
from amazoncaptcha import AmazonCaptcha
from io import BytesIO
from debug import Listing
import pandas

from data_scraper_selenium import get_selenium_settings
from data_scraper_selenium import init_tor_proxies
from data_scraper_selenium import get_listing_data

max_attempts = 5

MONGODB_URI = os.environ['MONGODB_URI']
client = MongoClient(MONGODB_URI)  # mongo db connection

SOCKS_PORT = 9150  # chosen port for proxy
init_tor_proxies(SOCKS_PORT)  # use tor to set up proxy

selenium_settings = get_selenium_settings(SOCKS_PORT)  # generate appropriate selenium browser settings


df = pandas.read_csv('BusinessReport-10-21-22.csv', index_col='(Child) ASIN')
# with open('BusinessReport-10-21-22.csv') as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=',')
#     line_count = 0
#     for row in csv_reader:
#         if line_count == 0:
#             print(f'Column names are {", ".join(row)}')
#             line_count += 1
#         else:
#             print(f'\t{row[0]} works in the {row[1]} department, and was born in {row[2]}.')
#             line_count += 1
#     print(f'Processed {line_count} lines.')
print(df.head(30))

amazon_base_url = 'https://www.amazon.com'

links_checks_scraped = []
for ind in df.index:
    link = amazon_base_url + "/dp/" + ind
    links_checks_scraped.append([link, 0, False])

print(links_checks_scraped)


def unscraped_and_less_than_5_attempts(link_checks_scraped):
    if link_checks_scraped[1] < max_attempts and link_checks_scraped[2] is False:
        return True
    else:
        return False

def unscraped_and_more_than_4_attempts(link_checks_scraped):
    if link_checks_scraped[1] > (max_attempts - 1) and link_checks_scraped[2] is False:
        return True
    else:
        return False


links_to_check = list(filter(unscraped_and_less_than_5_attempts, links_checks_scraped))

while len(links_to_check) > 0:
    scraping_links = []

    for link in links_to_check:
        if len(scraping_links) < 4:
            scraping_links.append([link[0], "unknown", "unknown"])
        if len(scraping_links) > 3 or len(scraping_links) == len(links_to_check):
            try:
                listing_data = get_listing_data(scraping_links, selenium_settings[0], selenium_settings[1])
            except:
                time.sleep(10)
                listing_data = get_listing_data(scraping_links, selenium_settings[0], selenium_settings[1])

            for response in listing_data:  # running parser on each successful response and uploading to mongo
                try:
                    db = client.Apollo_Products2  # database with collections of department listing data
                    soup = BeautifulSoup(response[0])
                    Listing(soup, response[1], response[2], response[3]).mongodb_upload(db)
                    for row in links_checks_scraped:
                        if response[1] == row[0]:
                            row[2] = True
                            print("amended to true")
                except:
                    for row in links_checks_scraped:
                        if response[1] == row[0]:
                            row[1] = row[1] + 1
                            print("added to check count")
            scraping_links = []

    links_to_check = list(filter(unscraped_and_less_than_5_attempts, links_checks_scraped))
    pprint(links_checks_scraped)
    pprint(links_to_check)

checked_5_times = list(filter(unscraped_and_more_than_4_attempts, links_checks_scraped))

for over_attempted_link in checked_5_times:
    mongo_upload = {"url": over_attempted_link[0]}
    db["parsing_errors"].insert_one(mongo_upload)