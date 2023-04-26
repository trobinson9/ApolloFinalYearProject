import random
import time
import requests
from pymongo import MongoClient
import os
from io import BytesIO
from PIL import UnidentifiedImageError

from data_scraper_selenium import init_tor_proxies

proxies = {
    "http": 'socks5://127.0.0.1:9150',
    "https": 'socks5://127.0.0.1:9150'
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:53.0) Gecko/20100101 Firefox/53.0',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; Trident/5.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MDDCJS)',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)']


def scrape_img_data(mongo_string, proxy):
    client = MongoClient(mongo_string)  # mongo db connection
    db = client.Amazon_Products
    colls = db.list_collection_names()
    for coll in colls:
        if coll != 'parsing_errors' and coll != "unscrapable" and coll != "failed_responses":
            print(coll)
            coll = db[coll]
            # docs = coll.find({"Landing page data.Images.Image_Bin_Data": {"$exists": False},
            #                   "Landing page data.Best seller rank.Sub-department rank": {"$ne": "null"}})
            # docs = coll.aggregate([
            #     { "$match": {'$and': [
            #     {"Landing page data.Images.Image_Bin_Data": {"$exists": False}},
            #     {"Landing page data.Best seller rank.Sub-department rank": {"$ne": "null"}}
            # ]}}])
            docs = coll.aggregate([
                {
                    '$match': {
                        'Landing page data.Images.Image_Bin_Data': {
                            '$exists': False
                        }
                    }
                }
            #     , {
            #         '$match': {
            #             'Landing page data.Best seller rank.Sub-department rank': {
            #                 '$ne': None
            #             }
            #         }
            #     }
            ])
            # docs = docs.find({ "Landing page data.Best seller rank.Sub-department rank": {'$exists': False} })
            for doc in docs:
                print(doc["ASIN"])
                image_urls = doc["Landing page data"]["Images"]["Urls"]
                data_dict = {}
                for url in image_urls:
                    print(url)
                    try:
                        img_content = requests.get(url, proxies=proxies).content
                        img_bytesio = BytesIO(img_content)
                        value = img_bytesio.getvalue()
                        upload_str = str(value).split("b'")[1]
                        data_dict[url] = upload_str
                    except requests.exceptions.ConnectionError:
                        print(requests.exceptions.ConnectionError)
                        init_tor_proxies(SOCKS_PORT)
                        img_content = requests.get(url, proxies=proxies).content
                        img_bytesio = BytesIO(img_content)
                        value = img_bytesio.getvalue()
                        upload_str = str(value).split("b'")[1]
                        data_dict[url] = upload_str
                    except UnidentifiedImageError:
                        print('Image reading error, uploading error and response content')
                        data_dict[url] = {'content_not_bytes': str(img_content)}
                        print(img_content)
                    except requests.exceptions.MissingSchema:
                        print("invalid url:")
                        print(url)
                        data_dict[url] = {'invalid url': url}

                id = doc["_id"]
                coll.update_one({"_id": id}, {"$set": {"Landing page data.Images.Image_Bin_Data": data_dict}})
                time.sleep(random.uniform(4, 21))


if __name__ == '__main__':
    MONGODB_URI = "mongodb://localhost:27017"

    SOCKS_PORT = 9150  # chosen port for proxy
    init_tor_proxies(SOCKS_PORT)  # use tor to set up proxy

    while True:
        try:
            scrape_img_data(MONGODB_URI, proxies)
        except Exception as e:
            print('general exception reached')
            print(e)
