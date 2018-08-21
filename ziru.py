import re
from lxml import etree
import time
import json
import pymongo
import requests

DB = "ziru"
base_url = "http://sh.ziroom.com"
headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"}

def get_disctricts():
    url = base_url + "/z/nl/z1.html"
    r = requests.get(url, headers=headers, verify=False)
    content = r.content.decode("utf-8")
    root = etree.HTML(content)
    distr_nodes = root.xpath('.//dl[contains(@class, "zIndex6")]/dd/ul/li/span[@class="tag"]/a')
    for node in distr_nodes:
        url = node.attrib["href"]
        print(url)

if __name__ == "__main__":
    get_disctricts()
