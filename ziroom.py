import re
from lxml import etree
import time
import json
import pymongo
import requests

DB = "shziroom"
base_url = "http://sh.ziroom.com"
headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"}

def fix_url(url):
    if re.match(r'//', url):
        url = 'http:{}'.format(url)
    return url

def get_sub_districts(node):
    sub_nodes = node.xpath('.//div[@class="con"]/span/a')
    result = []
    for sub_node in sub_nodes:
        sub_district = sub_node.text
        url = sub_node.attrib["href"]
        url = fix_url(url)
        if sub_district == "全部":
            continue

        result.append({"sub_district": sub_district, "url": url})
    return result

def get_disctricts():
    url = base_url + "/z/nl/z1.html"
    r = requests.get(url, headers=headers, verify=False)
    content = r.content.decode("utf-8")
    root = etree.HTML(content)
    distr_nodes = root.xpath('.//dl[contains(@class, "zIndex6")]/dd/ul/li')
    client = pymongo.MongoClient()
    db = client[DB]
    for distr_node in distr_nodes:
        nodes = distr_node.xpath('.//span[@class="tag"]/a')
        if len(nodes) == 0:
            continue
        node = nodes[0]
        district = node.text
        url = node.attrib["href"]
        url = fix_url(url)
        sub_distrs = get_sub_districts(distr_node)
        for sub_distr in sub_distrs:
            item = {"district": district,
                "sub_district": sub_distr["sub_district"],
                "url": sub_distr["url"]
            }
            db.sub_districts.insert_one(item)

def get_price(price_node):
    num_nodes = price_node.xpath('./span[@class="num"]')
    print(price_node.text)
    offset_map = {
        1: 6,
        30: 5,
        6: 3,
        90: 2,
        120: 1,
        3: 4,
        7: 8,
        210: 9,
        5: 0,
        270: 7,
    }

    price = 0
    for num_node in num_nodes:
        style = num_node.attrib["style"]
        matched = re.match(r'background-position:-(\d+)px', style)
        if not matched:
            raise Exception("error getting price")
        offset = matched.group(1)
        num = offset_map[offset]
        price = price*10 + num
    return price

def get_houses_by_sub_district(sub_distr_id, entry_url):
    url_patt = entry_url + "?p={}"

    i = 1
    client = pymongo.MongoClient()
    db = client[DB]
    while True:
        url = url_patt.format(i)
        url = "http://sh.ziroom.com/z/nl/z1-d310112.html"
        r = requests.get(url, headers=headers, verify=False)
        content = r.content.decode("utf-8")
        print(content)
        return
        root = etree.HTML(content)
        house_nodes = root.xpath('.//ul[@id="houseList"]/li[@class="clearfix"]')
        if len(house_nodes) == 0:
            break
        for house_node in house_nodes:
            title_nodes = house_node.xpath('.//div[@class="txt"]/h3/a')
            if len(title_nodes) == 0:
                continue
            title = title_nodes[0].text
            area = 0
            floor_info = ""
            room_type = ""
            detail_nodes = house_node.xpath('.//div[@class="detail"]/p/span')
            for node in detail_nodes:
                print(etree.tostring(node))
                text = node.text
                matched = re.search(r'(\d+) ㎡', text)
                if matched:
                    area = matched.group(1)
                elif re.search(r'室', text):
                    room_type = text
            price_nodes = house_node.xpath('.//div[@class="priceDetail"]/p[@class="price"]')
            if len(price_nodes) == 0:
                continue
            price = get_price(price_nodes[0])
            print(price)
        i += 1

def get_all_houses():
    client = pymongo.MongoClient()
    db = client[DB]
    sub_distr_rows = db.sub_districts.find()
    for sub_distr in sub_distr_rows:
        entry_url = sub_distr["url"]
        sub_distr_id = sub_distr["_id"]
        distr_name = sub_distr["district"]
        sub_distr_name = sub_distr["sub_district"]
        print(distr_name, sub_distr_name)
        get_houses_by_sub_district(sub_distr_id, entry_url)
        break

if __name__ == "__main__":
    get_all_houses()
