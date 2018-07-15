import re
from lxml import etree
import time
import json
import pymongo
import requests

def get_disctricts():
    url = "https://sh.ke.com/ershoufang/"
    r = requests.get(url, verify=False)
    content = r.content.decode("utf-8")
    root = etree.HTML(content)
    distr_nodes = root.xpath('.//div[@class="m-filter"]//div[@data-role="ershoufang"]/div/a')
    result = []
    for node in distr_nodes:
        rel_url = node.attrib["href"]
        distr_url = "https://sh.ke.com" + rel_url
        distr_name = node.text
        result.append([distr_name, distr_url])
    return result

def get_sub_districts():
    districts = get_disctricts()
    result = []
    client = pymongo.MongoClient()
    db = client.beike
    for item in districts:
        distr_name = item[0]
        distr_url = item[1]
        r = requests.get(distr_url, verify=False)
        content = r.content.decode("utf-8")
        root = etree.HTML(content)
        subdistr_nodes = root.xpath('.//div[@class="m-filter"]//div[@data-role="ershoufang"]/div')[1].xpath('./a')
        for node in subdistr_nodes:
            sub_distr_name = node.text
            sub_distr_url = "https://sh.ke.com" + node.attrib["href"]
            db.sub_districts.insert_one({
                "district": distr_name,
                "sub_district": sub_distr_name,
                "url": sub_distr_url,
            })

def get_item_num(entry_url):
    r = requests.get(entry_url, verify=False)
    content = r.content.decode("utf-8")
    root = etree.HTML(content)
    num_nodes = root.xpath('.//div[@class="content"]')
    print(len(num_nodes))
    if len(num_nodes) == 0:
        raise Exception("no total number for {}".format(entry_url))
    num_str = num_nodes[0].text.strip()
    print(int(num_str))

def get_houses_by_sub_district(entry_url):
    url_patt = "https://sh.ke.com/ershoufang/pg{}/"
    url_patt = "{}/pg{}/".format(entry_url)

    i = 1
    client = pymongo.MongoClient()
    db = client.beike
    while True:
        url = url_patt.format(i)
        print(url)
        r = requests.get(url, verify=False)
        content = r.content.decode("utf-8")
        root = etree.HTML(content)
        ul_node = root.find('.//ul[@class="sellListContent"]')
        div_info = ul_node.xpath('.//div[contains(@class, "info")]')
        for div_node in div_info:
            title_nodes = div_node.xpath('./div[@class="title"]/a[contains(@class, "maidian-detail")]')
            if len(title_nodes) == 0:
                print("title not found")
                continue
            title_node = title_nodes[0]
            title = title_node.text
            url = title_node.attrib["href"]

            xiaoqu_nodes = div_node.xpath('./div[@class="address"]/div[@class="houseInfo"]/a')
            xiaoqu_name = ""
            house_info = ""
            if len(xiaoqu_nodes) > 0:
                xiaoqu_name = xiaoqu_nodes[0].text
                house_info = xiaoqu_nodes[0].tail

            pos_nodes = div_node.xpath('./div[@class="flood"]/div[@class="positionInfo"]/span')
            building_info = ""
            if len(pos_nodes) > 0:
                building_info = pos_nodes[0].tail
                matched = re.search(r'(.*)\s+-\s+$', building_info)
                if matched:
                    building_info = matched.group(1)

            area_nodes = div_node.xpath('./div[@class="flood"]/div[@class="positionInfo"]/a')
            area = ""
            if len(area_nodes) > 0:
                area_node = area_nodes[0]
                area = area_node.text

            follow_nodes = div_node.xpath('./div[@class="followInfo"]/span')
            follow_info = ""
            if len(follow_nodes) > 0:
                follow_node = follow_nodes[0]
                follow_info = follow_node.tail

            subway_nodes = div_node.xpath('./div[@class="tag"]/span[@class="subway"]')
            subway_info = ""
            if len(subway_nodes) > 0:
                subway_node = subway_nodes[0]
                subway_info = subway_node.text

            tax_nodes = div_node.xpath('./div[@class="tag"]/span[@class="taxfree"]')
            tax_info = ""
            if len(tax_nodes) > 0:
                tax_node = tax_nodes[0]
                tax_info = tax_node.text

            price_nodes = div_node.xpath('./div[@class="priceInfo"]/div[@class="totalPrice"]/span')
            price_num = 0
            price_unit = ""
            if len(price_nodes) > 0:
                price_node = price_nodes[0]
                price_num = price_node.text
                price_unit = price_node.tail

            up_nodes = div_node.xpath('./div[@class="priceInfo"]/div[@class="unitPrice"]')
            unit_price = 0
            if len(up_nodes) > 0:
                up_node = up_nodes[0]
                unit_price = up_node.attrib["data-price"]

            item = {
                "title": title,
                "url": url,
                "house_info": house_info,
                "xiaoqu_name": xiaoqu_name,
                "building_info": building_info,
                "area": area,
                "follow_info": follow_info,
                "subway_info": subway_info,
                "tax_info": tax_info,
                "price_num": price_num,
                "price_unit": price_unit,
                "unit_price": unit_price,
            }
            db.house.insert_one(item)
        i += 1

def get_all_houses():
    client = pymongo.MongoClient()
    db = client.beike
    sub_distr_rows = db.sub_districts.find()
    for sub_distr in sub_distr_rows:
        entry_url = sub_distr["url"]
        get_houses_by_sub_district(entry_url)

if __name__ == "__main__":
    get_item_num("https://sh.ke.com/ershoufang/beicai/")
