import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import math
import re
from lxml import etree
import time
import json
import pymongo
import requests

DB = "hangzhou"
base_url = "https://hz.ke.com"

def get_disctricts():
    url = base_url + "/ershoufang/"
    r = requests.get(url, verify=False)
    content = r.content.decode("utf-8")
    root = etree.HTML(content)
    distr_nodes = root.xpath('.//div[@class="m-filter"]//div[@data-role="ershoufang"]/div/a')
    result = []
    for node in distr_nodes:
        rel_url = node.attrib["href"]
        distr_url = ""
        if re.match(r'https://', rel_url):
            distr_url = rel_url
        else:
            distr_url = base_url + rel_url
        distr_name = node.text
        result.append([distr_name, distr_url])
    return result

def get_sub_districts():
    districts = get_disctricts()
    result = []
    client = pymongo.MongoClient()
    db = client[DB]
    for item in districts:
        distr_name = item[0]
        distr_url = item[1]
        r = requests.get(distr_url, verify=False)
        content = r.content.decode("utf-8")
        root = etree.HTML(content)
        subdistr_nodes = root.xpath('.//div[@class="m-filter"]//div[@data-role="ershoufang"]/div')[1].xpath('./a')
        for node in subdistr_nodes:
            sub_distr_name = node.text
            sub_distr_url = base_url + node.attrib["href"]
            db.sub_districts.insert_one({
                "district": distr_name,
                "sub_district": sub_distr_name,
                "url": sub_distr_url,
            })

def get_item_num(entry_url):
    r = requests.get(entry_url, verify=False)
    content = r.content.decode("utf-8")
    root = etree.HTML(content)
    num_nodes = root.xpath('.//div[@class="content "]//h2[contains(@class, "total")]/span')
    if len(num_nodes) == 0:
        raise Exception("no total number for {}".format(entry_url))
    num_str = num_nodes[0].text.strip()
    return int(num_str)

def get_houses_by_sub_district(sub_distr_id, entry_url):
    url_patt = entry_url + "pg{}/"

    total_num = get_item_num(entry_url)
    last_page = math.ceil(total_num/30)
    i = 1
    client = pymongo.MongoClient()
    db = client[DB]
    for i in range(1, last_page+1, 1):
        url = url_patt.format(i)
        r = requests.get(url, verify=False)
        content = r.content.decode("utf-8")
        root = etree.HTML(content)
        content_node = root.find('.//div[@class="content "]')
        if content_node is None:
            print(url)
            r = requests.get(url, verify=False)
            content = r.content.decode("utf-8")
            root = etree.HTML(content)
            ul_node = root.find('.//div[@class="content "]')

        ul_node = root.find('.//ul[@class="sellListContent"]')
        div_info = ul_node.xpath('.//div[contains(@class, "info")]')
        for div_node in div_info:
            title_nodes = div_node.xpath('./div[@class="title"]/a[contains(@class, "maidian-detail")]')
            if len(title_nodes) == 0:
                print("title not found")
                continue
            title_node = title_nodes[0]
            title = title_node.text
            maidian = title_node.attrib["data-maidian"]
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
                "item_id": maidian,
                "sub_distr_id": sub_distr_id,
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
    db = client[DB]
    sub_distr_rows = db.sub_districts.find()
    start = 1
    for sub_distr in sub_distr_rows:
        entry_url = sub_distr["url"]
        sub_distr_id = sub_distr["_id"]
        distr_name = sub_distr["district"]
        sub_distr_name = sub_distr["sub_district"]
        print(distr_name, sub_distr_name)
        #if distr_name == "福田区" and sub_distr_name == "银湖":
        #    start = 1
        if start == 1:
            get_houses_by_sub_district(sub_distr_id, entry_url)

def parse_house_info(house_info):
    items = house_info.split("|")
    house_type = "apartment"
    matched = re.search(r'别墅', items[1])
    info_items = items[1:]
    if matched:
        info_items = items[2:]
        house_type = "house"

    if len(info_items) < 4:
        print(house_info)
        return {"house_type": "",
            "shi_num": -1,
            "ting_num": -1,
            "size": -1,
            "has_lift": -1,
            "direction": "",
            "decoration": "",
        }
    room_info = info_items[0]
    size_info = info_items[1]
    direc_info = info_items[2]
    decor_info = info_items[3]
    lift_info = ""
    if len(info_items) >= 5:
        lift_info = info_items[4]
    matched = re.search(r'(\d+)室(\d+)厅', room_info)
    shi_num = 0
    ting_num = 0
    if matched:
        shi_num = int(matched.group(1))
        ting_num = int(matched.group(2))

    matched = re.search(r'([.0-9]+)平米', size_info)
    size = 0.0
    if matched:
        size = float(matched.group(1))

    has_lift = None
    if re.search(r'有电梯', lift_info):
        has_lift = True
    elif re.search(r'无电梯', lift_info):
        has_lift = False
    result = {"house_type": house_type,
        "shi_num": shi_num,
        "ting_num": ting_num,
        "size": size,
        "has_lift": has_lift,
        "direction": direc_info,
        "decoration": decor_info,
    }
    return result

def update_house_info():
    client = pymongo.MongoClient()
    db = client[DB]
    houses = db.house.find()
    for house in houses:
        object_id = house["_id"]
        price_num = float(house["price_num"])
        unit_price = float(house["unit_price"])
        building_info = house["building_info"]
        matched = re.search(r'(\d+)年', building_info)
        build_year = 0
        if matched:
            build_year = int(matched.group(1))
        db.house.update({"_id": house["_id"]}, {"$set": {"price_num": price_num, "unit_price": unit_price, "build_year": build_year}})
        info = parse_house_info(house["house_info"])
        db.house.update({"_id": house["_id"]}, {"$set": info})

def stats():
    client = pymongo.MongoClient()
    db = client[DB]

    print("=========== average house age =============")
    houses = db.house.aggregate([
        {"$match": {"build_year": {"$gt": 0}}},
    ])
    total = 0
    count = 0
    for house in houses:
        total += house["build_year"]
        count += 1
    avg_build_year = total/count
    avg_age = 2018 - avg_build_year
    print(avg_age)
    import sys
    sys.exit(1)

    print("=========== most expensive xiaoqu in each district =============")
    districts = db.sub_districts.aggregate([
        {"$group": {"_id": "$district", "district_name": {"$first": "$district"}, "sub_districts": {"$push": "$_id"}}},
    ])
    for district in districts:
        district_name = district["district_name"]
        sub_districts = district["sub_districts"]
        xiaoqus = db.house.aggregate([
            {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_districts"}},
            {"$unwind": "$sub_districts"},
            {"$match": {"sub_districts.district": district_name}},
            {"$group": {"_id": "$xiaoqu_name", "district_name": {"$first": "$sub_districts.district"}, "xiaoqu_name": {"$first": "$xiaoqu_name"}, "avg_price": {"$avg": "$unit_price"}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gte": 3}}},
            {"$sort": {"avg_price": -1}},
            {"$limit": 1},
        ])
        for xiaoqu in xiaoqus:
            print(xiaoqu["district_name"], xiaoqu["xiaoqu_name"], xiaoqu["avg_price"], xiaoqu["count"])

    print("=========== cheapest xiaoqu in each district =============")
    districts = db.sub_districts.aggregate([
        {"$group": {"_id": "$district", "district_name": {"$first": "$district"}, "sub_districts": {"$push": "$_id"}}},
    ])
    for district in districts:
        district_name = district["district_name"]
        sub_districts = district["sub_districts"]
        xiaoqus = db.house.aggregate([
            {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_districts"}},
            {"$unwind": "$sub_districts"},
            {"$match": {"sub_districts.district": district_name}},
            {"$group": {"_id": "$xiaoqu_name", "district_name": {"$first": "$sub_districts.district"}, "xiaoqu_name": {"$first": "$xiaoqu_name"}, "avg_price": {"$avg": "$unit_price"}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gte": 3}}},
            {"$sort": {"avg_price": 1}},
            {"$limit": 1},
        ])
        for xiaoqu in xiaoqus:
            print(xiaoqu["district_name"], xiaoqu["xiaoqu_name"], xiaoqu["avg_price"], xiaoqu["count"])

    print("=========== average unit price =============")
    houses = db.house.find()
    total = 0
    count = 0
    for house in houses:
        total += house["unit_price"]
        count += 1
    avg_price = total/count
    print(avg_price)

    print("=========== average house price =============")
    houses = db.house.find()
    total = 0
    count = 0
    for house in houses:
        total += house["price_num"]
        count += 1
    avg_price = total/count
    print(avg_price)

    print("=========== apartment/house =============")
    houses = db.house.aggregate([
        {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_districts"}},
        {"$unwind": "$sub_districts"},
        {"$match": {"sub_districts": {"$ne": []}}},
        {"$group": {"_id": "$house_type", "house_type": {"$first": "$house_type"}, "count": {"$sum": 1}}},
    ])
    for house in houses:
        print(house["house_type"], house["count"])

    print("=========== biggest houses =============")
    houses = db.house.aggregate([
        {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_districts"}},
        {"$unwind": "$sub_districts"},
        {"$match": {"sub_districts": {"$ne": []}}},
        {"$sort": {"size": -1}},
        {"$limit": 10}
    ])
    for house in houses:
        print(house["title"], house["size"], house["xiaoqu_name"])

    print("=========== most number of houses district name =============")
    houses = db.house.aggregate([
        {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_districts"}},
        {"$unwind": "$sub_districts"},
        {"$match": {"sub_districts": {"$ne": []}}},
        {"$group": {"_id": "$sub_districts.district", "district_name": {"$first": "$sub_districts.district"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ])
    for house in houses:
        print(house["district_name"], house["count"])

    print("=========== most number of houses xiaoqu name =============")
    houses = db.house.aggregate([
        {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_districts"}},
        {"$unwind": "$sub_districts"},
        {"$match": {"sub_districts": {"$ne": []}}},
        {"$group": {"_id": "$xiaoqu_name", "xiaoqu_name": {"$first": "$xiaoqu_name"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    for house in houses:
        print(house["xiaoqu_name"], house["count"])

    print("=========== most expensive xiaoqu name =============")
    houses = db.house.aggregate([
        {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_districts"}},
        {"$unwind": "$sub_districts"},
        {"$match": {"sub_districts": {"$ne": []}}},
        {"$group": {"_id": "$xiaoqu_name", "district_name": {"$first": "$sub_districts.district"}, "sub_district_name": {"$first": "$sub_districts.sub_district"}, "xiaoqu_name": {"$first": "$xiaoqu_name"}, "avg_unit_price": {"$avg": "$unit_price"}}},
        {"$sort": {"avg_unit_price": -1}},
        {"$limit": 10}
    ])
    for house in houses:
        print(house["district_name"], house["sub_district_name"], house["xiaoqu_name"], house["avg_unit_price"])

    print("=========== most expensive sub district =============")
    houses = db.house.aggregate([
        {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_districts"}},
        {"$unwind": "$sub_districts"},
        {"$match": {"sub_districts": {"$ne": []}}},
        {"$group": {"_id": "$sub_districts.sub_district", "district_name": {"$first": "$sub_districts.district"}, "sub_district_name": {"$first": "$sub_districts.sub_district"}, "avg_unit_price": {"$avg": "$unit_price"}}},
        {"$sort": {"avg_unit_price": -1}},
    ])
    for house in houses:
        print(house["district_name"], house["sub_district_name"], house["avg_unit_price"])

    print("=========== most expensive district =============")
    houses = db.house.aggregate([
        {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_districts"}},
        {"$unwind": "$sub_districts"},
        {"$match": {"sub_districts": {"$ne": []}}},
        {"$group": {"_id": "$sub_districts.district", "district_name": {"$first": "$sub_districts.district"}, "avg_unit_price": {"$avg": "$unit_price"}}},
        {"$sort": {"avg_unit_price": -1}},
    ])
    for house in houses:
        print(house["district_name"], house["avg_unit_price"])

    print("=========== most expensive =============")
    houses = db.house.aggregate([
        {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_district"}},
        {"$unwind": "$sub_district"},
        {"$match": {"sub_districts": {"$ne": []}}},
        {"$sort": {"price_num": -1}},
        {"$limit": 10}
    ])
    for house in houses:
        print(house["title"], house["url"], house["xiaoqu_name"], house["price_num"], house["unit_price"])

    print("=========== most expensive unit price =============")
    houses = db.house.aggregate([
        {"$lookup": {"from": "sub_districts", "localField": "sub_distr_id", "foreignField": "_id", "as": "sub_district"}},
        {"$unwind": "$sub_district"},
        {"$match": {"sub_districts": {"$ne": []}}},
        {"$sort": {"unit_price": -1}},
        {"$limit": 10}
    ])
    for house in houses:
        print(house["title"], house["url"], house["xiaoqu_name"], house["price_num"], house["unit_price"])

if __name__ == "__main__":
    stats()
