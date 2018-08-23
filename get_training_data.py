import hashlib
import glob
import sys
import time
import os
import json
import re
from lxml import etree
import requests
import numpy as np
import cv2

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"}
FOLDER = "digits"

def fix_url(url):
    if re.match(r'//', url):
        url = 'http:{}'.format(url)
    return url

def save_pic(url):
    print(url)
    r = requests.get(url=url, headers=headers, verify=False)
    filename = url.split("/")[-1]
    filepath = os.path.join(FOLDER, filename)
    with open(filepath, "wb") as f:
        f.write(r.content)

def get_pic_url(url):
    r = requests.get(url, headers=headers, verify=False)
    content = r.content.decode("utf-8")
    matched = re.search(r'var ROOM_PRICE = (.*);', content)
    price_json = matched.group(1)
    data = json.loads(price_json)
    image_url = data["image"]
    if re.match(r'//', image_url):
        image_url = fix_url(image_url)
    return image_url

def label_pic(filepath):
    im = cv2.imread(filepath)
    imgray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
    ret, thresh = cv2.threshold(imgray, 127, 255, 0)
    im2, contours, hierarchy = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for contour in contours:
        [x, y, w, h] = cv2.boundingRect(contour)
        roi = imgray[y:y+h, x:x+w]
        roismall = cv2.resize(roi, (30, 30))
        cv2.imshow("small", roismall)
        key = cv2.waitKey(0)
        if key == 27:
            sys.exit()

        digit = int(chr(key))
        outname = "{}.png".format(digit)
        outpath = os.path.join("label", outname)
        cv2.imwrite(outpath, roismall)

def label_data():
    pics = os.listdir(FOLDER)
    for pic in pics:
        filename = pic.split(".")[0]
        patt = "label/{}_*".format(filename)
        saved_digits = glob.glob(patt)

        if len(saved_digits) == 10:
            print("{} done".format(patt))
            continue
        filepath = os.path.join(FOLDER, pic)
        label_pic(filepath)

def load_data():
    pics = os.listdir("label")
    samples = np.empty((0, 900))
    labels = []
    for pic in pics:
        filepath = os.path.join("label", pic)
        label = int(pic.split(".")[0].split("_")[-1])
        labels.append(label)
        im = cv2.imread(filepath, cv2.IMREAD_GRAYSCALE)
        sample = im.reshape((1, 900)).astype(np.float32)
        samples = np.append(samples, sample, 0)
    labels = np.array(labels).reshape((-1, 1)).astype(np.float32)
    return [samples, labels]

def recog_num(im):
    [samples, labels] = load_data()
    samples = samples.astype(np.float32)
    lables = labels.astype(np.float32)
    model = cv2.ml.KNearest_create()
    print(samples.dtype, labels.dtype)
    model.train(samples, cv2.ml.ROW_SAMPLE, labels)
    imgray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
    ret, thresh = cv2.threshold(imgray, 127, 255, 0)
    im2, contours, hierarchy = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for contour in contours[::-1]:
        [x, y, w, h] = cv2.boundingRect(contour)
        roi = imgray[y:y+h, x:x+w]
        roismall = cv2.resize(roi, (30, 30))
        sample = roismall.reshape((1, 900)).astype(np.float32)
        ret, results, neighbours, distances = model.findNearest(sample, k = 1)
        print(int(results[0,0]))


if __name__ == "__main__":
    im = cv2.imread("e72ac241b410eac63a652dc1349521fds.png")
    recog_num(im)
