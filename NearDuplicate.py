from __future__ import division
import os
import re
import random
import time
import bisect
import heapq
import hashlib
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.Crawler
page_collection = db.page_info


def process_content(content):
    #split to get all words

    unprocessed = re.sub(r'[^\w ]', "", content)
    processed = unprocessed.split(" ")
    processed = list(filter(None, processed))
    return processed

#Generates ONE hash, no min hash yet.
def generate_shingles(words):
    #create set to hold hashed shingles
    shingles = set()

    #Create shingle, hash it, add to set of shingles.    
    for index in range(0, len(words)-2):

        shingle = words[index] + " " + words[index+1] + " " + words[index+2]
        shingles.add(shingle)

    return shingles

def hash_shingles(shingles):
    return list(map(lambda x: hashlib.sha1(x.encode()).hexdigest(), shingles))

def min_shingles(times, shingles):
    minimum_shingles = []
    temp_shingles = shingles
    try:
        for i in range(times):
            hashed_shingles = hash_shingles(temp_shingles)
            minimum_shingles.append(min(hashed_shingles))
            temp_shingles = hashed_shingles
        return minimum_shingles
    except ValueError:
        return minimum_shingles
    
def is_near_duplicate(set1, set2):
    if (len(set1.intersection(set2)) / len(set1.union(set2))) >= 0.9:
        return True
    else:
        return False
        
def process_text(page):
    processed = process_content(page)
    shingles = generate_shingles(processed)
    minimum_shingles = min_shingles(10, shingles)

    return minimum_shingles

def text_seen_before(text):
    min_hash = process_text(text)
    collection = page_collection.distinct('min_hashes')
    if len(collection) > 0:
        for hashes in collection:
            if is_near_duplicate(set(min_hash), set(hashes)):
                return True, min_hash
    return False, min_hash    

def check_and_save_page(url, text):
    seen, min_hash = text_seen_before(text)
    if not seen:
        page_collection.insert_one({"url":url, "min_hash":min_hash, "text":text})


def check_duplicate_urls(urls):
    checked = []
    db_urls = page_collection.distinct('url')
    if len(db_urls) > 0:
        for url in urls:
            if url in db_urls:
                continue
            checked.append(url)
        return checked
    else:
        return urls