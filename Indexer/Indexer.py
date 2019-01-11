from pymongo import MongoClient
import Tokenizer as T

client = MongoClient('localhost', 27017)
db = client.Crawler
page_collection = db.page_info
index_collection = db.index_info

def index(doc, id):
  try:
    tokens = sorted(set(T.tokenize_and_stem(doc)))
    for token in tokens:
      term = index_collection.find_one({"term": token}, {"term":1, "postings_list":1})
      if term is None:
        index_collection.insert_one({"term": token, "frequency": 1, "postings_list": [id]})
      elif id not in term["postings_list"]:
        index_collection.update_one({"term": token}, 
                                    {"$addToSet": {"postings_list": id}, 
                                    "$inc": {"frequency": 1}})
  except TypeError:
    pass


def main():
  cursor = page_collection.find({}, {"text":1, "_id":1})
  for idx, doc in enumerate(cursor): 
    index(doc["text"], doc["_id"])

if __name__ == "__main__":
  main()