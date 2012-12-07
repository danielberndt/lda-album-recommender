from datetime import datetime
from pymongo import Connection, ASCENDING, DESCENDING
try:
  from bson.binary import Binary
except ImportError, e:
  from pymongo.binary import Binary

from StringIO import StringIO
from gzip import GzipFile

connection = Connection()
db = connection.lastfm_lda
db.cached_request.ensure_index("options")
db.user_top_albums.ensure_index([("period", ASCENDING), ("name", ASCENDING)])
db.albums.ensure_index([("topics.topic", ASCENDING), ("topics.count", DESCENDING)])
db.albums.ensure_index([("artist", ASCENDING),("name", ASCENDING)])

def save_user_top_albums(username, album_list, period):
  collection = db.user_top_albums
  data = {
    "name":username,
    "last_modified":datetime.now(),
    "top_albums":album_list,
    "period":period
  }
  collection.update({"name":username, "period":period},data,upsert=True)
  
def get_user_top_albums(username, period):
  collection = db.user_top_albums
  result = collection.find_one({"name":username, "period":period})
  if not result: return []
  return result.get("top_albums",[])
  
def get_existing_user_names():
  return map(lambda user: user["name"], db.user_top_albums.find({},{"name":True}))
  
def find_topics(albums):
  return db.albums.find({"$or":albums}, {"artist":1, "name":1, "distribution":1})
  
def get_top_of_topic(topic_index, limit=50, offset=0):
  return db.albums.find({"topics.topic":topic_index},limit=limit, skip=offset)

def check_cache(options, refresh_after_days=7):
  result = db.cached_request.find_one({"options":options})
  if result and (datetime.now() - result["created"]).seconds/(3600*24.0) < refresh_after_days:
    f = GzipFile(fileobj=StringIO(result["cached_result"]), mode="rb")
    unzipped_result = f.read()
    f.close()
    return unzipped_result
  else:
    return None
  
def add_to_cache(options, result):
  gzipped = StringIO()
  f = GzipFile(fileobj=gzipped, mode="wb")
  f.write(result)
  f.close()
  data = {
            "options":options,
            "created":datetime.now(),
            "cached_result": Binary(gzipped.getvalue())
          }
  gzipped.close()
  db.cached_request.update({"options":options}, data, upsert=True)
