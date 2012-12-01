import json, gevent, data
from urllib import urlencode

from gevent import monkey; monkey.patch_socket()
from urllib2 import Request, urlopen
from StringIO import StringIO

from gevent.queue import PriorityQueue
from gevent import Greenlet
from gzip import GzipFile
from httplib import BadStatusLine
import config

ROOTURL = "http://ws.audioscrobbler.com/2.0/"

_base_json_options = {
  "api_key": config.get("apikey"),
  "format": "json"
}

queue = PriorityQueue()

def call_json(method, callback, options={}, priority=100):
  call_options = _base_json_options.copy()
  call_options.update(options)
  call_options["method"]=method
  cached_result = data.check_cache(call_options)
  if cached_result: 
    return gevent.spawn(lambda result : callback(json.loads(result)), cached_result)
  url = "%s?%s" % (ROOTURL,urlencode(call_options))
  g = Greenlet(URLLoadListener(url, lambda result : callback(json.loads(result)), call_options).open)
  queue.put((priority, g))
  return g
  
def get_user_top_albums(user, callback, period="overall", limit=100):
  return call_json("user.gettopalbums", callback,{"user":user, "period": period, "limit": limit})
  
class URLLoadListener:
  
  num_connections = 0
  
  def __init__(self, url, callback, api_options):
    self.url = url
    self.callback = callback
    self.api_options = api_options
    self.retries = 0
    
  def open(self):
    request = Request(self.url)
    request.add_header('User-Agent','lastfm-lda recommender v.0.0.-1')
    request.add_header('Accept-encoding', 'gzip')
    while True:
      URLLoadListener.num_connections+=1
      response = None
      try:
        response = urlopen(request,timeout=10)
        if response.info().get('Content-Encoding') == 'gzip':
          f = GzipFile(fileobj=StringIO(response.read()))
          result = f.read()
          f.close()
        else:
          result = response.read()
        break
      except Exception, e:
        if self.retries>2: 
          if isinstance(e, BadStatusLine): raise Exception("last.fm server does not respond (%s)" % e)
          raise e
        self.retries+=1
        print self.url
        print "failed with", e
        print "retry #",self.retries
        print
      finally:
        if response: response.close()
        URLLoadListener.num_connections-=1
        
    data.add_to_cache(self.api_options, result)
    self.callback(result)

def _start():
  while True:
    (_, g) = queue.get()
    g.start()
    gevent.sleep(0.25)
    while URLLoadListener.num_connections>20: gevent.sleep(0.25)
    
gevent.spawn(_start)