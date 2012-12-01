import math, gevent
import lastfm_api, data

from crawler import PagedRequest
from gevent.queue import Queue

class UserProcess:
  
  processing = {}   # oh how stateful!
  
  @staticmethod
  def get_message_queue(name):
    q = Queue()
    up = UserProcess.processing.get(name)
    if up: 
      for msg in up.messages_so_far: q.put(msg)
      up.listeners.add(q)
    return q  
  
  @staticmethod
  def remove_message_queue(name, q):
    up = UserProcess.processing.get(name)
    if up: up.listeners.remove(q)
  
  @staticmethod
  def is_still_processing(name):
    return name in UserProcess.processing
    
  @staticmethod
  def get_greenlet_for(name):
    return UserProcess.processing.get(name).greenlet
  
  def __init__(self, name):
    self.name = name
    self.all_albums = set()
    self.recent_albums = []
    self.messages_so_far = []
    self.listeners = set()
    self.greenlet = None
  
  def retrieve_results(self):
    """retrieve results from db and return get_results()"""
    for a in data.get_user_top_albums(self.name,"overall"):
      self.all_albums.add((a["artist"],a["name"]))
    self.recent_albums = data.get_user_top_albums(self.name,"12month")
    return self._get_results()
    
  def retrieve_results_for_topic(self, topic, offset, past_album):
    for a in data.get_user_top_albums(self.name,"overall"):
      self.all_albums.add((a["artist"],a["name"]))
    results = []
    past = False
    for album in data.get_top_of_topic(topic, limit=100, offset=offset-1):
      if not past:
        if album["name"]==past_album: past=True
        continue
      if (album["artist"],album["name"]) in self.all_albums: continue
      results.append(album)
    return results
    
    
  def _get_results(self):
    if len(self.recent_albums) < 5: 
      return {"error":"no or too little data"}
      
    recent_album_dict = dict([((a["artist"], a["name"]), a) for a in self.recent_albums])
    
    topicsums = None
    topics = []
    topic_influence = {}
    for dist in data.find_topics(map(lambda a: {"artist": a["artist"],"name": a["name"]}, self.recent_albums)):
      if not topicsums: topicsums = [0]*len(dist["distribution"])
      count = (int(recent_album_dict[(dist["artist"],dist["name"])]["playcount"]))**0.5
      topicsums = map(lambda (a,b): a+b*count, zip(topicsums, dist["distribution"]))
      for topic_index, dist_value in enumerate(dist["distribution"]):
        influence = dist_value * count
        if influence<0.1: continue
        topic_influence.setdefault(topic_index,[]).append((influence,dist))
        
    top_topics = filter(lambda (e,t): t>0, sorted(enumerate(topicsums), key=lambda (e,t): t, reverse=True))
    
    for topic_index, score in top_topics[:10]:
      topic = (topic_index,{"new":[]})
      for album in data.get_top_of_topic(topic_index):
        if (album["artist"],album["name"]) in self.all_albums: continue
        topic[1]["new"].append(album)
      topic[1]["exist"] = [a for count, a in sorted(topic_influence[topic_index],reverse=True)[:10]]
      topics.append(topic)
    return topics
  
  def _send_feedback(self, page, type, done, extra=None):
    d = {"page":page,"type":type,"done":done}
    if extra: d["extra"]=extra
    for q in self.listeners: q.put(d)
    self.messages_so_far.append(d)
  
  def get_new_recommendations(self):
    """get data from lastfm api and store them in db"""
    self.greenlet = gevent.spawn(self.get_new_recommendations_and_wait)
    UserProcess.processing[self.name] = self

  def get_new_recommendations_and_wait(self):
    """get data from lastfm api and store them in db, blocks until catching processed is finished"""
    try:
      children = [self._get_all_albums(), self._get_current_top_albums()]
      gevent.joinall(children)
      for child in children: 
        if not child.successful():
          self._send_feedback(-1,"error",True,str(child.exception))
          return
      self._send_feedback(0,"everything",True)
    finally:
      UserProcess.processing.pop(self.name)
    
    

  def _get_all_albums(self):
    return gevent.spawn(PagedRequest(
                      self.name,
                      "user.gettopalbums",
                      lambda r: r.get("topalbums",{}).get("album",[]),
                      PagedRequest.extract_album,
                      lambda r: int(r["topalbums"]["@attr"]["total"]),
                      lambda username, albums, period: self._got_all_albums(albums),
                      period="overall",
                      threshold_exp=0.25,
                      listeners = (lambda page: self._send_feedback(page, "all",False), lambda page: self._send_feedback(page, "all",True))
                    ).call_and_wait)
    
  def _got_all_albums(self, albums):
    if not isinstance(albums, list): albums = [albums]
    data.save_user_top_albums(self.name,albums,"overall")
    for a in albums: self.all_albums.add((a["artist"],a["name"]))
  
  
  def _get_current_top_albums(self):
    return gevent.spawn(PagedRequest(
                      self.name,
                      "user.gettopalbums",
                      lambda r: r.get("topalbums",{}).get("album",[]),
                      PagedRequest.extract_album,
                      lambda r: int(r["topalbums"]["@attr"]["total"]),
                      lambda username, albums, period: self._got_current_top_albums(albums),
                      period="12month",
                      threshold_exp=0.45,
                      listeners = (lambda page: self._send_feedback(page, "current",False), lambda page: self._send_feedback(page, "current",True))
                    ).call_and_wait)
    
  def _got_current_top_albums(self, albums):
    if not isinstance(albums, list): albums = [albums]
    data.save_user_top_albums(self.name,albums,"12month")
    self.recent_albums = albums      
    
    
if __name__ == "__main__":
  import sys
  if len(sys.argv)<2:
    print "give me a name!"
    sys.exit(1)
  UserProcess(sys.argv[1]).get_new_recommendations_and_wait()