import lastfm_api, data

checked_user = set()

def identify_existing_users():
  checked_user.update(map(LastFMUser,data.get_existing_user_names()))

class LastFMUser:
  
  def __init__(self, name, playcount=100000):
    self.name = name
    self.playcount = playcount
    
  def start_process(self):
    print "start process for %s" % (self.name)
    checked_user.add(self)
    queuenum = self.get_top_albums()
    if lastfm_api.queue.qsize()<500: self.get_friends()  
    return queuenum
  
  def get_friends(self):
    return lastfm_api.call_json("user.getfriends", self.got_friends, {"user":self.name})

  def got_friends(self, result):
    for user_obj in result["friends"].get("user",[]):
      if not isinstance(user_obj,dict): continue
      if int(user_obj["playcount"]) < 10000: continue
      user = LastFMUser(user_obj["name"],int(user_obj["playcount"]))
      if user not in checked_user: user.start_process()
      
  def get_top_albums(self):
    return PagedRequest(
                      self.name,
                      "user.gettopalbums",
                      lambda r: r.get("topalbums",{}).get("album",[]),
                      PagedRequest.extract_album,
                      lambda r: int(r["topalbums"]["@attr"]["total"]),
                      lambda username, albums, period: data.save_user_top_albums(username, albums, period)
                    ).call()
  
  def __eq__(self, other):
    return other and self.name == other.name

  def __hash__(self):
    return hash(self.name)

class PagedRequest:
  
  @staticmethod
  def extract_album(album):
    return {"name":album["name"],"playcount":album["playcount"], "artist":album["artist"]["name"], "mbid":album["mbid"], "image_medium":album["image"][1]["#text"], "image_large":album["image"][3]["#text"], "artist_url":album["artist"]["url"],"album_url":album["url"]}
  
  def __init__(self, username, method, extract_results, extract_element_information, get_total, save_to_db, period="12month", threshold_exp=0.5, listeners = (None,None)):
    self.username = username
    self.method = method
    self.extract_results = extract_results
    self.extract_element_information = extract_element_information
    self.get_total = get_total
    self.save_to_db = save_to_db
    self.currpage = 1
    self.all = []
    self.min_play_threshold = None
    self.greenlets = set()
    self.period = period
    self.threshold_exp = threshold_exp
    self.start_listener = listeners[0]
    self.end_listener = listeners[1]

  def call_and_wait(self):
    self.call()
    while len(self.greenlets): 
      g = self.greenlets.pop()
      g.join()
      if not g.successful(): raise g.exception

  def call(self):
    g = lastfm_api.call_json(self.method, self.got_result, {"user":self.username,"page":self.currpage,"limit":100, "period":self.period}, priority=100-self.currpage)
    if self.start_listener: self.start_listener(self.currpage)
    self.greenlets.add(g)
    return g
    
  def calc_threshold(self, total_elems, first_page_elems):
    self.min_play_threshold = max((int(first_page_elems[len(first_page_elems)/2-1]["playcount"])*total_elems**0.5)**self.threshold_exp,30*self.threshold_exp)
  
  def finish(self):
    print "found %d %s for %s" % (len(self.all),self.method,self.username)
    if len(self.all)>10: self.save_to_db(self.username, self.all, "12month")
  
  def got_result(self, result):
    if self.end_listener: self.end_listener(self.currpage)
    if isinstance(result,basestring): return self.finish()
    elems = self.extract_results(result)
    if isinstance(elems,dict): elems = [elems]
    if not len(elems):
      self.finish()
      return
    
    if not self.min_play_threshold: self.calc_threshold(self.get_total(result), elems)
  
    for elem in elems:
      if isinstance(elem,basestring): return self.finish()
      if int(elem["playcount"])<self.min_play_threshold: return self.finish()
      self.all.append(self.extract_element_information(elem))
  
    self.currpage += 1
    self.call()
    
    

if __name__=="__main__":
  import sys
  if len(sys.argv)<2:
    print "give me a name!"
    sys.exit(1)
  identify_existing_users()
  
  for name in sys.argv[1:]: LastFMUser(name).start_process()
  import gevent
  while True:
    gevent.sleep(10)
    print
    print "QUEUE: %5d" % lastfm_api.queue.qsize()
    print
    if lastfm_api.queue.qsize()==0: break