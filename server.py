from gevent import monkey; monkey.patch_all()

import json

from bottle.ext.websocket import GeventWebSocketServer
from bottle.ext.websocket import websocket

from bottle import run, get, post, static_file, redirect, request, response

import config
from templates import render
from userprocess import UserProcess

HOST = config.get("host")

def render_json(obj,status=200):
  response.content_type = "application/json"
  response.status = status
  return json.dumps(obj)

def is_XHR():
  return request.get_header("X-Requested-With") == "XMLHttpRequest"
      
@get("/")
def index():
  return render("index")
  
@post("/username")
def post_username():
  username = request.forms.get('username')
  if username: username = username.strip()
  
  if not username: 
    if is_XHR(): 
      return render_json({"error":"need valid username"},409)
    else:
      redirect("/")
  
  if UserProcess.is_still_processing(username): 
    if is_XHR():
      return render_json({"update":"/get-update/%s" % username, "ws_update":"ws://%s/ws/%s" % (HOST,username)})
    else:
      redirect("/await/%s" % username)
  UserProcess(username).get_new_recommendations()
  
  if is_XHR(): 
    return render_json({"update":"/get-update/%s" % username, "ws_update":"ws://%s/ws/%s" % (HOST,username)})
  
  redirect("/await/%s" % username)

@get('/ws/<username>', apply=[websocket])
def echo(ws, username):
  if not ws: return "please provide websocket"
  
  q = UserProcess.get_message_queue(username)
  try:
    if not UserProcess.is_still_processing(username):
      return ws.send(json.dumps({"done":True,"results":"/results/%s" % username}))
    while True:
      msg = q.get()
      if msg.get("type")=="everything" and msg.get("done"): 
        return ws.send(json.dumps({"done":True,"results":"/results/%s" % username}))
      else:
        ws.send(json.dumps({"done":False,"message":msg}))
  finally:
    UserProcess.remove_message_queue(username,q)

# long polling fallback
@get("/get-update/<username>")
def get_update(username):
  g = UserProcess.get_greenlet_for(username)
  if not g: return render_json({"done":True, "results":"/results/%s" % username})
  g.join(55)
  if g.ready(): 
    return render_json({"done":True, "results":"/results/%s" % username})
  else:
    return render_json({"done":False, "update":"/get-update/%s" % username})
  
@get("/await/<username>")
def await_results(username):
  if not UserProcess.is_still_processing(username): redirect("/results/%s" % username)
  return render("await",username=username)
  
@get("/results/<username>")
def show_results(username):
  if UserProcess.is_still_processing(username): redirect("/await/%s" % username)
  results = UserProcess(username).retrieve_results()
  return render("results", username=username, results=results)
  
  
@get("/results/<username>/show-more/<topic>/<offset>/<past_album>")
def show_more(username, topic, offset, past_album):
  results = UserProcess(username).retrieve_results_for_topic(int(topic), int(offset), unicode(past_album.decode("utf-8")))
  if is_XHR(): return render("show-more-xhr", username=username, topic=topic, results=results, nextoffset=int(offset)+len(results))
  return render("show-more", username=username, topic=topic, results=results, nextoffset=int(offset)+len(results))
  
  
@get('/static/js/<filename:re:.*\.js>')
def javascripts(filename):
  return static_file(filename, root=config.basedir('static/js'))

@get('/static/css/<filename:re:.*\.css>')
def stylesheets(filename):
  return static_file(filename, root=config.basedir('static/css'))

@get('/static/images/<filename:re:.*\.(jpg|png|gif|ico)>')
def images(filename):
  return static_file(filename, root=config.basedir('static/images'))

@get('/static/fonts/<filename:re:.*\.(eot|ttf|woff|svg)>')
def fonts(filename):
  return static_file(filename, root=config.basedir('static/fonts'))

if __name__=="__main__":
  run(host='0.0.0.0', port=8080, server=GeventWebSocketServer,reloader=config.get("debug"),debug=config.get("debug"))