FILE = "app.conf"

_conf = {}

def interprete(value):
  if value.lower()=="true": return True
  if value.lower()=="false": return False
  return value

f = open(FILE)
for line in map(str.strip,f):
  if not line or line.startswith("#"): continue
  _conf.update(dict((map(str.strip, line.split("=",1)),)))

for key, val in _conf.iteritems(): _conf[key] = interprete(val)

f.close()

def get(key):
  return _conf[key]
