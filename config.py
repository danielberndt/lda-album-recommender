import os.path

BASEDIR = os.path.dirname(os.path.realpath(__file__))
FILE = "app.conf"

_conf = {}

def interprete(value):
  if value.lower()=="true": return True
  if value.lower()=="false": return False
  return value

f = open(os.path.join(BASEDIR, FILE))
for line in map(str.strip,f):
  if not line or line.startswith("#"): continue
  _conf.update(dict((map(str.strip, line.split("=",1)),)))

for key, val in _conf.iteritems(): _conf[key] = interprete(val)

f.close()

def get(key):
  return _conf[key]


if __name__=="__main__":
  print _conf