from jinja2 import Environment, FileSystemLoader
import config

env = Environment(
    autoescape=True,
    loader=FileSystemLoader('templates')
  )

class Filters:
  
  def wsurl(path):
    return "ws://%s%s" % (config.get("host"), path)

for k,v in Filters.__dict__.items(): 
  if k.startswith("_"):continue
  if hasattr(v,"__call__"): env.filters[k] = v

def render(filename, *args, **kwargs):
  return env.get_template("%s.html" % filename).render(*args,**kwargs)
  
macro_template = env.get_template("macros.html")
  
def macro(name, *args):
  return getattr(name,macro_template.module)(*args)