from urllib.request import urlopen
from urllib.parse import urlparse
import urllib.robotparser
from url_normalize import url_normalize

def parse_and_test_links(url, links, user_agent):
  robot = url + "/robots.txt"
  rp = urllib.robotparser.RobotFileParser()
  rp.set_url(robot)
  norm = normalize(url, links)
  try:
    rp.read()
  except UnicodeDecodeError:
    return []


  allowed_links = []
  for link in norm:
    if rp.can_fetch("*", link) or rp.can_fetch(user_agent, link):
      allowed_links.append(link)
    else:
      continue

  return allowed_links
  

def normalize(url, links):
  normalized = []
  for link in links:
    try:
      if '@' in link:
        continue
      elif bool(urlparse(link).netloc):
        normalized.append(url_normalize(link, default_scheme="http"))
      elif link.startswith('/'):
        if url.endswith('/'):
          link = '{}{}'.format(url, link[1:])
          normalized.append(url_normalize(link, default_scheme="http"))
    except UnicodeError:
      continue
  return normalized