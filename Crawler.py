from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.parse import urlparse
import urllib
import threading
import ParseRobotsTxt as prt
import NearDuplicate as N
import Frontier
import pymongo

seed = ["http://yahoo.com", "http://www.youtube.com", "http://www.dba.dk", "http://www.dr.dk", "https://www.bt.dk", "https://en.wikipedia.org", "https://www.aau.dk", "https://tv2.dk", "http://bilbasen.dk"]
number_of_crawlers = 7
number_of_priorities = 10
user_agent = "CrawlyMcCrawlFace"
header = {"User-Agent": user_agent}

#robots = prt.parse(base_url)

##html_doc = urllib.request.urlopen(base_url + starting_point)
##soup = BeautifulSoup(html_doc, 'html.parser')
threads = []

def get_links(page):
  links = []
  for url in page.find_all('a', href=True):
    link = url.get('href')
    if link.startswith('#'):
      continue
    else:
      links.append(link)
  return links

def get_text(page):
  for line in page(["script", "style"]):
    line.decompose()
  return page.get_text(" ")



def crawl(url):
  try:
    #print("Visiting: " + url)
    full_url = urlparse(url)
    req = urllib.request.Request(url, headers=header)
    page = urlopen(req)
    soup = BeautifulSoup(page, 'html.parser')
    text = get_text(soup)
    links = get_links(soup)
    allowed_links = prt.parse_and_test_links(get_base_url(full_url), links, user_agent)
    checked_links = N.check_duplicate_urls(allowed_links)

    N.check_and_save_page(url, text)
    
    Frontier.prioritize_urls(checked_links, number_of_priorities)
    #print(Frontier.queue_map)
    #print("Printing Front Queue:")
    #for index, queue in Frontier.front_queues.items():
    #  print(queue.queue)
    #print("Printing Back Queue:")
    #for index, queue in Frontier.back_queues.items():
    #  print(queue.queue)
  except (urllib.error.HTTPError, urllib.error.URLError) as e:
    pass

def get_base_url(url):
  return '{uri.scheme}://{uri.netloc}'.format(uri=url)


def crawler():
  while True:
    url = Frontier.get_url_from_back_queue()
    if url is None:
      break
    crawl(url)


def start(num_crawler_threads):
  for i in range(num_crawler_threads):
    t = threading.Thread(target=crawler)
    t.start()
    threads.append(t)


def main():
  Frontier.initialize_queues_and_priorities(number_of_crawlers, number_of_priorities)
  Frontier.prioritize_urls(seed, number_of_priorities)
  Frontier.update_back_queue()
  start(number_of_crawlers)
  

if __name__ == "__main__":
  main()