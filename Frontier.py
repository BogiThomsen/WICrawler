import queue as q
import random
from urllib.parse import urlparse
import datetime
import time
import heapq

priority_bias = []
queue_map = {}
heap = []
front_queues = {}
back_queues = {}
polite_time = 2

def initialize_queues_and_priorities(number_of_crawlers, number_of_priorities):
  for i in range(number_of_crawlers*3):
    back_queues[str(i+1)] = q.Queue()
  for k in range(number_of_priorities):
    front_queues[str(k+1)] = q.Queue()
    priority = [str(k+1)] * (number_of_priorities-k)
    priority_bias.extend(priority)



def prioritize_urls(urls, number_of_priorities):
  #!?!?!??!?!!?!?!?1
  i = 1
  for url in urls:
    add_to_front(url, i)
    if i % (number_of_priorities) == 0:
      i = 1
    else:
      i=i+1

def add_to_front(url, priority):
  front_queues[str(priority)].put(url)

def pick_url_from_front_queue():
  try:
    index = random.choice(priority_bias)
    url = front_queues[index].get(False)
    front_queues[index].task_done()
    return url
  except q.Empty:
    pass

def front_not_empty():
  for index, queue in front_queues.items():
    if not queue.empty():
      return True
  return False

def back_not_full():
  for index, queue in back_queues.items():
    if queue.empty():
      return True
  return False

def update_back_queue():
  while front_not_empty() and back_not_full():
        add_to_back(pick_url_from_front_queue())


def add_to_back(url):
  if url != None:
    full_url = urlparse(url)
    host = '{uri.netloc}'.format(uri=full_url)
    if host in queue_map:
      back_queues[queue_map[host]].put(url)
      update_back_queue()
    else:
      for index, queue in back_queues.items():
        if queue.empty():
          queue_map[host] = index
          queue.put(url)
          heapq.heappush(heap, (datetime.datetime.now().time(), host))
          break


def get_url_from_back_queue():
  try:
    current_host = heap[0]
    if current_host[0] < datetime.datetime.now().time():
      heapq.heappop(heap)
      index = queue_map[current_host[1]]
      url = back_queues[index].get()
      back_queues[index].task_done()
      if back_queues[index].empty():
        del queue_map[current_host[1]]
        update_back_queue()
      else:
        heapq.heappush(heap, ((datetime.datetime.now()+datetime.timedelta(seconds=polite_time)).time(), current_host[1]))
      return url
    else:
      time.sleep(1)
      return get_url_from_back_queue()
  except IndexError:
    print("No more URL's")
#initialize_queues_and_priorities(3, 2)
#add_to_back("http://dr.dk")
#get_from_back()
