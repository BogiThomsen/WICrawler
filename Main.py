import Crawler
import Frontier

seed = ["https://microsoft.com", "https://dr.dk", "https://aau.dk"]

def main():
  Frontier.add_seed(seed)
  Crawler.crawl()



if __name__ == "__main__":
  main()