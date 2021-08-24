import requests
from bs4 import BeautifulSoup
from Queue import Queue
import pprint
import time
import datetime
import logging
import logging.config
import threading
import json
import task_queue

pp = pprint.PrettyPrinter(indent=4)
articles = []
exist_link = []

init_depth = 0  # init crawl depth
site = 'all'  # init site
date = '2021-08-24'  # init date
init_url = "https://indeks.kompas.com/?site={}&date={}".format(site, date)
max_pages = 1  # init max page count

# init crawl content, if True will crawl the body content, if False will crawl data without content
crawl_content = True

logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
        },
    },
    'loggers': {
        '': {  # root logger
            'level': 'INFO',
            'propagate': True
        }
    }
}
logging.config.dictConfig(logging_config)


def parse_url(post_url):
    headers = {
        'user-agent': 'okhttp/3.11.0',
    }
    response = requests.get(post_url, headers=headers)

    content = response.content
    parsed_response = BeautifulSoup(content, "lxml")
    return parsed_response


def extract_post_data(post_url, depth=0):
    try:
        logging.info("process article link - depth: {} - {}".format(depth, post_url))
        article_link = "{}?page=all".format(post_url)
        soup_post = parse_url(article_link)
        title = soup_post.find("h1", {"class", "read__title"}).text
        read__time = soup_post.find("div", {"class", "read__time"}).text

        read_time_container = read__time.split("-")

        author = read_time_container[0].strip() if len(read_time_container) > 1 else ""
        published_date = read_time_container[1].strip() if len(read_time_container) > 1 else read_time_container[
            0].strip() if len(
            read_time_container) > 0 else ""
        par = soup_post.findAll('p')

        list_text = []

        # related_article = []

        for x in par:
            text_p = x.text.strip()
            # text_p = re.sub(r"^\W+", "", text_p)
            if not text_p:
                continue
            if 'baca juga' in text_p.lower() or 'baca:' in text_p.lower():
                if depth > 0:
                    depth -= 1
                    link = x.find("a")["href"]
                    extract_post_data(link, depth)
                    # if related and len(related) > 0:
                    #     data = related[0]
                    #     if data["url"] not in exist_link:
                    #         related_article.append(data)
                continue
            if text_p[-1] != '.':
                continue
            list_text.append(text_p)

        results = []
        obj = {
            "title": title,
            "url": post_url,
            "published_date": published_date,
            "author": author,
        }

        if crawl_content:
            obj["content"] = list_text

        if post_url not in exist_link:
            exist_link.append(post_url)
            results.append(obj)
            # results.extend(related_article)
            articles.extend(results)
        time.sleep(0.5)
    except Exception as e:
        logging.error(e, exc_info=True)


def get_urls():
    next_button = ""
    url = init_url
    urls = [url]
    count = 1
    while next_button is not None and count < max_pages:
        soup = parse_url(url)
        next_button = soup.find("div", {"class": "paging"}).find("a", {"class": "paging__link paging__link--next",
                                                                       "rel": "next"})
        if next_button is not None:
            url = next_button["href"]
            if url not in urls:
                urls.append(url)
                count += 1

    return urls


def process(url):
    # url = task_queue.get()
    logging.info("Process {}".format(url))
    soup = parse_url(url)
    section = soup.find("div", {"class": "latest--indeks"})
    posts = section.findAll("div", {"class": "article__list"})

    int_max_pool = 5
    for post in posts:
        uri = post.find("h3").find("a")["href"]

        pool = task_queue.ThreadPool(int_max_pool)
        pool.add_task(extract_post_data, uri, init_depth)
        pool.wait_completion()


def execute():
    try:
        urls = get_urls()
        for url in urls:
            process(url)
            time.sleep(1)

    except Exception as e:
        logging.error(e, exc_info=True)


execute()
pp.pprint(articles)
with open("data_file-{}.json".format(datetime.datetime.now()), "w") as write_file:
    json.dump(articles, write_file)
logging.info("Site: {}".format(site))
logging.info("Date: {}".format(date))
logging.info("Index: {}".format(init_url))
logging.info("Depth: {}".format(init_depth))
logging.info("Max Pages: {}".format(max_pages))
logging.info("Finish Total: {} article".format(len(articles)))
