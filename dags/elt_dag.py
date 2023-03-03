from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import feedparser
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import requests
import random
import redis
import concurrent.futures
import html




NEWS_FEEDS = {
        "en": [
            "https://www.cnn.com/rss/edition.rss",
            "https://www.bbc.com/news/10628494",
            "https://www.nbcnews.com/id/303207/device/rss/rss.xml",
            "https://www.foxnews.com/about/rss/"
        ],
        "pl": [
            "https://www.tvn24.pl/najnowsze.xml",
            "https://www.rmf24.pl/fakty/polska/feed",
            "https://wiadomosci.wp.pl/rss",
            "https://www.money.pl/rss/wszystkie"
        ],
        "es": [
            "https://www.elpais.com/rss/feed.html?feedId=1022",
            "https://www.abc.es/rss/feeds/abc_EspanaEspana.xml",
            "https://www.elconfidencial.com/rss/",
            "https://www.elperiodico.com/es/rss/"
        ],
        "de": [
            "https://www.tagesschau.de/xml/rss2",
            "https://www.faz.net/rss/aktuell/",
            "https://www.zeit.de/rss",
            "https://www.spiegel.de/schlagzeilen/tops/index.rss"
        ],
        "fr": [
            "https://www.lemonde.fr/rss/une.xml",
            "https://www.lefigaro.fr/rss/figaro_actualites.xml",
            "https://www.liberation.fr/rss/",
            "https://www.lci.fr/rss"
        ]
    }

headers_list = [
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate", 
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8", 
        "Dnt": "1", 
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1", 
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36", 
        "X-Amzn-Trace-Id": "Root=1-5ee7bae0-82260c065baf5ad7f0b3a3e3"
    },
    {
        "User-Agent": 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0',
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.reddit.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }   
]
# Define default_args dictionary to pass to the DAG


ARGS = {
    "owner": "stefentaime",
    "start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}


dag = DAG(
    dag_id="ELT-Pipeline",
    default_args=ARGS,
    description="",
    schedule_interval="0 0 1 * *",
    tags=["ETL", "kafka", "Scrapting"]
)

REDIS_CONFIG = {'host': 'redis', 'port': 6379, 'decode_responses': True}
REDIS_KEY = 'proxies'
PROXY_WEBPAGE = 'https://free-proxy-list.net/'
TESTING_URL = 'https://httpbin.org/ip'
MAX_WORKERS = 20
PROXY_EXPIRATION = timedelta(minutes=5)

def get_proxies():
    r = redis.Redis(**REDIS_CONFIG)
    if r.exists(REDIS_KEY):
        proxies = r.lrange(REDIS_KEY, 0, -1)
        expiration = r.ttl(REDIS_KEY)
        if expiration == -1:
            r.expire(REDIS_KEY, PROXY_EXPIRATION)
        elif expiration < PROXY_EXPIRATION.total_seconds():
            r.delete(REDIS_KEY)
            proxies = []
    else:
        proxies = []
    if not proxies:
        headers = random.choice(headers_list)
        page = requests.get(PROXY_WEBPAGE, headers=headers)
        soup = BeautifulSoup(page.content, 'html.parser')
        for row in soup.find('tbody').find_all('tr'):
            proxy = row.find_all('td')[0].text + ':' + row.find_all('td')[1].text
            proxies.append(proxy)
        r.rpush(REDIS_KEY, *proxies)
        r.expire(REDIS_KEY, PROXY_EXPIRATION)
    return proxies

def update_proxypool(**kwargs):
    get_proxies()

def test_proxy(proxies):
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(test_single_proxy, proxies))
    return (proxy for valid, proxy in zip(results, proxies) if valid)

def test_single_proxy(proxy):
    headers = random.choice(headers_list)
    try:
        resp = requests.get(TESTING_URL, headers=headers, proxies={"http": proxy, "https": proxy}, timeout=3)
        if resp.status_code == 200:
            return True
    except:
        pass
    return False


# Define the task to update the proxypool
def update_proxypool(**kwargs):
    proxies = get_proxies()
    valid_proxies = list(test_proxy(proxies))
    kwargs['ti'].xcom_push(key='valid_proxies', value=valid_proxies)

import datetime

next_id = 1

def extract_website_name(link):
    # Extract the website name from the link
    website_name = link.split('//')[1].split('/')[0]
    # Remove any leading "www." from the website name
    website_name = website_name.replace('www.', '')
    return website_name

def extract_article_data(entry, language):
    global next_id
    title = entry.title.encode('ascii', 'ignore').decode()
    soup = BeautifulSoup(entry.summary, 'html.parser')
    summary = html.unescape(soup.get_text().strip().replace('\xa0', ' '))
    link = entry.link
    date_published = entry.get('published_parsed', None)
    if date_published is not None:
        date_published = datetime.datetime(*date_published[:6])
        time_since_published = datetime.datetime.utcnow() - date_published
        if time_since_published < datetime.timedelta(hours=1):
            today = datetime.datetime.utcnow().strftime("%d-%m-%Y")
            website_name = extract_website_name(link)
            unique_id = f"{language.upper()}{next_id:02d}-{website_name}-01-{today}"
            next_id += 1
            return {
                'id': unique_id,
                'title': title,
                'link': link,
                'summary': summary,
                'language': language
            }
    return None

def extract_news_feed(feed_url, language, proxy):
    feed = feedparser.parse(feed_url, request_headers={'User-Agent': proxy})
    articles = []
    extracted_articles = set()
    for entry in feed.entries:
        if len(articles) >= 2:
            break
        link = entry.link
        title = entry.title.encode('ascii', 'ignore').decode()
        unique_id = f'{language}-{link}-{title}'
        if unique_id in extracted_articles:
            continue
        extracted_articles.add(unique_id)
        article_data = extract_article_data(entry, language)
        if article_data is not None:
            articles.append(article_data)
    return articles


def extract_news(**kwargs):
    valid_proxies = set(kwargs['ti'].xcom_pull(key='valid_proxies'))
    articles = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(extract_news_feed, feed_url, language, proxy) for language in NEWS_FEEDS.keys() 
                   for feed_url in NEWS_FEEDS[language] for proxy in valid_proxies]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                articles.extend(result)
        kwargs['ti'].xcom_push(key='articles', value=articles)
    return articles

# Define the task to validate the quality of the data
def validate_data(**kwargs):
    articles = kwargs['ti'].xcom_pull(key='articles', task_ids='extract_news')
    validated_articles = [article for article in articles if all(article.get(k) for k in ('title', 'link', 'summary'))]
    kwargs['ti'].xcom_push(key='validated_articles', value=validated_articles)
    return validated_articles


# Define the task to send data to the Kafka topic
def send_to_kafka(**kwargs):
    validated_articles = kwargs['ti'].xcom_pull(key='validated_articles', task_ids='validate_data')
    producer = KafkaProducer(bootstrap_servers='broker:29092')
    for article in validated_articles:
        try:
            producer.send('rss_feeds', key=article['title'].encode(), value=json.dumps(article).encode())
        except KafkaError as e:
            print(f"Failed to send message to Kafka: {e}")
        producer.flush()
    print("Data sent to Kafka successfully.")

# Define the task dependencies
update_proxypool_task = PythonOperator(task_id='update_proxypool', python_callable=update_proxypool, provide_context=True, dag=dag)
extract_news_task = PythonOperator(task_id='extract_news', python_callable=extract_news, provide_context=True, dag=dag)
validate_data_task = PythonOperator(task_id='validate_data', python_callable=validate_data, provide_context=True, dag=dag)
send_to_kafka_task = PythonOperator(task_id='send_to_kafka', python_callable=send_to_kafka, provide_context=True, dag=dag)

# Set the task dependencies
update_proxypool_task >> extract_news_task >> validate_data_task >> send_to_kafka_task