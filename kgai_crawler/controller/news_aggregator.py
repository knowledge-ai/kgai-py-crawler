"""
Collects, scraps and aggregates the news
"""
from typing import List

import xxhash
from kgai_py_commons.clients.kafka.producer.producer import TRAvroProducer
from kgai_py_commons.logging.log import TRLogger
from kgai_py_commons.model.googlenews.news_article import NewsArticle

from kgai_crawler.connector.google_news import GoogNews
from kgai_crawler.service.news_cleaner import NewsCleaner


class NewsAggregator(object):

    def __init__(self, goog_api_key: str, kafka_broker: str, kafka_schema: str, avro_namespace: str):
        self.logger = TRLogger.instance().get_logger(__name__)
        self.goog_news = GoogNews(api_key=goog_api_key)
        self.news_cleaner = NewsCleaner()
        self.kafka_producer = TRAvroProducer(bootstrap_servers=kafka_broker,
                                             schema_registry=kafka_schema,
                                             namespace=avro_namespace,
                                             data_class=NewsArticle)

    def aggregate(self, news_topic: str, kafka_topic: str, scrape_month: bool = False, parse_all: bool = True) -> List[
        NewsArticle]:
        """
        scrapes and aggregates all news articles and publishes to kafka stream
        Args:
            kafka_topic: 
            news_topic:
            scrape_month:
            parse_all:

        Returns:

        """
        news_articles = self.news_cleaner.clean_scrape(
            self.goog_news.get_news(topic=news_topic, scrape_month=scrape_month, parse_all=parse_all))

        for article in news_articles:
            self.kafka_producer.produce(topic=kafka_topic, key=self._hash_article(article),
                                        value=article)
        return news_articles

    @staticmethod
    def _hash_article(article: NewsArticle) -> str:
        """
        creates a unique ID for an article by hashing
        Args:
            article:

        Returns:

        """
        hash_content = article.articleText + article.title + article.url
        return xxhash.xxh64(hash_content).hexdigest()
