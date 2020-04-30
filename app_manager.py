"""
Starting point for managing/running the app
"""

__author__ = 'Ritaja'

import os

from distutils.util import strtobool
from dotenv import load_dotenv
from kgai_py_commons.logging.log import TRLogger

from kgai_crawler.controller.news_aggregator import NewsAggregator


class AppManager(object):
    def __init__(self):
        self.logger = TRLogger.instance().get_logger(__name__)
        self._goog_api_key()
        self._kafka_config()
        self.news_aggregator = NewsAggregator(goog_api_key=self.goog_api_key, kafka_broker=self.kafka_broker,
                                              kafka_schema=self.kafka_schema, avro_namespace=self.avro_namespace)

    def _goog_api_key(self):
        """
        configuration helper
        Returns:

        """
        if not os.getenv("GOOG_NEWS_KEY"):
            self.logger.error("News API key not found, no defaults set")
            raise EnvironmentError
        self.goog_api_key = os.getenv("GOOG_NEWS_KEY")

    def _kafka_config(self):
        """
        configuration helper
        Returns:

        """
        # check if all configs are set properly
        if not any([os.getenv("KAFKA_BROKER"), os.getenv("KAFKA_SCHEMA"),
                    os.getenv("AVRO_NAMESPACE")]):
            self.logger.error("Kafka configs not set properly, no defaults set")
            raise EnvironmentError

        if not os.getenv("KAFKA_NEWS_RAW_TOPIC"):
            self.logger.error("News kafka topic name not found, no defaults set")
            raise EnvironmentError

        self.kafka_broker = os.getenv("KAFKA_BROKER")
        self.kafka_schema = os.getenv("KAFKA_SCHEMA")
        self.avro_namespace = os.getenv("AVRO_NAMESPACE")
        self.kafka_topic = os.getenv("KAFKA_NEWS_RAW_TOPIC")
        self.logger.info(
            "kafka client configured with KAFKA_BROKER: {}, KAFKA_SCHEMA: {}, AVRO_NAMESPACE: {}, "
            "KAFKA_NEWS_RAW_TOPIC: {}".format(
                self.kafka_broker, self.kafka_schema, self.avro_namespace, self.kafka_topic))

    def gather_news(self, news_topic: str, scrape_month: bool = False):
        """
        scrapes and aggregates all news articles and publishes to kafka stream
        Args:
            news_topic:
            scrape_month:

        Returns:

        """
        self.logger.info(
            "Gathering news.. config: NEWS_TOPIC:{}, SCRAPE_MONTH:{}".format(news_topic, scrape_month))
        self.news_aggregator.aggregate_google(news_topic=news_topic, kafka_topic=self.kafka_topic,
                                              scrape_month=scrape_month)
        self.news_aggregator.aggregate_non_google(kafka_topic=self.kafka_topic)

    def common_crawl_news(self, download_dir_article: str, download_dir_warc: str):
        self.news_aggregator.common_crawl_news(kafka_topic=self.kafka_topic, download_dir_article=download_dir_article,
                                               download_dir_warc=download_dir_warc)


if __name__ == '__main__':
    load_dotenv()
    app_manager = AppManager()
    news_topic = os.getenv("NEWS_TOPIC")
    download_dir_warc = os.getenv("WARC_DOWNLOAD_DIR")
    download_dir_article = os.getenv("ARTICLE_DOWNLOAD_DIR")
    scrape_month = strtobool(os.getenv("SCRAPE_MONTH"))
    common_crawl = strtobool(os.getenv("COMMON_CRAWL"))
    news_crawl = strtobool(os.getenv("NEWS_CRAWL"))

    if news_crawl:
        AppManager().gather_news(news_topic=news_topic, scrape_month=scrape_month)
    if common_crawl:
        app_manager.common_crawl_news(download_dir_warc=download_dir_warc, download_dir_article=download_dir_article)
