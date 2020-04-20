"""
Tests functionality of the Google news class
"""
import os

import pytest

from kgai_crawler.controller.news_aggregator import NewsAggregator


class TestNewsAggregator(object):

    @classmethod
    def setup_class(cls):
        """
        setup common class functionalities
        """
        goog_api_key = os.getenv("GOOG_NEWS_KEY")
        kafka_broker = os.getenv("KAFKA_BROKER")
        kafka_schema = os.getenv("KAFKA_SCHEMA")
        avro_namespace = os.getenv("AVRO_NAMESPACE") + "-test"
        cls.news_aggregator = NewsAggregator(goog_api_key=goog_api_key, kafka_broker=kafka_broker,
                                             kafka_schema=kafka_schema, avro_namespace=avro_namespace)

    @pytest.mark.slow
    def test_news_article(self):
        kafka_topic = os.getenv("KAFKA_NEWS_RAW_TOPIC") + "-test"
        news = self.news_aggregator.aggregate(news_topic="corona", scrape_month=False, parse_all=False,
                                              kafka_topic=kafka_topic)
        assert len(news), "Expected news on topic"
        assert news.pop().articleText, "Expected news on topic"
