"""
Tests functionality of the Google news class
"""
import os

from dotenv import load_dotenv

from kgai_crawler.connector.google_news import GoogNews


class TestGooglenews(object):

    @classmethod
    def setup_class(cls):
        """
        setup common class functionalities
        """
        load_dotenv()
        if not os.getenv("GOOG_NEWS_KEY"):
            cls.logger.error("News API key not found, no defaults set")
            raise EnvironmentError
        cls.goog_client = GoogNews(api_key=os.getenv("GOOG_NEWS_KEY"))

    def test_news(self):
        news = self.goog_client.get_news(topic="corona", scrape_month=False, parse_all=False)
        assert len(news), "Expected news on topic"

    def test_news_headlines(self):
        news = self.goog_client.get_news_headlines(country="in")
        assert len(news), "Expected news headlines on topic"

    def test_get_news_sources(self):
        sources = self.goog_client.get_news_sources()
        assert len(sources), "Expected non empty sources"
