"""
Tests functionality of the Google news class
"""
from kgai_py_commons.model.googlenews.source_article import SourceArticle

from kgai_crawler.connector.generic_news import GenericNews


class TestGooglenews(object):

    @classmethod
    def setup_class(cls):
        """
        setup common class functionalities
        """
        cls.news_client = GenericNews()

    def test_news(self):
        news = self.news_client.get_news(sources=[
            SourceArticle(name="BBC news", url="http://www.bbc.co.uk/news", id=None, description=None, category=None,
                          language=None, country=None)])
        assert len(news), "Expected news on topic"
