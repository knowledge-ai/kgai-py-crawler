"""
Connector to Google news API
"""
from datetime import datetime, timedelta
from http.client import HTTPException
from typing import List

from dacite import from_dict
from kgai_py_commons.logging.log import TRLogger
from kgai_py_commons.model.googlenews.news_article import NewsArticle
from kgai_py_commons.model.googlenews.source_article import SourceArticle
from newsapi import NewsApiClient


class GoogNews(object):
    """
    Interacts with Google news API
    """

    def __init__(self, api_key: str):
        self._news_client = NewsApiClient(api_key=api_key)
        self.logger = TRLogger.instance().get_logger(__name__)

    def get_news(self, topic: str, scrape_month: bool = False) -> List[NewsArticle]:
        """
        Retrieves google news for a topic
        Args:
            parse_all:
            topic:
            scrape_month:

        Returns:

        """
        all_articles = []
        from_date = datetime.now()
        to_date = datetime.now() - timedelta(days=1)
        try:
            if scrape_month:
                for delta in range(0, 30):
                    from_date = from_date - timedelta(days=delta)
                    to_date = to_date - timedelta(days=delta + 1)
                    all_articles.extend(self._get_news_free(topic=topic, from_date=from_date, to_date=to_date))
            else:
                all_articles.extend(self._get_news_free(topic=topic, from_date=from_date, to_date=to_date))
        except HTTPException as exp:
            self.logger.error("Unable to fetch news, HTTP error: {}".format(exp))

        return all_articles

    def _get_news_free(self, topic: str, from_date: datetime, to_date: datetime) -> List[NewsArticle]:
        """
        Get free news which is limited to 100 news articles, for the given date
        Args:
            topic:
            from_date:
            to_date:

        Returns:

        """
        try:
            # /v2/everything
            articles = self._news_client.get_everything(q=topic,
                                                        language='en',
                                                        from_param=from_date,
                                                        to=to_date,
                                                        sort_by='relevancy',
                                                        page_size=100,  # cannot get more than this in free plan
                                                        page=1)  # same as above
            # map to dataclass
            return self._map_articles(articles=articles)

        except HTTPException as exp:
            self.logger.error("Unable to fetch news, HTTP error: {}".format(exp))

    def get_news_headlines(self, country: str) -> List[NewsArticle]:
        """
        Retrieves headlines for a country (must be iso2 code)
        Args:
            country:

        Returns:

        """
        headlines = {}
        try:
            headlines = self._news_client.get_top_headlines(country=country, page_size=100)
        except HTTPException as exp:
            self.logger.error("Unable to fetch news, HTTP error: {}".format(exp))

        return self._map_articles(articles=headlines)

    def get_news_sources(self) -> List[SourceArticle]:
        """
        Get all listed google news sources
        Returns:

        """
        sources = {}
        try:
            # /v2/sources
            sources = self._news_client.get_sources()
        except HTTPException as exp:
            self.logger.error("Unable to fetch news, HTTP error: {}".format(exp))

        return self._map_sources(sources=sources)

    @staticmethod
    def _map_articles(articles: dict) -> List[NewsArticle]:
        """
        Map google news articles to respective data classes
        Args:
            articles:

        Returns:

        """
        articls = []

        for articl in articles.get("articles", []):
            articls.append(from_dict(data_class=NewsArticle, data=articl))

        return articls

    @staticmethod
    def _map_sources(sources: dict) -> List[SourceArticle]:
        """
        Map google news sources to respective data classes
        Args:
            sources:

        Returns:

        """
        sourcs = []

        for sourc in sources.get("sources", []):
            sourcs.append(from_dict(data_class=SourceArticle, data=sourc))

        return sourcs
