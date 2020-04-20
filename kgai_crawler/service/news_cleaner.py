"""
Cleans up news articles so that clean text is remaining
"""
from typing import List

from kgio_py_commons.logging.log import TRLogger
from kgio_py_commons.model.googlenews.news_article import NewsArticle
from newspaper import Article, ArticleException


class NewsCleaner(object):

    def __init__(self):
        self.logger = TRLogger.instance().get_logger(__name__)

    def _scrape(self, url: str) -> str:
        """
        gets a clean text of the article
        Args:
            url:

        Returns:

        """
        text = ""

        try:
            article = Article(url)
            article.download()
            article.parse()
            text = article.text
        except ArticleException:
            self.logger.error("could not parse article in {}".format(url))

        return text

    def clean_scrape(self, news_articles: List[NewsArticle]) -> List[NewsArticle]:
        """
        clean scrapes the news article text if it can be accessed
        Args:
            news_articles:

        Returns:

        """
        for article in news_articles:
            if article.url:
                article.articleText = self._scrape(article.url)

        return news_articles
