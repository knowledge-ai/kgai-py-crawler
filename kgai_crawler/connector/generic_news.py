"""
Gathers generic news from the internet
"""
from typing import List

import newspaper
from kgai_py_commons.logging.log import TRLogger
from kgai_py_commons.model.googlenews.news_article import NewsArticle
from kgai_py_commons.model.googlenews.source_article import SourceArticle
from newspaper import Article


class GenericNews(object):
    def __init__(self):
        self.logger = TRLogger.instance().get_logger(__name__)

    def get_news(self, source: SourceArticle) -> List[NewsArticle]:
        """
        scrapes a given SourceArticle for all news it can find
        Args:
            source:

        Returns:

        """
        all_articles = []
        news = newspaper.build(source.url)

        # for each source get articles published in source
        self.logger.info("Found {} articles in {}".format(len(news.articles), source.url))
        for article in news.articles:
            try:

                article.download()
                article.parse()
                all_articles.append(
                    self._map_articles(article=article, source_name=source.name, source_url=source.url))

            except Exception as err:
                self.logger.error("Error while gathering article from: {}, error: {}".format(article.url, err))

        return all_articles

    @staticmethod
    def _map_articles(article: Article, source_name: str, source_url: str) -> NewsArticle:
        """
        INternal helper to map from scraped articles to Dataclass representations
        Args:
            article:
            source_name:
            source_url:

        Returns:

        """
        return NewsArticle(title=article.title, publishedAt=str(article.publish_date), url=article.url,
                           articleText=article.text, content=article.article_html, author=str(article.authors),
                           urlToImage=None, description=None,
                           source=SourceArticle(name=source_name, id=None, description=None, category=None,
                                                language=None, country=None, url=source_url))
