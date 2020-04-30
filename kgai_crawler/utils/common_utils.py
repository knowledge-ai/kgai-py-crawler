import xxhash
from kgai_py_commons.model.googlenews.news_article import NewsArticle


def hash_article(article: NewsArticle) -> str:
    """
    creates a unique ID for an article by hashing
    Args:
        article:

    Returns:

    """
    hash_content = article.articleText + article.title + article.url
    return xxhash.xxh64(hash_content).hexdigest()
