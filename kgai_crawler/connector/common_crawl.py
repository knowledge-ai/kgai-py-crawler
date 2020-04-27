#!/usr/bin/env python
"""
This service downloads WARC files from commoncrawl.org's news crawl and extracts articles from these files. You can
define filter criteria that need to be met (see YOUR CONFIG section), otherwise an article is discarded. Currently, the
script stores the extracted articles in JSON files, but this behaviour can be adapted to your needs in the method
on_valid_article_extracted. To speed up the crawling and extraction process, the script supports multiprocessing. You can
control the number of processes with the parameter my_number_of_extraction_processes.
You can also crawl and extract articles programmatically, i.e., from within your own code, by using the class
CommonCrawlCrawler provided in newsplease.crawler.commoncrawl_crawler.py
In case the script crashes and contains a log message in the beginning that states that only 1 file on AWS storage
was found, make sure that awscli was correctly installed. You can check that by running aws --version from a terminal.
If aws is not installed, you can (on Ubuntu) also install it using sudo apt-get install awscli.
This script uses relative imports to ensure that the latest, local version of news-please is used, instead of the one
that might have been installed with pip. Hence, you must run this script following this workflow.

Adapted from: https://github.com/fhamborg/news-please.git

"""
import datetime
import hashlib
import json
import logging
import os
import sys

from kgai_py_commons.clients.kafka.producer.producer import TRAvroProducer
from kgai_py_commons.logging.log import TRLogger
from kgai_py_commons.model.googlenews.news_article import NewsArticle
from kgai_py_commons.model.googlenews.source_article import SourceArticle
from newsplease.crawler import commoncrawl_crawler as commoncrawl_crawler
from newsplease.examples.commoncrawl import on_valid_article_extracted

from kgai_crawler.utils.common_utils import hash_article


class CommonCrawl(object):
    def __init__(self, download_dir_article: str, download_dir_warc: str, kafka_producer: TRAvroProducer,
                 kafka_topic: str, filter_start_date: datetime = None, filter_end_date: datetime = None):
        """
        Configures the class to prep for downloading from common crawl
        explanations for arguments below
        Args:
            download_dir_article:
            download_dir_warc:
            filter_start_date:
            filter_end_date:
        """
        self.dir_warc = download_dir_warc
        if not os.path.exists(download_dir_article):
            os.makedirs(download_dir_article)
        self.dir_article = download_dir_article
        # hosts (if None or empty list, any host is OK)
        self.filter_valid_hosts = []  # example: ['elrancaguino.cl']
        # start date (if None, any date is OK as start date), as datetime
        if not filter_start_date:
            # self.filter_start_date = datetime.datetime.now()  # datetime.datetime(2016, 1, 1)
            self.filter_start_date = datetime.datetime(2016, 8, 1)  # datetime.datetime(2016, 1, 1)
        else:
            self.filter_start_date = filter_start_date
        # end date (if None, any date is OK as end date), as datetime
        if not filter_end_date:
            # self.filter_end_date = self.filter_start_date + datetime.timedelta(days=30)  # datetime.datetime(2016, 1, 1)
            self.filter_end_date = self.filter_start_date + datetime.timedelta(days=30)  # datetime.datetime(2016, 1, 1)
        else:
            self.filter_end_date = filter_end_date
        # if date filtering is strict and news-please could not detect
        # the date of an article, the article will be discarded
        self.filter_strict_date = True
        # if True, the script checks whether a file has been downloaded
        # already and uses that file instead of downloading
        # again. Note that there is no check whether the file has
        # been downloaded completely or is valid!
        self.reuse_previously_downloaded_files = True
        # continue after error
        self.continue_after_error = True
        # show the progress of downloading the WARC files
        self.show_download_progress = False
        # log_level
        self.log_level = logging.INFO
        # json export style
        self.json_export_style = 1  # 0 (minimize), 1 (pretty)
        # number of extraction processes
        self.number_of_extraction_processes = 1
        # if True, the WARC file will be deleted after all articles have been extracted from it
        self.delete_warc_after_extraction = True
        # if True, will continue extraction from the latest fully
        # downloaded but not fully extracted WARC files and then
        # crawling new WARC files. This assumes that the filter
        # criteria have not been changed since the previous run!
        self.continue_process = True
        # the kafka producer client (configured) to publish each article to
        self.kafka_producer = kafka_producer
        # the topic to pulish to
        self.kafka_topic = kafka_topic
        self.logger = TRLogger.instance().get_logger(__name__)
        # log essential config
        self.logger.info(
            "Common Crawl configured with start_date: {}, end_date: {}, warc_dir: {}, article_dir: {}".format(
                self.filter_start_date, self.filter_end_date, self.dir_warc, self.dir_article))

    def __get_pretty_filepath(self, path, article):
        """
        Pretty might be an euphemism, but this function tries to avoid too long filenames, while keeping some structure.
        :param path:
        :param name:
        :return:
        """
        short_filename = hashlib.sha256(article.filename.encode()).hexdigest()
        sub_dir = article.source_domain
        final_path = os.path.join(path, sub_dir)
        if not os.path.exists(final_path):
            os.makedirs(final_path)
        return os.path.join(final_path, short_filename + '.json')

    def _on_valid_article_extracted(self, article):
        """
        This function will be invoked for each article that was extracted successfully from the archived data and that
        satisfies the filter criteria.
        :param article:
        :return:
        """
        # do whatever you need to do with the article (e.g., save it to disk, store it in ElasticSearch, etc.)
        with open(self.__get_pretty_filepath(self.dir_article, article), 'w', encoding='utf-8') as outfile:
            if self.json_export_style == 0:
                json.dump(article.__dict__, outfile, default=str, separators=(',', ':'), ensure_ascii=False)
            elif self.json_export_style == 1:
                json.dump(article.__dict__, outfile, default=str, indent=4, sort_keys=True, ensure_ascii=False)

        # Push to Kafka
        article_dict = article.__dict__
        news_article = NewsArticle(title=article_dict.get("title"), publishedAt=str(article_dict.get("date_publish")),
                                   url=article_dict.get("url"),
                                   urlToImage="", description=article_dict.get("description"),
                                   content="",
                                   author=str(article_dict.get("authors")), articleText=article_dict.get("maintext"),
                                   source=SourceArticle(name=article_dict.get("source_domain"), id="",
                                                        description="", url=article_dict.get("url"),
                                                        category="", language=article_dict.get("language"),
                                                        country=""))
        # publish to kafka
        self.kafka_producer.produce(topic=self.kafka_topic, key=hash_article(news_article),
                                    value=news_article)
        self.logger.debug("Pushed article to kafka from common crawl")

    def _callback_on_warc_completed(self, warc_path, counter_article_passed, counter_article_discarded,
                                    counter_article_error, counter_article_total, counter_warc_processed):
        """
        This function will be invoked for each WARC file that was processed completely. Parameters represent total values,
        i.e., cumulated over all all previously processed WARC files.
        :param warc_path:
        :param counter_article_passed:
        :param counter_article_discarded:
        :param counter_article_error:
        :param counter_article_total:
        :param counter_warc_processed:
        :return:
        """
        self.logger.info(
            "WARC completed download: {} artciles_passed: {}, articles_discarded: {}, articles_error: {}, "
            "articles_total: {}, warc_processed: {}".format(
                warc_path, counter_article_passed, counter_article_discarded, counter_article_error,
                counter_article_total, counter_warc_processed))

    def crawl_news(self):
        """
        Downloads the common crawl archives (WARC files) for news and extracts them as JSON
        Returns:

        """
        if len(sys.argv) >= 2:
            my_local_download_dir_warc = sys.argv[1]
        if len(sys.argv) >= 3:
            my_local_download_dir_article = sys.argv[2]
        if len(sys.argv) >= 4:
            delete_warc_after_extraction = sys.argv[3] == "delete"
        if len(sys.argv) >= 5:
            my_number_of_extraction_processes = int(sys.argv[4])

        self.logger.info("download_dir_warc: {}".format(self.dir_warc))
        self.logger.info("download_dir_article: {}".format(self.dir_article))
        self.logger.info("delete_warc_after_extraction: {}".format(str(self.delete_warc_after_extraction)))
        self.logger.info("number_of_extraction_processes: {}".format(str(self.number_of_extraction_processes)))

        commoncrawl_crawler.crawl_from_commoncrawl(self._on_valid_article_extracted,
                                                   callback_on_warc_completed=self._callback_on_warc_completed,
                                                   valid_hosts=self.filter_valid_hosts,
                                                   start_date=self.filter_start_date,
                                                   end_date=self.filter_end_date,
                                                   strict_date=self.filter_strict_date,
                                                   reuse_previously_downloaded_files=self.reuse_previously_downloaded_files,
                                                   local_download_dir_warc=self.dir_warc,
                                                   continue_after_error=self.continue_after_error,
                                                   show_download_progress=self.show_download_progress,
                                                   number_of_extraction_processes=self.number_of_extraction_processes,
                                                   log_level=self.log_level,
                                                   delete_warc_after_extraction=self.delete_warc_after_extraction,
                                                   continue_process=True)
