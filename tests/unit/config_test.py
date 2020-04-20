"""
Tests logging capabilities
"""
import os

from dotenv import load_dotenv


class TestConfig(object):

    def test_config_priority(self):
        os.environ["GOOG_NEWS_KEY"] = "false-key"
        load_dotenv()
        obs_value = os.getenv("GOOG_NEWS_KEY")
        assert obs_value == "false-key", "Expected environment override to work"
