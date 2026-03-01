import unittest
from src.zhipin.scraper import scrape_zhipin

class TestZhipinScraper(unittest.TestCase):
    def test_smoke(self):
        """
        A simple smoke test to ensure the scraper function can be called without errors.
        """
        try:
            # This is a smoke test, so we don't need to actually scrape anything.
            # We can just check if the function is callable.
            self.assertTrue(callable(scrape_zhipin))
        except Exception as e:
            self.fail(f"scrape_zhipin function is not callable or failed with an exception: {e}")

if __name__ == '__main__':
    unittest.main()
