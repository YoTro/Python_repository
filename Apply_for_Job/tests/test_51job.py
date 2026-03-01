import unittest
from src.job51.scraper import JobSpider

class Test51JobScraper(unittest.TestCase):
    def test_smoke(self):
        """
        A simple smoke test to ensure the scraper can be instantiated.
        """
        try:
            spider = JobSpider(headless=True)
            self.assertIsNotNone(spider)
        except Exception as e:
            self.fail(f"JobSpider instantiation failed with an exception: {e}")

if __name__ == '__main__':
    unittest.main()
