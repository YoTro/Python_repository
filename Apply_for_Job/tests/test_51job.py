import unittest
from src.job51.drission_scraper import JobSpider, get_city_code

class Test51JobScraper(unittest.TestCase):
    def test_smoke(self):
        """
        A simple smoke test to ensure the DrissionPage scraper can be instantiated.
        """
        try:
            spider = JobSpider(headless=True)
            self.assertIsNotNone(spider)
            if hasattr(spider, 'page') and spider.page:
                spider.page.quit()
        except Exception as e:
            self.fail(f"JobSpider instantiation failed with an exception: {e}")

    def test_get_city_code(self):
        """
        Test that city code translation works for known cities.
        """
        code = get_city_code("深圳")
        self.assertEqual(code, "040000") # 深圳 is 040000 on 51job

        code_unknown = get_city_code("未知城市")
        self.assertEqual(code_unknown, "000000")

if __name__ == '__main__':
    unittest.main()
