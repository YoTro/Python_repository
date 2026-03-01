import argparse
import sys
from src.job51.scraper import JobSpider
from src.zhipin.scraper import scrape_zhipin

# Add src to the Python path to allow absolute imports
sys.path.insert(0, './src')

def main():
    parser = argparse.ArgumentParser(description="A job scraper for 51job and zhipin.")
    parser.add_argument('scraper', choices=['51job', 'zhipin'], help="The scraper to use.")
    parser.add_argument('-k', '--keyword', required=True, help="The job keyword to search for.")
    parser.add_argument('-c', '--city', required=True, help="The city to search in.")
    parser.add_argument('-p', '--pages', type=int, default=2, help="The number of pages to scrape (for 51job).")

    args = parser.parse_args()

    if args.scraper == '51job':
        print(f"Running 51job scraper for '{args.keyword}' in '{args.city}'...")
        spider = JobSpider(headless=True)
        spider.run(keyword=args.keyword, city=args.city, pages=args.pages)
        print("51job scraper finished.")
    elif args.scraper == 'zhipin':
        # Note: zhipin scraper requires city code. You might want to add a mapping here.
        city_code_map = {
            "深圳": "101280600",
            "广州": "101280100",
            "北京": "101010100",
            "上海": "101020100",
            "杭州": "101210100",
        }
        city_code = city_code_map.get(args.city)
        if not city_code:
            print(f"Error: City '{args.city}' not supported for zhipin scraper.")
            sys.exit(1)
            
        print(f"Running zhipin scraper for '{args.keyword}' in '{args.city}'...")
        scrape_zhipin(query=args.keyword, city_code=city_code, max_pages=args.pages)
        print("Zhipin scraper finished.")

if __name__ == '__main__':
    main()