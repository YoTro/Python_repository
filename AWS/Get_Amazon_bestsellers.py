import requests
import time
import re
import platform
import csv

url = "https://www.amazon.com/Best-Sellers-Industrial-Scientific-Cut-Off-Wheels/zgbs/industrial/256194011/ref=zg_bs_nav_industrial_3_2665570011"
fp = "c:/bs.csv"
if platform.system().lower() == 'windows':
	fp = "c:/bs.csv"
if platform.system().lower() == 'darwin' or platform.system().lower() == 'linux':
	fp = "/Users/bs.csv"
def amazonbs(url):
    headers={
        "Host":
        "www.amazon.com",
        "User-Agent":
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9v",
        "Accept-Language":
        "zh-CN,en;q=0.8,zh;q=0.7,zh-TW;q=0.5,zh-HK;q=0.3,en-US;q=0.2",
        "Accept-Encoding":
        "gzip, deflate, br",
        "Connection":
        "keep-alive",
                 "Upgrade-Insecure-Requests":"1"
                 }
    proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}
    try:
    	r = requests.get(url, headers = headers, proxies = proxies)
    except requests.exceptions.RequestException as e:
    	raise SystemExit(e)
    if (r.status_code == 200):
	    ranks = re.findall(r'<span class="zg-bdg-text">#(\d+)<\/span>', r.content)
	    asins = re.findall(r'<a class="a-link-normal" tabindex="-1" href=".*?\/dp\/(.*?)\/ref', r.content)
	    titles = re.findall(r'<div class="_cDEzb_p13n-sc-css-line-clamp-3_g3dy1">(.*?)<\/div>', r.content)
	    imgs = re.findall(r'a-section a-spacing-mini _cDEzb_noop_3Xbw5.*?src="(.*?)\"', r.content)
	    stars = re.findall(r'a-icon-alt">(.*?) out of 5 stars<\/span>', r.content)
	    reviews = re.findall(r'a-size-small">(.*?)<\/span>', r.content) 
	    prices = re.findall(r'<span class="a-size-base a-color-price"><span class="_cDEzb_p13n-sc-price_3mJ9Z">(.*?)<\/span>', r.content)
    return ranks, asins, titles, imgs, stars, reviews, prices
def list_to_csv(fp, ranks, asins, titles, imgs, stars, reviews, prices):
	with open('/Users/jin/Desktop/bs.csv', 'w+') as csvfile:
		spamwriter = csv.writer(csvfile, delimiter=',', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
		spamwriter.writerow(['Rank'] + ['Asin'] + ['Title'] + ['Img'] + ['Star'] + ['Review'] + ['prices'])
		t = []
		print(len(prices))
		for i in range(len(prices)):
			#print i
			spamwriter.writerow([ranks[i], asins[i], titles[i], imgs[i], stars[i], reviews[i], prices[i]])
		print("{}: Save sucessfully!".format(fp))

if __name__ == '__main__':
	ranks, asins, titles, imgs, stars, reviews, prices = amazonbs(url)
	list_to_csv(fp, ranks, asins, titles, imgs, stars, reviews, prices)