# -*- coding:UTF-8 -*-
# 登陆卖家精灵
import json
import hashlib
import requests

def salt_pwd_hash(email, password):
	'''对password加密,返回salt'''
	password_hash = hashlib.md5(password.encode()).hexdigest()
	email_password_hash = email + password_hash
	salt = hashlib.md5(email_password_hash.encode()).hexdigest()
	return password_hash, salt

def Sellersprite_login(session, email, pwd, salt):
	'''模拟登陆卖家精灵'''
	url = "https://www.sellersprite.com/w/user/signin"
	headers = {
		"host": "www.sellersprite.com",
		"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
		"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
	    "accept-language": "zh-CN,zh;q=0.9",
	    "cache-control": "max-age=0",
	    "content-type": "application/x-www-form-urlencoded",
	    "sec-ch-ua": "\"Chromium\";v=\"110\", \"Not A(Brand\";v=\"24\", \"Google Chrome\";v=\"110\"",
	    "sec-ch-ua-mobile": "?0",
	    "sec-ch-ua-platform": "\"Windows\"",
	    "sec-fetch-dest": "document",
	    "sec-fetch-mode": "navigate",
	    "sec-fetch-site": "same-origin",
	    "sec-fetch-user": "?1",
	    "upgrade-insecure-requests": "1"
	}
	FormData = {
		"callback": "",
		"password": pwd,
		"email": email,
		"autoLogin": "Y",
		"salt": salt
	}
	r = session.post(url, headers = headers, data = FormData)
#	with open('./t.html', 'w', encoding = "UTF-8") as f:
#		f.write(r.text)
#		f.close()
	return session

def get_keywors_traffic_extend_asin(session, asins):
	'''获取asin数组的扩展流量词'''
	url = "https://www.sellersprite.com/v3/api/traffic/extend/asin"
	headers = {
		"host": "www.sellersprite.com",
		"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
	    "accept": "application/json, text/plain, */*",
	    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
	    "content-type": "application/json;charset=UTF-8",
#	    "sec-ch-ua": "\"Chromium\";v=\"110\", \"Not A(Brand\";v=\"24\", \"Google Chrome\";v=\"110\"",
#	    "sec-ch-ua-mobile": "?0",
#	    "sec-ch-ua-platform": "\"Windows\"",
#	    "sec-fetch-dest": "empty",
#	    "sec-fetch-mode": "cors",
#	    "sec-fetch-site": "same-origin"
  	}
	Payload = {
		"queryVariations":"true",
		"asinList":asins,
		"originAsinList":asins,
		"market":1,
		"page":1,
		"month":"",
		"size":50,
		"orderColumn":12,
		"desc":"true",
		"exactly":"false",
		"ac":"false"
	}
	r = session.post(url, headers = headers, data = json.dumps(Payload))
	keywords = []
	if r.status_code == 200:
		#print(r.json())
		keywordlist = r.json()['data']['items']
		for i in range(len(keywordlist)):
			keywords.append(keywordlist[i]['keywords'])
	else:
		print(r.text)
	return keywords
if __name__ == '__main__':
	email = ""
	password = ""
	asins = ["B098T9ZFB5","B09JW5FNVX","B0B71DH45N","B07MHHM31K","B08RYQR1CJ"]
	pwd, salt = salt_pwd_hash(email, password)
	session = requests.Session()
	session = Sellersprite_login(session, email, pwd, salt)
	keywords = get_keywors_traffic_extend_asin(session, asins)
	print(keywords)
