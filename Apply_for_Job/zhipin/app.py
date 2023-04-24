import os
import web

urls = (
	'/zp_stoken',"zp_stoken",
	)

class zp_stoken(object):
	"""docstring for zp_stoken"""
	def GET(self):
		try:
			data = web.input()
			if len(data)==0:
				filename = './zp_stoken.txt'
				if (os.path.exists(filename)):
					with open(filename, 'r') as f:
						zpt = f.read()
				else:
					zpt = ""		
				return zpt
			else:
				return {"data":data}
		except Exception as e:
			print(e)
			return {"Exception":e}

if __name__ == '__main__':
	app = application(urls, globals())
	app.run()

		