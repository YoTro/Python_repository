#coding:utf-8
import matplotlib.pyplot as plt 
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
from sklearn import svm #对比
from sklearn.datasets import load_digits
from sklearn.model_selection  import train_test_split
from sklearn.metrics import classification_report
import pickle
import os

'''
http://jmlr.csail.mit.edu/papers/volume11/cawley10a/cawley10a.pdf
http://jmlr.csail.mit.edu/papers/volume8/cawley07a/cawley07a.pdf

libsvm源码论文http://www.csie.ntu.edu.tw/~cjlin/papers/libsvm.pdf

核函数选取的方法https://www.csie.ntu.edu.tw/~cjlin/papers/guide/guide.pdf
	1. 如果特征的数量大到和样本数量差不多，则选用线性核

	2. 如果特征的数量小，样本的数量正常，则选用高斯核函数

	3. 如果特征的数量小，而样本的数量很大，则需要手工添加一些特征从而变成第一种情况

svm原论文中文翻译https://zhuanlan.zhihu.com/p/23068673
sklearn数据集文件地址: /Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages/sklearn/datasets/data

'''

__all__ = [
"loadData",
"Kernel",
"heurstic_selectJ",
"calEi",
"Normal_vector",
"SMOv1",
"SMOv2",
"innerLoop",
"show3DClassifer",
"predict",
"show2DData",
"cmp_sklearn_svm"
]
def loadData(filepath):
	'''
	加载数据并分类
	return:   数据集和标签集
	'''
	datas = []
	labels = []
	f = open(filepath)
	for i in f.readlines():
		l = i.strip().split('\t')
		datas.append((float(l[0]), float(l[1])))
		labels.append(float(l[2]))
	return datas, labels

def show2DData(datas, labels):
	'''
	function: 可视化样本数据
	datas:   list
	labels:  list
	rtype:   none
	'''
	data_positive = []
	data_negetive = []
	for i in range(len(datas)):
		#分类
		if labels[i] > 0:
			data_positive.append(datas[i])
		else:
			data_negetive.append(datas[i])
	x_p, y_p = np.transpose(np.array(data_positive))[0], np.transpose(np.array(data_positive))[1]
	x_n, y_n = np.transpose(np.array(data_negetive))[0], np.transpose(np.array(data_negetive))[1]
	plt.scatter(x_p, y_p)
	plt.scatter(x_n, y_n)
	plt.show()

class SVM(object):
	'''
	求最小间隔margin 
	1/2||w||^2
	s.t. y_i(w^Tx_i + b)>= 1, i = 1,2,..,n
	核心步骤
	1. 计算误差Ei,Ej
	2. 计算alpha上下界L,H
	3. 计算学习速率theta = 2*K(xi, xj) - K(xi, xi) - K(xj, xj)
	4. 更新alpha_i, 和alpha_j
	5. 计算b1,b2, 并更新b
	'''
	def __init__(self, datas, labels, C, tol, maxIter, kwg):
		self.X = np.mat(datas)                         #自变量X向量shape(m,n)
		self.y = np.mat(labels).transpose()            #(+1,-1)数据集shape(m, 1)
		self.w = np.zeros((len(datas[0]), 1))          #超平面的法向量w
		self.C = C                                     #惩罚系数一般为1
		self.tol = tol                                 #误差上限一般为1e4 (Tolerance for stopping criteria) float, optional.
		self.m = len(datas)                            #矩阵长度
		self.K = np.mat(np.zeros((self.m, self.m)))
		self.b = 0                                     #阈值
		self.maxIter = maxIter                         #最大迭代次数
		self.alphas = np.mat(np.zeros((self.m, 1)))    #超参数由拉格朗日函数引入                      
		self.eCache = np.mat(np.zeros((self.m, 2)))    #存储误差Ei = Ui-yi 
		self.kwargs = kwg                              #核函数的选择linear, rbf, poly等
		self.nonzeralphas_train = None                 #用于保存训练模型中的非0alpha下标
		self.support_vector_X = None                   #训练过的支持向量
		self.support_vector_alphas = None              #支持向量参数alphas
		self.support_vector_labels = None              #支持向量标签    

	def Kernel(self, x1, x2):
		'''
		核函数:
			线性核函数linear:                                    x1*x2
			多项式核函数poly:                                    (x1*x2 + C)^d   可以使原来线性不可分的样本数据变为线性可分
			高斯核函数rbf(Radial Basis Function Kernel)          正态分布E^{(||x1-x2||^2)/-2*theta^2}
			拉普拉斯核函数Laplace
			Sigmoid
		具体参考sklearn svm源代码

		'''
		#初始化一个矩阵保存计算结果
		KT = np.mat(np.zeros((self.m,1)))
		if self.kwargs['kernel'] == 'linear':
			return x1 * x2.T

		elif self.kwargs['kernel'] == 'rbf':
			#theta默认1.3
			theta = self.kwargs['theta']
			for j in range(self.m):
				delta = x1[j,:] - x2
				KT[j] = delta * delta.T
			return np.exp(KT / (-2.0 * theta**2))

		elif self.kwargs['kernel'] == 'poly':
			KT = x1 * x2.T
			degree = self.kwargs['degree'] #int, default=3
			for j in range(self.m):
				KT[j] = (KT[j] + self.C)**degree
			return KT

		elif self.kwargs['kernel'] == 'Laplace':
			theta = self.kwargs['theta']
			for j in range(self.m):
				deltaX = x1[j,:] - x2 
				KT[j] = np.sqrt(deltaX * deltaX.T)
			return np.exp( - KT / theta)

		elif self.kwargs['kernel'] == 'sigmoid':
			KT = x1 * x2.T
			a = self.kwargs['a']
			for j in range(self.m):
				KT[j] = np.tanh(a * KT[j] + self.m)
		else:
			raise NameError("can't recognise the kernel mathod")

	def calEi(self, i):
		'''
		计算误差Ei
		Ei = ui - yi

		'''
		
		ui = float(np.multiply(self.alphas, self.y).T * self.K[:,i]) + self.b
		Ei = ui - float(self.y[i])
		
		return Ei


	def heurstic_selectJ(self, i, Ei):
		'''
		启发式选择j,使得|Ej - Ei|最大

		'''
		maxK = -1
		maxDeltaE = 0
		Ej = 0
		self.eCache[i] = [1, Ei] #更新,存入Ei
		#查找误差Ei集合中的非0元素并返回所有下标 
		oneEcachelist = np.nonzero(self.eCache[:,0].A)[0]
		if len(oneEcachelist) > 1:
			for k in oneEcachelist:
				if k == i:
					continue
				Ek = self.calEi(k)
				deltaEk = abs(Ek - Ei)
				if (deltaEk > maxDeltaE):
					maxK = k
					maxDeltaE = deltaEk
					Ej = Ek 
		else:
			maxK = i 
			while maxK == i:
				maxK = np.random.choice(self.m)
			Ej = self.calEi(maxK)
		return maxK, Ej

	def Normal_vector(self):
		'''
		计算w向量
		datas:  数据矩阵
		labels: (-1, +1)集合
		alphas: 
		'''
		#转换成array
		for i in range(self.m):
			self.w += np.multiply(self.alphas[i] * self.y[i], self.X[i, :].T)

	def SMOv1(self):
		'''
		简化版smo算法,不包含启发式选择j参数
		datas:          数据矩阵
		labels:         标签(1, -1)
		C:              松弛变量
		tol:            容错率
		maxIter:        最大迭代次数
		b:              截距
		alpha:          拉格朗日乘子
		yi:             +1, -1
		xi:             数据集(向量)
		w:              法向量 sum{alpha_i*yi*dataMats[i:]*dataMats^T}
		fxi:            sum{alpha_i*yi*xi^TxI} + b
		Ei:             误差项Ei = fxi - yi
		eta:            学习速率xi^Txi + xj^Txj - 2xi^Txj
		数学公式:
		min 1/2||w||^2
		s.t. y_i(w^Tx_i + b)>= 1, i = 1,2,..,n

		'''
		for i in range(self.m):
			self.K[:,i] = self.Kernel(self.X, self.X[i,:])
		#初始化迭代次数
		iternum = 0
		#更新核矩阵
		while (iternum < self.maxIter):
			#统计alpha优化次数
			alphachangenum = 0
			for i in range(self.m):
				#1. 计算误差Ei
				yi = self.y[i]
				Ei = self.calEi(i)
				#优化alpha, 容错率
				if((yi*Ei < -self.tol) and (self.alphas[i] < self.C)) or ((yi*Ei > self.tol) and (self.alphas[i] > 0)):
					#随机选择alphaj并且不等于alphai
					j = i 
					while j == i:
						j = np.random.choice(self.m)
					#计算误差Ej
					yj = self.y[j]
					Ej = self.calEi(j)
					#保存更新前的alpha
					alpha_i_old = self.alphas[i].copy()
					alpha_j_old = self.alphas[j].copy()
					#2. 计算alpha上下界
					if yi != yj:#异侧
						L = max(0, self.alphas[j] - self.alphas[i])
						H = min(self.C, self.C + self.alphas[j] - self.alphas[i])
					else: #同侧
						L = max(0, self.alphas[j] + self.alphas[i] - self.C)
						H = min(self.C, self.alphas[j] + self.alphas[i])
					if L == H:
						print("L == H")
						continue
					#3. 计算eta
					eta = 2.0 * self.K[i,j] - self.K[i,i] - self.K[j,j]
					if eta >= 0:#半正定
						print("eta >= 0")
						continue
					#4. 更新alphaj
					self.alphas[j] -= yj * (Ei - Ej)/eta
					#5. 修剪alphaj
					if self.alphas[j] > H:
						self.alphas[j] = H
					if self.alphas[j] < L:
						self.alphas[j] = L
					if abs(self.alphas[j] - alpha_j_old) < 0.00001:
						print("alphas_{} = {} is updated".format(j, self.alphas[j]))
						continue
					#6. 更新alphai
					s = yi*yj
					self.alphas[i] += s*(alpha_j_old - self.alphas[j])
					#7. 更新b1, b2
					b1 = self.b - Ei - yi * (self.alphas[i] - alpha_i_old) * self.K[i,i] - yj * (self.alphas[j] - alpha_j_old) * self.K[i,j]
					b2 = self.b - Ej - yi * (self.alphas[i] - alpha_i_old) * self.K[i,j] - yj * (self.alphas[j] - alpha_j_old) * self.K[j,j]
					#8. 更新b
					if 0 < self.alphas[i] and self.C > self.alphas[i]:
						self.b = b1 
					elif 0 < self.alphas[j] and self.C > self.alphas[j]:
						self.b = b2
					else:
						self.b = (b1 + b2) / 2.0
					#更新优化统计
					alphachangenum += 1
					print("Iter:{} Smaples: No.{}, alphas update times:{}".format(iternum, i, alphachangenum))
			if alphachangenum == 0:
				iternum += 1
			else:
				iternum = 0 
			print("-----------------No.{}th iteration-----------------".format(iternum))
		

	def SMOv2(self):
		'''
		完整版包含启发式选择j
		datas:          数据矩阵
		labels:         标签(1, -1)
		C:              松弛变量
		tol:            容错率
		maxIter:        最大迭代次数
		b:              截距
		alpha:          拉格朗日乘子
		yi:             +1, -1
		xi:             数据集(向量)
		w:              法向量
		fxi:            sum{alpha_i*yi*xi^TxI} + b
		Ei:             误差项Ei = fxi - yi
		eta:            学习速率xi^Txi + xj^Txj - 2xi^Txj
		数学公式:
		min 1/2||w||^2
		s.t. y_i(w^Tx_i + b)>= 1, i = 1,2,..,n
		'''
		#更新核矩阵

        #迭代次数
		iternum = 0
		#遍历所有训练数据标识符
		AllX = True 
		for i in range(self.m):
			self.K[:,i] = self.Kernel(self.X, self.X[i,:])
		#alpha更新次数
		alphachangenum = 0
		while (iternum < self.maxIter) and ((alphachangenum > 0) or (AllX)):
			alphachangenum = 0 
			if AllX:
				#遍历整个训练集
				for i in range(self.m):
					alphachangenum += self.innerLoop(i)
				iternum += 1 
				
			else:
				#遍历非边界子集(0<a<C),获取界内所有alpha下标的数组
				nonBound = np.nonzero((self.alphas.A > 0) * (self.alphas.A < self.C))[0]
				for i in nonBound:
					alphachangenum += self.innerLoop(i)
				iternum += 1 
			#交替
			if AllX:
				AllX = False
			elif alphachangenum == 0:
				#如果非边界的点没有更新alpha, 切换为遍历整个训练集
				AllX = True
		self.nonzeralphas_train    = np.nonzero(self.alphas.A)[0]
		self.support_vector_X      = self.X[self.nonzeralphas_train]
		self.support_vector_alphas = self.alphas[self.nonzeralphas_train]
		self.support_vector_labels = self.y[self.nonzeralphas_train]
		self.Normal_vector() 

	def innerLoop(self, i):
		'''
		找出不满足KKT条件的alpha,并优化
		核心步骤:
		1. 计算误差Ei,Ej
		2. 计算alpha上下界L,H
		3. 计算学习速率theta = 2*K(xi, xj) - K(xi, xi) - K(xj, xj)
		4. 更新alpha_i, 和alpha_j
		5. 计算b1,b2, 并更新b
		退出循环条件: L == H, eta >= 0, alpha_j变化值很小等
		'''
		#1. 计算误差Ej
		Ei = self.calEi(i)
		yi = self.y[i]
		if (yi*Ei < - self.tol and self.alphas[i] < self.C) or (yi * Ei > self.tol and self.alphas[i] > 0):
			#1. 计算误差Ej
			j, Ej = self.heurstic_selectJ(i, Ei)
			yj = self.y[j]
			#保存旧的alpha
			alpha_i_old = self.alphas[i].copy()
			alpha_j_old = self.alphas[j].copy()
			#2. 计算alpha上下界
			if yi != yj:
				L = max(0, self.alphas[j] - self.alphas[i])
				H = min(self.C, self.C + self.alphas[j] - self.alphas[i])
			else:
				L = max(0, self.alphas[j] + self.alphas[i] - self.C)
				H = min(self.C, self.alphas[i] + self.alphas[j])
			if L == H:
				return 0
			#3. 计算theta
			eta = 2.0 * self.K[i,j] - self.K[i,i] - self.K[j,j]
			if eta >= 0:
				return 0
			#4. 更新alpha_j
			self.alphas[j] -= yj * (Ei - Ej) / eta
			#修正alpha_j
			if self.alphas[j] > H:
				self.alphas[j] = H
			if self.alphas[j] < L:
				self.alphas[j] = L
			#更新误差缓存Ej,1表示Ej被计算过
			self.eCache[j] = [1, self.calEi(j)]
			#如果alpha收敛到一定值,则退出(ε一般默认0.00001
			if abs(alpha_j_old - self.alphas[j]) < 0.00001:
				return 0
			#4. 否则更新alpha_i
			s = yj * yi
			self.alphas[i] += s * (alpha_j_old - self.alphas[j])
			#更新Ei
			self.eCache[i] = [1, self.calEi(i)]
			#5. 计算b1, b2
			b1 = self.b - Ei - yi * (self.alphas[i] - alpha_i_old) * self.K[i,i] - yj * (self.alphas[j] - alpha_j_old) * self.K[i,j]
			b2 = self.b - Ej - yi * (self.alphas[i] - alpha_i_old) * self.K[i,j] - yj * (self.alphas[j] - alpha_j_old) * self.K[j,j]
			#5. 更新b
			if 0 < self.alphas[i] and self.alphas[i] < self.C:
				self.b = b1
			elif 0 < self.alphas[j] and self.alphas[j] < self.C:
				self.b = b2
			else:
				self.b = (b1 + b2) / 2.0 
			return 1
		else:
			return 0
	def show3DClassifer(self):
		'''
		分类结果可视化
		datas:  数据矩阵type:list
		w:      超平面法向量
		b:      超平面截距
		'''
		data_positive = []
		data_negetive = []
		for i in range(len(self.X)):
			#样本分类
			if self.y[i] > 0:
				data_positive.append(self.X[i])
			else:
				data_negetive.append(self.X[i])
		x_p, y_p = np.transpose(np.array(data_positive))[0], np.transpose(np.array(data_positive))[1]
		x_n, y_n =  np.transpose(np.array(data_negetive))[0], np.transpose(np.array(data_negetive))[1]
		ax = plt.subplot(111, projection='3d')
		ax.scatter(x_p, y_p, zs = 1.0, c = 'red')
		ax.scatter(x_n, y_n, zs = -1.0, c = 'blue')
		#绘制直线
		x1 = max(self.X.tolist())[0]
		x2 = min(self.X.tolist())[0]
		x = np.mat([x1, x2])#数据集x的范围
		#y = wx + b
		print self.w
		y = ((-self.b-self.w[0, 0]*x)/self.w[1, 0]).tolist()
		plt.plot(x.tolist()[0], y[0], 'k--')
		#找出支持向量的点
		for i, alpha in enumerate(self.alphas):
			if alpha > 0.0:
				x, y = self.X[i, 0], self.X[i, 1]
				plt.scatter([x], [y], s = 150, c = 'none', alpha = 0.7, linewidth = 1.5, edgecolor = 'red')
		plt.show()
	def predict(self, testDatas, testLables):
		'''
		输入测试数据,根据训练模型得出预测结果
		'''
		testDataMat = np.mat(testDatas)
		testLableMat = np.mat(testLables)
		preresult = []
		c = 0.0
		for i in range(testDataMat.shape[0]):
			pre_y = float(np.multiply(self.support_vector_alphas, self.support_vector_labels).T * self.Kernel(self.support_vector_X, testDataMat[i,:])) + self.b
			preresult.append(pre_y)
			if np.sign(pre_y) == np.sign(testLables[i]):
				c += 1.0
		print("The correct rate and number is {}, {}".format(c/len(testDataMat), c))
		return np.array(preresult)

	def cmp_sklearn_svm(self):
		mnist = load_digits()
		x_train,x_test,y_train,y_test = train_test_split(self.X,self.y,test_size=0.25,random_state=40)
		model = svm.SVC(kernel = self.kwargs['kernel'])
		model.fit(x_train, y_train)
		z = model.predict(x_test)
		print('The total number of predicts: {} \nThe correct rate and number: {}, {}'.format(z.size, float(np.sum(z==y_test))/z.size, np.sum(z==y_test)))
		print("The score of model: {}".format(model.score(x_test, y_test)))
		print(classification_report(y_test, z))#, target_names = "mnist.target_names".astype(str)))
		if not os.path.exists('modelpkl'):
			os.mkdir('modelpkl')
		with open('modelpkl/digits.pkl','wb') as file:
			pickle.dump(model,file)

if __name__ == '__main__':
	print __all__
	filepath = '/Users/jin/Desktop/testSet.txt'
	#datas, labels = loadData(filepath)
	mnist = load_digits()
	datas, labels = mnist.images.reshape((len(mnist.images), -1)), mnist.target
	#kwg = {'kernel': 'linear'}
	kwg = {'kernel': 'rbf', 'theta': 1.3}
	svm0 = SVM(datas, labels, 0.6, 0.001, 100, kwg)
	svm0.SMOv2()
	svm0.show3DClassifer()
	svm0.predict(datas, labels)
	svm0.cmp_sklearn_svm()
