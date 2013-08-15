
import csv


class nsky_fscaner :
	def init(self, configreader, rootpath = './', resultfile = './logfiles.csv') :
		self.configreader = configreader
		self.rootpath = rootpath
		self.resultfile = resultfile

		self.dbfile = open(resultfile, 'wb+')
		self.resreader = csv.DictReader(self.dbfile)
		self.reswriter = csv.DictWriter(self.dbfile)

	def dump(self) :
		if not self.csvreader :
			raise 'not init..'
		for row in self.csvreader :
			print row

	def procone(self, curmodule) :
		module = curmodule['module']
		relpath = curmodule['relpath']
		path = self.rootpath + relpath
		
	def procall(self) :
		for curmodule in self.configreader :
			self.procone(curmodule)
	
	

def run() :
	scaner = nsky_fscaner()
	csvfile = open('ninesky_module.csv', 'rb')
	reader = csv.DictReader(csvfile)
	scaner.init(reader)
	scaner.procone()

run()

