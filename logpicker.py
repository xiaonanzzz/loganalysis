# coding=utf-8
import re
import logcollector
import collections
import collections
import codecs
import time


#log item class 
LogItem = collections.namedtuple('LogItem' , ['time', 'level', 'source', 'msg'])


class LogPicker :
	filepath = ""
	modulename = ""
	currentbufferindex = 0
	logFile = None
	logbuffer = None
	buffersize = 1024 
	headsizelimit = 35
	logcollector = None
	encoding = 'gbk'
	def __init__(self, filepath, modulename, logcollector, encoding = 'gbk') :
		if not filepath :
			raise "should indicate a file path"
		if not modulename :
			raise "should indicate a module name"
		self.filepath = filepath
		self.modulename = modulename
		self.headreg = re.compile("(\d{8}.\d{6})<(\w+)\s*:(\w+)>:\s")
		self.msgre = re.compile(".*")
		self.logcollector = logcollector
		self.encoding = encoding

		try :
			self._openFile()
		except Exception, e :
			raise "can't open file, please check the init parameter"

	def __del__(self) :
		self._closeFile()
		return 0

	def _openFile(self) :
		f = codecs.open(self.filepath, 'r', self.encoding) # open file
		self.logFile = f
		return 0

	def _closeFile(self) :
		if self.logFile :
			self.logFile.close()

	def _readbuffer(self) :
		assert(self.logFile)
		self.logbuffer = self.logFile.read(self.buffersize)
		self.currentbufferindex = 0
		return

	def pickOnelog(self):

		if not self.logbuffer :
			self._readbuffer()
		if not self.logbuffer :
			print 'reading to the end of file, no more log.'
			return None 

		time,level,source = self._pickHeader()
		if not time :
			print 'no more valid log'
			return None

		msg = self._pickMsg()
		assert(msg)
		return LogItem(time,level,source,msg)
	
	def _updatebufferindex(self, index) :
		assert(index >= 0 and index <= len(self.logbuffer))
		if index >= len(self.logbuffer) :
			self._readbuffer()
		else :
			self.currentbufferindex = index


	def _pickHeader(self) :
		logitem = None
		assert(self.logbuffer and self.currentbufferindex < len(self.logbuffer))

		while self.logbuffer :
			match = self.headreg.search(self.logbuffer, self.currentbufferindex)
			if match :
				time = self._pickTime(match.group(1))
				level = self._pickLevel(match.group(2))
				source = self._pickSource(match.group(3))
				self._updatebufferindex(match.end(0))
				return (time, level, source)
			else :
				if len(self.logbuffer) < self.buffersize :
					print 'the last buffer, can not find a log header', len(self.logbuffer)
					return None
				else :
					assert(len(self.logbuffer) >= self.headsizelimit)
					startindex = max(len(self.logbuffer) - self.headsizelimit, self.currentbufferindex)
					tailbuffer = self.logbuffer[startindex :]
					assert(tailbuffer)
					self._readbuffer()
					if not self.logbuffer :
						return None

	def _pickTime(self, timestr) :
		time = timestr[0:4] + '-' + timestr[4:6] + '-' + timestr[6:8] + ' ' + timestr[9:11] + ':' + timestr[11:13] + ':' + timestr[13:15]
		return time
	
	def _pickLevel(self, levelstr) :
		return levelstr
	
	def _pickSource(self, sourcestr) :
		return self.modulename 
	
	def _pickMsg(self) :
		msg = ''
		if not self.logbuffer :
			print 'no more buffer, empty msg'
			return ''
		while self.logbuffer :
			match = self.headreg.search(self.logbuffer, self.currentbufferindex)
			if match :
				# next header found
				msg += self.logbuffer[self.currentbufferindex : match.start(0)]
				self._updatebufferindex(match.start(0))
				return msg
			else :
				msg += self.logbuffer[self.currentbufferindex : - self.headsizelimit]
				startindex = max(len(self.logbuffer) - self.headsizelimit, self.currentbufferindex)
				tailbuffer = self.logbuffer[startindex :]
				self._readbuffer()
				if not self.logbuffer : 
					msg += tailbuffer
					print 'end of file'
					return msg
				else :
					self.logbuffer = tailbuffer + self.logbuffer

	def sendToCollector(self, logitem) :
		self.logcollector.pushLog(logitem.time, logitem.level, logitem.source, logitem.msg)	

	def pickAll(self) :
		now = time.clock()
		logitem = self.pickOnelog()
		while logitem:
			self.sendToCollector(logitem)
			logitem = self.pickOnelog()

		self.logcollector.shutdown()
		print 'pick all cost...', time.clock() - now




def class FileDesc :
	filepath = ""
	modulename = ""
	encoding = "utf8"

def pickFile(filedesc, lc):

	lp = LogPicker(filedesc.filepath, filedesc.modulename, lc, filedesc.encoding)
	lp.pickAll()



#example
#lc = logcollector.LogCollector()
#lp = LogPicker("gameserver_2013_07_15_22_00_16.log", "gameserver", lc)
#lp.pickAll()
			


