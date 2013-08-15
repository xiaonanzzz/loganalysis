#encoding=utf8
mysqluser = "root"
mysqlpassword = "Free789123"

mysqltablename = "happylog"
mysqlcreatetable = "create table if not exists " + mysqltablename + "( \
                    logid int not null AUTO_INCREMENT,\
                    time datetime not null,\
					level varchar(255) not null,\
                    source varchar(255) not null,\
                    msg text not null,\
                    primary key(logid)\
                    );"

import MySQLdb
import sys

def connecthappylog():
    db = MySQLdb.connect(host = "localhost", user = mysqluser, passwd = mysqlpassword, db='test', charset="utf8")
    print("connecting to happy log database...", db)
    cur = db.cursor()
    cur.execute(mysqlcreatetable)
    return db

def sqlpreproc(sql):
    return sql
    
def addlog(db, time, level, source, msg) :
    cur = db.cursor()
    sqlstr = "insert into " + mysqltablename + "(time, level, source, msg) values (%s,%s,%s,%s );"
    try :
        cur.execute(sqlstr, (time, level, source, msg))
    except Exception, e:
		print e
		raise 'execute sql failed'
		
		






class LogCollector :
	loglist = []
	db = None
	def __init__(self) :
		self.startup()
		return 

	def startup(self) :
		self.shutdown()
		self.db = connecthappylog()

	def pushLog(self, time, level, source, msg) :
		self.loglist.append((time, level, source, msg))
		if len(self.loglist) >= 100 :
			self._writeLog()

	def _writeLog(self) :
		for log in self.loglist :
			time, level, source, msg = log
			addlog(self.db, time, level, source, msg)
		self.loglist = []
		if not self.db :
			raise 'collector not start up'
		self.db.commit()

	def shutdown(self) :
		if self.db :
			self._writeLog()
			self.db.close()
			self.db = None
	
	def __del__(self) :
		self.shutdown()


	
