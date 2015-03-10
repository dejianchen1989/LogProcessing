# -*- coding:utf-8 -*-

import mysql.connector
import threading

DBTables = []				# 已在数据库中创建的tables
CLock = threading.Lock()	# 互斥锁，用于数据库访问的多线程同步
conn = None					# 数据库连接变量


# 初始化数据库接口
def init_DB_interface():
	global conn
	conn = mysql.connector.connect(user='root', password='password', database='test', use_unicode=True)


# 关闭数据库接口
def close_DB_interface():
	global conn
	conn.close()
	DBTables = []


# 存储单个记录
def store_record(record_object):
	global conn
	log_type = record_object['LogType']
	del record_object['LogType']
	# 如果所插入的表不存在，则创建新的表
	if log_type not in DBTables:
		create_tables(log_type, record_object)
		DBTables.append(log_type)
	pass
	# 产生插入的SQL语句
	attrs = [k for k in record_object]
	keys = ''
	values = ''
	for k in attrs:
		v = record_object[k]
		keys += '`' + k + '`,'
		if get_data_type(v) in ['int', 'float']:
			values += str(v) + ','
		else:	# 为字符值时，需加引号并转义
			values += '\'' + str(v).replace('\\', '\\\\') + '\','
	keys = keys.rstrip(',')
	values = values.rstrip(',')
	stmt = 'insert into ' + log_type + ' (' + keys + ') values (' + values + ')'
	# 执行SQL语句
	CLock.acquire()
	try:
		cursor = conn.cursor()
		cursor.execute(stmt)
		conn.commit()
		cursor.close()
	finally:
		CLock.release()


# 新建表
def create_tables(log_type, record_kv):
	CLock.acquire()
	try:
		cursor = conn.cursor()
		cursor.execute('show tables like \'' + log_type + '\'')
		if len(cursor.fetchall()) == 0:		#表还没创建
			stmt = 'create table ' + log_type + ' ('
			attrs = [k for k in record_kv]
			for k in attrs:
				v = record_kv[k]
				# 这里属性名要加`name`引用(非单引号)，否则when等会和mysql关键字冲突
				stmt += '  `' + k + '` ' + get_data_type(v, k) + ','
			stmt = stmt.rstrip(',')
			stmt += ') default charset=utf8'
			cursor.execute(stmt)
		cursor.close()
	finally:
		CLock.release()


# 获取数值在mysql数据库中的数据类型
def get_data_type(val, key = ''):
	if type(val) is type(1):
		if val > 1000000000:
			return 'bigint'
		else:
			return 'int'
	elif type(val) is type(1.0):
		return 'float'
	elif key == 'LogOtherInfo':
		return 'text'
	else:
		return 'varchar(100)'


# 清除所有的表
def clear_all_tables():
	init_DB_interface()
	CLock.acquire()
	try:
		cursor = conn.cursor()
		stmt = 'show tables'
		cursor.execute(stmt)
		tables = cursor.fetchall()
		for t in tables:
			stmt = 'drop table ' + t[0]
			cursor.execute(stmt)
		conn.commit()
		cursor.close()
	finally:
		CLock.release()
	close_DB_interface()


# 显示数据库中所有的记录数目
def show_total_record_numbers():
	init_DB_interface()
	CLock.acquire()
	try:
		cursor = conn.cursor()
		stmt = 'show tables'
		cursor.execute(stmt)
		tables = cursor.fetchall()
		num_records = 0
		for t in tables:
			stmt = 'select count(*) from ' + t[0]
			cursor.execute(stmt)
			num_records += cursor.fetchall()[0][0]
		print '>>> Total Record Numbers in DB: ' + str(num_records)
		conn.commit()
		cursor.close()
	finally:
		CLock.release()
	close_DB_interface()
