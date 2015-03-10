# -*- coding: utf-8 -*-

import time
from LogGenerator import gen_logs
from LogParser import parse_logs
from LogDBInterface import clear_all_tables, show_total_record_numbers

def main():
	while True:
		print ''
		print '*** Welcome to Log Processor. Please enter to select: ***' 
		print ' 1:      Generate Logs'
		print ' 2:      Parse Logs and Store into Database'
		print ' 3:      Show number of total records in Database'
		print ' 4:      Clear Database'
		print ' Others: Quit'
		order = raw_input(' Choose: ')

		# 生成日志
		if order == '1':
			start_time = time.time()
			gen_logs(20)
			end_time = time.time()
			time_interval = round(end_time - start_time, 3)
			print '>>> Total time cost (seconds): ' + str(time_interval)
			pass
		# 解析存储日志
		elif order == '2':
			start_time = time.time()
			parse_logs()
			end_time = time.time()
			time_interval = round(end_time - start_time, 3)
			print '>>> Total time cost (seconds): ' + str(time_interval)
		# 显示数据库中所有记录总个数
		elif order == '3':
			show_total_record_numbers()
		# 清空数据库中的tables
		elif order == '4':
			clear_all_tables()
		else:
			return
	pass


main()