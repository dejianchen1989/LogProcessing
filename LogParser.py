# -*- coding: utf-8 -*-

import os
import re
import json
import sys
import threading
import mysql.connector
from LogDBInterface import store_record, init_DB_interface, close_DB_interface

LogDir = os.path.join(os.getcwd(), 'logs')      # 日志目录
LogNameExp = r'^log\_\d+.txt$'                  # 日志名称的正则表达式
TypeFile = 'TypeDefinition.json'                # 日志类型定义文件
TypeObject = None                               # 类型对象

NumberTypes = [type(1), type(1.0)]              # 数值类型
StringTypes = [type(''), type(u'')]             # 字符串类型
ListTypes = [type([])]                          # 列表类型

# 以下是用于多线程控制的变量
NumThreads = 2                                  # 线程数目
Lock = threading.Lock()                         # 互斥锁，用于线程同步
LogFiles = []                                   # 日志文件列表
CurrentFileNum = 0                              # 当前解析的日志序号
TotalRecordNum = 0                              # 所有解析的记录数目


reload(sys)
sys.setdefaultencoding('utf-8')


# 读取log文件夹下的所有日志，并解析存入数据库
def parse_logs():
    global LogFiles, TypeObject, NumThreads, TotalRecordNum
    reset_status()
    files = os.listdir(LogDir+'/.')
    LogFiles = [f for f in files if re.search(LogNameExp, f)]
    if len(LogFiles) == 0:
        print 'No log file is found!'
        return
    if TypeObject is None:
        with open(TypeFile, 'r') as f:
            type_str = f.read()
            TypeObject = json.loads(type_str)
    init_DB_interface()
    threads = []
    for i in range(NumThreads):
        t = threading.Thread(target=parse_worker, name='Parse_Worker_'+str(i))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    close_DB_interface()
    print '>>> Thread Numbers: ' + str(NumThreads)
    print '>>> Total parsed and stored records: ' + str(TotalRecordNum)


# 解析工作流，可以用于多线程
def parse_worker():
    global CurrentFileNum, LogFiles
    while True:
        file_name = ''
        Lock.acquire()
        try:
            if CurrentFileNum >= len(LogFiles):
                return
            else:
                file_name = LogFiles[CurrentFileNum]
                CurrentFileNum += 1
        finally:
            Lock.release()
        parse_a_log(file_name)


# 解析单个日志文件
def parse_a_log(log_name):
    global TotalRecordNum
    log_url = os.path.join(LogDir, log_name)
    i = 0
    with open(log_url, 'r') as f:
        for line in f.readlines():
            parse_record(line.strip())
            i += 1
    print '>>> ' + log_name + ' parsed and stored! Record number: ' + str(i)
    TotalRecordNum += i


# 解析单个记录
def parse_record(record_str):
    m = re.match(r'\[(.*)\]\[([A-Za-z0-9_]*)\](.*)', record_str)
    log_time = m.group(1)
    log_type = m.group(2)
    log_content = json.loads(m.group(3))
    attr_list = [a for a in log_content]
    log_format = TypeObject[log_type]
    result = {'LogTime': log_time, 'LogType': log_type}
    for attr in log_format:
        # 对于数值类型
        if log_format[attr][0] == 'NUMBER':
            result[attr] = -1      # 异常的、缺失的数值设置为-1
            if attr in attr_list:
                if type(log_content[attr]) in NumberTypes:      # 正常匹配数值
                    result[attr] = log_content[attr]
                elif type(log_content[attr]) in StringTypes:    # 整型字符串
                    if re.match(r'\d+', log_content[attr]):
                        result[attr] = int(log_content[attr])
                    elif re.match(r'\d+(\.+d+)?', log_content[attr]):   # 浮点型字符串
                        result[attr] = float(log_content[attr])
        # 对于字符串类型
        elif log_format[attr][0] == 'STRING':
            result[attr] = ''      # 缺失的字符串设置为空串
            if attr in attr_list:
                if type(log_content[attr]) in StringTypes:      # 正常匹配字符串
                    result[attr] = log_content[attr]
                elif type(log_content[attr]) in ListTypes:      # 列表类型字符串(多值列表)
                    for l in log_content[attr]:
                        if len(result[attr]) > 0:
                            result[attr] += ' '
                        result[attr] += str(l)
                else:
                    result[attr] = str(log_content[attr])       # 其他类型，转换成字符串
        else:
            raise StandardError('Error data type when parse record')
        if attr in [a for a in log_content]:
            del log_content[attr]

    # 有多余项
    other_info = ''
    if len(log_content) > 0:
        for attr in log_content:
            if len(other_info) > 0:
                other_info += ', '
            if type(log_content[attr]) in ListTypes:
                other_info += attr + ':'
                for l in log_content[attr]:
                        other_info += str(l) + ' '
                other_info = other_info.strip()
            else:
                other_info += attr + ':' + str(log_content[attr])
    result['LogOtherInfo'] = other_info
    store_record(result)


# 重设状态
def reset_status():
    global LogFiles, CurrentFileNum, TotalRecordNum
    LogFiles = []
    CurrentFileNum = 0
    TotalRecordNum = 0
