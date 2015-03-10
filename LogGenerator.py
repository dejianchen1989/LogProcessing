# -*- coding: utf-8 -*-

import os
import re
import json
import random
import time
import sys
from CorpusTraining import DefinedAttributes, get

LogDir = os.path.join(os.getcwd(), 'logs')      # 日志目录
LogNameExp = r'^log\_\d+.txt$'                  # 日志名称的正则表达式
TypeFile = 'TypeDefinition.json'                # 日志类型定义文件
TypeObject = None                               # 类型对象
TypeList = None                                 # 类型列表
RecordTime = None                               # 日志起始时间

reload(sys)
sys.setdefaultencoding('utf-8')

# 产生nlogs个日志文件
def gen_logs(nlogs):
    random.seed(20150218)
    if not os.path.exists(LogDir):
        os.mkdir(LogDir)
    files = os.listdir(LogDir+'/.')
    files = [f for f in files if re.search(LogNameExp, f)]
    if len(files) > 0:
        file_seq = [int(re.search(r'\d+', f).group()) for f in files]
        seq = max(file_seq)+1
    else:
        seq = 1
    nlogs = min(nlogs, 30)
    for i in range(0, nlogs):
        gen_new_file(seq+i)


# 生成一个新的日志文件，编号为seq，日志文件格式为：log_seq.txt
def gen_new_file(seq):
    # 设定日志开始时间
    global RecordTime
    start_time = "2015-2-1 00:00:00"
    time_array = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    RecordTime = int(time.mktime(time_array)) + seq*24*60*60
    # 生成日志
    log_name = 'log_'+str(seq).zfill(3)+'.txt'
    log_url = os.path.join(LogDir, log_name)
    with open(log_url, 'wb+') as f:
        # 大小为10M, 0.85调整系数是因为中文编码问题，导致实际文件大小与理论大小有差别
        file_size = 10*1024*1024*0.85
        written_size = 0
        while written_size < file_size:
            new_record = gen_record() + '\n'
            f.write(new_record)
            written_size += len(new_record)
    print '>>> ' + log_name + ' generated!'


# 生成一个随机记录，用于写入日志中
def gen_record():
    global TypeObject, TypeList
    if TypeObject is None or TypeList is None:
        with open(TypeFile, 'r') as f:
            type_str = f.read()
            TypeObject = json.loads(type_str)
            TypeList = [t for t in TypeObject]

    gen_type = TypeList[random.randint(0, len(TypeList)-1)]
    record_str = '[' + gen_next_time() + ']'+'[' + gen_type + ']'

    attr_list = TypeObject[gen_type]
    record_obj = {}
    for attr in attr_list:
        attr_info = attr_list[attr]
        val = None
        if attr_info[0] == 'STRING':
            # 第一类属性 枚举型字符串
            if len(attr_info) >= 2:
                enums = [e for e in attr_info[1]]
                val = gen_unstable_string(attr, enums)
            # 第二类属性 任意字符串
            else:
                val = gen_unstable_string(attr)
        elif attr_info[0] == 'NUMBER':
            # 第三类属性 枚举型数值
            if len(attr_info) >= 2:
                enums = [int(e) for e in attr_info[1]]
                val = gen_unstable_number(attr, enums)
            # 第四类属性 任意数值
            else:
                val = gen_unstable_number(attr)
        else:
            raise StandardError('Unexpected attribute type!')

        if val is not None:
            record_obj[attr] = val

    new_attr = gen_unusual_attr(attr_list)
    while new_attr is not None:
        record_obj[new_attr['key']] = new_attr['val']
        new_attr = gen_unusual_attr(attr_list)

    record_str += json.dumps(record_obj, ensure_ascii=False)
    return record_str


# 生成不稳定的数值，增加日志复杂性
def gen_unstable_number(attr, enum_list=[]):
    if attr == 'when':
        global RecordTime
        return RecordTime
    floor, ceiling = 1, 10000
    if len(enum_list) > 0:
        floor = min(enum_list)
        ceiling = max(enum_list)
    indicator = random.randint(1,100)
    # 1% 概率出现空字符
    if indicator == 100:
        return ''
    # 1% 概率为字符串形式(数值型)
    elif indicator == 99:
        return str(random.randint(floor, ceiling))
    # 1% 概率为非数值型字符串
    elif indicator == 98:
        return get('other')
    # 1% 概率数值不在指定范围内
    elif indicator == 97:
        return random.randint(ceiling+1, 100000)
    # 1% 概率为空项
    elif indicator == 96:
        return None
    # 95% 概率为正确
    else:
        return random.randint(floor, ceiling)
    pass


# 生成不稳定的字符串，增加日志复杂性
def gen_unstable_string(attr, enum_list=[]):
    global DefinedAttributes
    indicator = random.randint(1,100)
    # 1% 概率出现空字符
    if indicator == 100:
        return ''
    # 1% 概率为数值型
    elif indicator == 99:
        return random.randint(1, 100)
    # 1% 概率为随机字符串
    elif indicator == 98:
        return get('random')
    # 1% 概率为空项
    elif indicator == 97:
        return None
    # 3% 概率出现带标点符号的随机字符
    elif indicator <= 96 and indicator >= 94:
        signs = [',', '.', '\\', '*', '@', '\n', '\r', '\n\r']
        new_str = get('other') + signs[random.randint(0, len(signs)-1)] + get('other')
        return new_str
    # 93% 概率为正确
    else:
        # 枚举类型
        if len(enum_list) > 0:
            return enum_list[random.randint(0, len(enum_list)-1)]
        # 非枚举类型, 在语料库中定义的属性
        elif attr in DefinedAttributes:
            return get(attr)
        # 非枚举类型, 未在语料库中定义的属性
        else:
            return get('other')
    pass


# 生成不稳定的属性（日志中没有定义的属性），增加日志复杂性
def gen_unusual_attr(attr_list):
    indicator = random.randint(1, 30)
    r_list = dict()
    # 1/30 概率产生多余项
    if indicator == 30:
        r_list['key'] = get('other')
        r_list['val'] = get('random')
        return r_list
    # 1/30 概率产生多余项,并且具有多值
    elif indicator == 29:
        r_list['key'] = get('other')
        r_list['val'] = [get('other'), get('other'), get('other')]
        return r_list
    else:
        return None
    pass


# 产生下一个日志时间
def gen_next_time():
    global RecordTime
    if random.randint(0, 1) == 1:
        RecordTime += 1
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(RecordTime))
