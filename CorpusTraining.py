# -*- coding: utf-8 -*-

import os
import re
import random
import sys
import cStringIO

DefinedAttributes = ['hero', 'item', 'where', 'who']    # 已定义的属性

# 以下是语料词典，chs为中文，eng为英文
hero_chs = None
hero_eng = None
item_chs = None
item_eng = None
other_chs = None
other_eng = None
where_chs = None
where_eng = None
who_chs = None
who_eng = None

reload(sys)
sys.setdefaultencoding('utf-8')


# 从语料库中产生随机属性的值
def get(attr):
    if attr == 'hero':
        return get_hero()
    elif attr == 'item':
        return get_item()
    elif attr == 'where':
        return get_where()
    elif attr == 'who':
        return get_who()
    elif attr == 'other':
        return get_other()
    elif attr == 'random':
        return get_other(random.randint(2,5))
    else:
        raise StandardError('Unexpected attribute argument in function \'get\'')


# 从语料库中选择hero的值
def get_hero(nwords=1):
    global hero_chs, hero_eng
    if hero_chs is None or hero_eng is None:
        hero_eng = load_dictionary('hero', 'eng')
        hero_chs = load_dictionary('hero', 'chs')
    return get_random_words(nwords, hero_chs, hero_eng)


# 从语料库中选择item的值
def get_item(nwords=1):
    global item_chs, item_eng
    if item_chs is None or item_eng is None:
        item_eng = load_dictionary('item', 'eng')
        item_chs = load_dictionary('item', 'chs')
    return get_random_words(nwords, item_chs, item_eng)


# 从语料库中选择其他属性的值
def get_other(nwords=1):
    global other_chs, other_eng
    if other_chs is None or other_eng is None:
        other_chs = load_dictionary('other', 'eng')
        other_eng = load_dictionary('other', 'chs')
    return get_random_words(nwords, other_chs, other_eng)


# 从语料库中选择where的值
def get_where(nwords=1):
    global where_chs, where_eng
    if where_chs is None or where_eng is None:
        where_chs = load_dictionary('where', 'eng')
        where_eng = load_dictionary('where', 'chs')
    return get_random_words(nwords, where_chs, where_eng)


# 从语料库中选择who的值
def get_who(nwords=1):
    global who_chs, who_eng
    if who_chs is None or who_eng is None:
        who_chs = load_dictionary('who', 'eng')
        who_eng = load_dictionary('who', 'chs')
    return get_random_words(nwords, who_chs, who_eng)


# 从语料库文件中建立字典
def load_dictionary(attr_name, lang):
    attr_dir = os.path.join('.', 'corpus', attr_name)
    files = os.listdir(attr_dir)
    if lang == 'chs':
        files = [f for f in files if re.search(r'^chs', f)]
    elif lang == 'eng':
        files = [f for f in files if re.search(r'^eng', f)]
    else:
        raise StandardError('invalid language type in function load_dictionary: %s' % lang)
    result_list = []
    for f in files:
        with open(os.path.join(attr_dir, f), 'r') as fs:
            result_list = fs.read().decode('GBK').split('\n')
    return result_list


# 从字典中（中文或英文），选择nwords个随机单词
def get_random_words(nwords, chs_list, eng_list):
    nwords = min(20, nwords)
    nwords = max(0, nwords)
    rand_lang = random.randint(0,1)
    if rand_lang == 0:
        list_len = len(chs_list)
        use_list = chs_list
    else:
        list_len = len(eng_list)
        use_list = eng_list
    sentence = ''
    for n in range(nwords):
        if n == 0:
            sentence = use_list[random.randint(0, list_len-1)]
        else:
            sentence = sentence + ' ' + use_list[random.randint(0, list_len-1)]
    return sentence
