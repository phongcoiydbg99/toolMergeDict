import re
import json
import os, sys, mmap
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import time

# class Person:
#     def __init__(self, name, age):
#         self.name = name
#         self.age = age
#
# s = {"a b c" : 5}
# b = [Person("a b c",5),Person("a b c",5)]
# print(s.__sizeof__())
# print(b[0].name)

# a_file = open("DicTest1.txt", "r")
# list_of_lines = a_file.readlines()
# list_of_lines[1] = "Line2\n"
#
# a_file = open("DicTest1.txt", "w")
# a_file.writelines(list_of_lines)
# a_file.close()

# with open("text4.txt", "r") as file:
#     d = file.read()
# for fileName in os.listdir("/media/phongnve/HDD/source_bkav/toolDictV2/dictionary"):
#     print(fileName)
# def _read_dic_to_json(name):
#     with open(name) as json_file:
#         return json.load(json_file)
#
# def _write_unigram(unigram_dict, name):
#     f = open(name, "w")
#     f.write("Ádasd")
#     # Write unigram
#     for x in unigram_dict.values():
#         f.write("Ádasd")
#         # f.write(dumpUnigram(x)+"\n")
#     f.close()
# with open('dictionary/union.txt', 'w+') as uni_file:
#     uni_file.write("asdasd")
# _write_unigram({}, 'dictionary/unigram_pro.txt')

print("Halo")
# readFile._file_to_csv(r'E:\Python\test.txt.txt')
# df = pd.read_csv('/media/phongnve/HDD/source_bkav/dictionary_csv/test_dic_csv.csv')
# print(df.iloc[:10])
# key,f,count,type,prev_array
# col_Names=["key", "f", "count", "type", "prev_array"]
# df = pd.read_csv("/media/phongnve/HDD/source_bkav/dictionary_csv/test_dic_csv.csv",names=col_Names)
# df.set_index('key', inplace=True)
# try:
#     print(df.loc["Àf",])
# except:
#     print("No index")
# print(df.iloc[:10])
# Size KB
chunkSize = 1000
# # st = 'Ngon'
# i = 0
# for chunk in pd.read_csv('/media/phongnve/HDD/source_bkav/dictionary_csv/test_dic_csv.csv', chunksize= chunkSize):
#     chunk.to_csv('/media/phongnve/HDD/source_bkav/dictionary_csv/test_dic_csv_{}.csv'.format(i), index=False, header=False)
#     i = i + 1
st = ['Ngon', 'nghe', 'Ngủ']
# st = ['nghe', 'Ngủ']
for x in st:
    t = time.time()
    col_Names=["key", "f", "count", "type", "prev_array"]
    chunk = pd.read_csv('test_dic_csv.csv', names=col_Names)
    chunk.set_index('key', inplace=True)
    try:
        dk = chunk.loc[x,]
        check = True
    except:
        check = False
    print(check)
    if check == True:
        # dk = chunk.loc[x,]
        chunk.loc[x,"f"] = -100
        print(chunk.shape[0])
        # chunk.loc[x,] = dk
        chunk.to_csv('test_dic_csv.csv', header=False)
    else:
        test = {'key' : [x], 'f': [-1], 'count':[2], 'type':[0], 'prev_array':[""]}
        print(test)
        df = pd.DataFrame(test)
        df.to_csv('test_dic_csv_0.csv', mode='a', index=False, header=False)
    # dk = chunk[chunk['key'] == x]
    # dk.loc['f'] = -100
    # x = dk.iloc[0]
    # x['f'] = -100
    # # print(x)
    # chunk.iloc[dk.index[0]] = x
    print(time.time() - t)

# print(df.iloc[100])