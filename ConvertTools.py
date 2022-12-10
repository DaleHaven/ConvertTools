# coding:utf-8
"""

Copyright [2023] [DaleHaven]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Name    : ConvertTools.py
Author  : DaleHaven
Contect : xuepeng@88.com
Time    : 2022年11月20日 15点26分
Version : Python 3.6
CodeVersion :
    2022年11月26日 11点07分 0.0.1 ↓
        1. 增加了严谨模式,用于在json转csv时将列中内容所有英文逗号替换成中文,使其在分割列时正常显示.
        2. 将日志输出更改为中文,便于使用者阅读.
    2022年12月4日 14点17分 0.0.2 ↓
        1. 增加了严谨与不严谨模式的高性能和正常性能模式支持.
        2. 提升用户交互体验
Thank List  :
        1. 魏义硕(贡献参与测试与提出改进交互方式并贡献代码)
Desc    :
1. 请确保安装了Python 3.X
2. 运行时请参照如下模板
python3 ConvertTools.py C:/here/is/your/json/path.json json转csv 严谨模式 高性能模式 D:/here/is/your/convert/result.csv
注意点:
1. 结果路径不存在则自动创建,已存在则追加写入文件
"""
import json
import sys
import os
import logging
import multiprocessing

def strictModelAndPowerfulJsonToCsvWorker(yourJobList, resultSavePath,subProcessStatus):
    # 结果缓冲器
    resultBuffer = []
    # 损坏行记录器
    damagedCount = 0
    for aLine in yourJobList:
        try:
            currentLineValues = json.loads(aLine).values()
            resultBuffer.append(','.join(map(lambda x,: str(x).replace(',', '，'), currentLineValues)) + '\n')
        except Exception as e:
            damagedCount += 1
    # 将结果保存到本地
    with open(resultSavePath, "a",encoding='utf8') as f:
        f.writelines(resultBuffer) # TODO:可能会出现脏写Bug,目前尚未遇到过
    f.close()
    subProcessStatus['brokenCount'] += damagedCount


def unstrictModelAndPowerfulJsonToCsvWorker(yourJobList, resultSavePath, subProcessStatus):
    # 结果缓冲器
    resultBuffer = []
    # 损坏行记录器
    damagedCount = 0
    for aLine in yourJobList:
        try:
            currentLineValues = json.loads(aLine).values()
            resultBuffer.append(','.join(map(lambda x,: str(x), currentLineValues)) + '\n')
        except Exception as e:
            damagedCount += 1
    # 将结果保存到本地
    with open(resultSavePath, "a",encoding='utf8') as f:
        f.writelines(resultBuffer)
    f.close()
    subProcessStatus['brokenCount'] += damagedCount

def splitListToSubList(origin_list, n):
    if len(origin_list) % n == 0:
        cnt = len(origin_list) // n
    else:
        cnt = len(origin_list) // n + 1

    for i in range(0, n):
        yield origin_list[i * cnt:(i + 1) * cnt]

def strictModelAndNormalPerformanceJsonToCsv(lines, resultSavePath):
    resultBuffer = []
    headers = json.loads(lines[0]).keys()
    resultBuffer.append(','.join(headers) + '\n')
    logging.info('\tCSV表头创建完成,它们是:' + str(resultBuffer).replace('\\n', ''))

    # file status controls
    passedCount: int = 1
    errorLineIndices = []
    logging.info('\t转换操作开始.')
    for line in lines:
        try:
            currentLineValues = json.loads(line).values()
            if len(headers) == len(currentLineValues):
                resultBuffer.append(','.join(map(lambda x,: str(x).replace(',', '，'), currentLineValues)) + '\n')
            else:
                errorLineIndices.append(passedCount)
            passedCount += 1
        except Exception as e:
            errorLineIndices.append(passedCount)
            passedCount += 1
            continue
    logging.info('\t转换完成.')
    # create result file
    os.makedirs(os.path.dirname(resultSavePath), exist_ok=True)
    with open(resultSavePath, "a",encoding='utf8') as f:
        f.writelines(resultBuffer)
    logging.info('\t文件写入完成.')
    if len(errorLineIndices) > 0:
        logging.error('\t这些行JSON不能够转换,它们的文件坐标行号为 '+str(errorLineIndices)+'.')


def strictModelAndPowerfulJsonToCsv(lines, savePath):
    # 获得当前机器总CPU核心数量,并除以二+1 -> P1
    cpuCounts = int(os.cpu_count() / 2) + 1
    # 对数据进行分组
    arraySplits = list(splitListToSubList(lines, cpuCounts))
    # 创建P1个工作子进程
    processList = []
    # 生成结果保存路径,并创建绝对路径
    os.makedirs(os.path.dirname(savePath),exist_ok=True)
    # 写入表头
    headers = json.loads(lines[0]).keys()
    with open(savePath,mode='a',encoding='utf8') as  csvHeaderWriter:
        csvHeaderWriter.write(','.join(headers)+'\n')
    csvHeaderWriter.close()
    logging.info('\tCSV表头创建完成,它们是:' + str(headers).replace('\\n', ''))

    # 子进程状态记录器
    manager = multiprocessing.Manager()
    subProcessStatus = manager.dict()
    subProcessStatus['brokenCount'] = 0
    # 创建子进程
    for index in range(0, cpuCounts):
        yourJobList = arraySplits[index]
        aProcess = multiprocessing.Process(target=strictModelAndPowerfulJsonToCsvWorker,args=(yourJobList, savePath ,subProcessStatus,))
        # 启动进程
        aProcess.start()
        processList.append(aProcess)
    # 堵塞主进程,等待子进程处理结束
    for aProc in processList:
        aProc.join()
    # 打印子进程在处理时出现的坏行数量.
    subBrokenCount = subProcessStatus['brokenCount']
    if subBrokenCount > 0:
        logging.warning('\t共计有:'+str(subBrokenCount)+'条无法解析的损坏行,已自动执行跳过.')

def unstrictModelAndPowerfulJsonToCsv(lines, savePath):
    # 获得当前机器总CPU核心数量,并除以二+1 -> P1
    cpuCounts = int(os.cpu_count() / 2) + 1
    # 对数据进行分组
    arraySplits = list(splitListToSubList(lines, cpuCounts))
    # 创建P1个工作子进程
    processList = []
    # 生成结果保存路径,并创建绝对路径
    os.makedirs(os.path.dirname(savePath),exist_ok=True)
    # 写入表头
    headers = json.loads(lines[0]).keys()
    with open(savePath,mode='a',encoding='utf8') as  csvHeaderWriter:
        csvHeaderWriter.write(','.join(headers)+'\n')
    csvHeaderWriter.close()
    logging.info('\tCSV表头创建完成,它们是:' + str(headers).replace('\\n', ''))
    # 子进程状态记录器
    manager = multiprocessing.Manager()
    subProcessStatus = manager.dict()
    subProcessStatus['brokenCount'] = 0

    for index in range(0, cpuCounts):
        yourJobList = arraySplits[index]
        aProcess = multiprocessing.Process(target=unstrictModelAndPowerfulJsonToCsvWorker,args=(yourJobList, savePath, subProcessStatus))
        # 启动进程
        aProcess.start()
        processList.append(aProcess)
    # 堵塞主进程,等待子进程处理结束
    for aProc in processList:
        aProc.join()
    # 打印子进程在处理时出现的坏行数量.
    subBrokenCount = subProcessStatus['brokenCount']
    if subBrokenCount > 0:
        logging.warning('共计有:'+str(subBrokenCount)+'条无法解析的损坏行,已自动执行跳过.')

def unstrictModelAndNormalPerformanceJsonToCsv(lines, resultSavePath):
    resultBuffer = []
    headers = json.loads(lines[0]).keys()
    resultBuffer.append(','.join(headers) + '\n')
    logging.info('\tCSV表头创建完成,它们是:' + str(resultBuffer).replace('\\n', ''))

    # file status controls
    passedCount: int = 1
    errorLineIndices = []
    logging.info('\t转换操作即将开始.')

    for line in lines:
        try:
            currentLineValues = json.loads(line).values()
            if len(headers) == len(currentLineValues):
                resultBuffer.append(','.join(map(lambda x,: str(x), currentLineValues)) + '\n')
            else:
                errorLineIndices.append(passedCount)
            passedCount += 1
        except Exception as e:
            errorLineIndices.append(passedCount)
            passedCount += 1
            continue

    # create result file
    os.makedirs(os.path.dirname(resultSavePath), exist_ok=True)
    logging.info('\t转换完成.')
    with open(resultSavePath, "a",encoding='utf8') as f:
        f.writelines(resultBuffer)
    logging.info('\t文件写入完成.')
    if len(errorLineIndices) > 0:
        logging.error('\t这些行JSON不能够转换,它们的文件坐标行号为 '+str(errorLineIndices)+'.')

def help():
    print("""
    *
    **
    ***
    ****
    ***************************************************************************************************************************
    *                                               帮助指南                                                                   *
    * 调用模板如下:                                                                                                             *
    * python3 ConvertTools.py C:/here/is/your/json/path.json json转csv 严谨模式 高性能模式 D:/here/is/your/convert/result.csv     *
    *                                                                                                                         *
    * 可选项:                                                                                                                  *
    * 1.    严谨模式   可   更改为  不严谨模式                                                                                     *
    * 2.    高性能模式  可   更改为  正常性能模式                                                                                  *
    *                                                                                                                        *
    * 使用必读:                                                                                                                *
    * 1.    当转换文件较大时,请使用 高性能模式 ,小文件使用此模式将会适得其反.                                                           *
    * 2.    严谨模式在JSON转换CSV时会将每个列内容中的英文逗号转为中文逗号,防止出现CSV列叉行,不严谨模式则不会做任何处理.                        *
    * 3.    调用参数以空格分割,不要输错了字,建议直接拷贝,然后替换路径                                                                  *
    * 4.    结果路径不存在则自动创建,已存在则追加写入文件(重复调用请手动删除)                                                            *
    ***************************************************************************************************************************
    ****
    ***
    **
    *
    """)

if __name__ == '__main__':

    # init logging
    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)
    help()

    sourceDataPath = os.path.normpath(input("请输入被转换文件(绝对路径)："))
    ModelList = ['严谨模式','不严谨模式','高性能模式','正常性能模式']
    ModelNum = int(input("请选择模式(0.严谨模式/1.不严谨模式): "))
    convertModel = "json转csv"
    convertType = ModelList[ModelNum]
    ModelNum = int(input("请选择模式(2.高性能模式/3.正常性能模式): "))
    convertHighPerformance = ModelList[ModelNum]
    resultSavePath = os.path.normpath(input("请输入转换后目录及文件名(绝对路径)："))
    jsonReader = open(sourceDataPath, 'r',encoding='utf8')
    if os.path.exists(sourceDataPath) is not True:
        logging.error('JSON 文件 '+sourceDataPath+' 不存在.')
        sys.exit()


    jsonReader = open(sourceDataPath, 'r',encoding='utf8')
    if os.path.exists(sourceDataPath) is not True:
        logging.error('JSON 文件 '+sourceDataPath+' 不存在.')
        sys.exit()

    # 文件内容初始化
    lines = jsonReader.readlines()
    logging.info('文件加载完成,共有' + str(len(lines)) + '行要进行转换.')

    if convertModel.lower() == 'json转csv':
        try:
            if convertType == '严谨模式' and convertHighPerformance == '高性能模式':
                logging.info('开启 严谨模式 和 高性能模式 处理.')
                strictModelAndPowerfulJsonToCsv(lines,resultSavePath) # checked
                logging.info('完成 严谨模式 和 高性能模式 处理.')
            elif convertType == '严谨模式' and convertHighPerformance == '正常性能模式':
                logging.info('开启 严谨模式 和 正常性能模式 处理.')
                strictModelAndNormalPerformanceJsonToCsv(lines,resultSavePath)
                logging.info('完成 严谨模式 和 正常性能模式 处理.')
            elif convertType == '不严谨模式' and convertHighPerformance == '高性能模式':
                logging.info('开启 不严谨模式 和 高性能模式 处理.')
                unstrictModelAndPowerfulJsonToCsv(lines,resultSavePath)
                logging.info('完成 不严谨模式 和 高性能模式 处理.')
            elif convertType == '不严谨模式' and convertHighPerformance == '正常性能模式':
                logging.info('开启 不严谨模式 和 正常性能模式 处理.')
                unstrictModelAndNormalPerformanceJsonToCsv(lines, resultSavePath)
                logging.info('完成 不严谨模式 和 正常性能模式 处理.')
            else:
                help()
                raise Exception('不支持的模式.')

        except Exception as e:
            logging.error(e)
        finally:
            jsonReader.close()