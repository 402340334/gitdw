#coding: gb2312
import pandas as pd
import chardet
import json, re
import sys


# 当前数据文件路径，json配置文件路径
global chainFilePath, jsonFilePath

# 当前文件对象
global chainFile, jsonFile

# 当前文件数据二维dataframe对象
global data

# 文本列对应的列表
global columns

# 日期正则匹配
global dateMatch

# 初始化必要数据，为后续check操作进行数据准备
def init_config_info():
    global chainFilePath, chainFile, data, columns, jsonFilePath, jsonFile,dateMatch 
    columns = ['meid','phone','num','province','city','brand','product_id','product_cd','create_time']
    chainFilePath = 'E:/Practice/eclipse_workspace/ETLChainFileChecker/file/reg_info.txt'
    jsonFilePath = 'E:/Practice/eclipse_workspace/ETLChainFileChecker/file/src_file_config.json'
    jsonFile = json.load(open(jsonFilePath))
    
    dateMatch = jsonFile['date_match']
    
    #print jsonFile['columns_title']
    #print data
    #print data.ix[2:]['meid']
    #print 'show :', max(data.ix[2:]['meid'].str.len())
    #pass

# 判断所有行的字段数是否符合要求，无问题后才能将加载成dataframe
def loadDataFromFile():
    global chainFilePath, chainFile, jsonFile, data
    columns = jsonFile['columns_title'].split(jsonFile['columns_title_split'])
    currentLine = 0
    isSuccess = True
    print chainFilePath
    chainFile = open(chainFilePath)
    print len(chainFile.readlines())
    print chainFile.readline()
    print chainFile.readline()
    print chainFile.readlines()
    for line in chainFile.readline():
        print line
        if currentLine <= 2:
            currentLine += 1
            continue
        cnt = len(line.split(','))
        print line
        print line.split(',')
        print 'columnsCnt=', len(columns), ':', 'columns=', cnt
        if len(columns) != cnt:
            print 'error currentLine=', currentLine, 'columns=', cnt
            isSuccess = False
            currentLine += 1
    if isSuccess:
        data = pd.read_table(chainFilePath,sep=',',names = columns, header=None,encoding='gb2312',engine = 'python')
        print '数据加载成功'
    else:
        print '存在某行不符合 要求的记录'
    print 'loadDataFromFile() end'


# 对文件编码进行检查
def checkFileEncoding():
    '''
            对文件编码进行检查
    '''
    global chainFile
    encoding = chardet.detect(chainFile.read())['encoding']
    if encoding in ['GBK', 'GB2312']:
        print '编码正确'
    else:
        print '编码错误，当前编码为：', encoding

# 对文件记录行数进行检查
def checkNumberOfLines():
    global data
    lineTag = data['meid'][0]
    if int(lineTag) == int(len(data) - 2):
        print '文本长度标识正确'
    else:
        print '文本长度标识错误'

# 对文本格式说明部分进行检查
def checkFileDec():
    global jsonFile
    fileDeclare = data['meid'][1]
    columnsTitle = jsonFile['columns_title']
    #print fileDeclare
    #print jsonFile['columns_title']
    if fileDeclare == columnsTitle:
        print '文件格式声明校验正确'
    else:
        print '文件格式声明校验错误'

# 对文本格式是否超长进行检查
def checkColumnLength():
    global jsonFile
    tableConfig = jsonFile['table_config']
    tableKeyList =  tableConfig.keys()
    for key in tableKeyList:
        reload(sys)
        sys.setdefaultencoding("gb2312")
        keyLenSeries = data.ix[2:][key].astype(str).str.len()
        #print keyLenSeries
        #print '-------------'
        #print keyLenSeries.max()
        #print tableConfig[key][1]
        #return
        if keyLenSeries.max() > tableConfig[key][1]:
            print key, ':', keyLenSeries.max(), ':', tableConfig[key][1]
            print '以上为超长信息'

    print 'checkColumnLength() end'

# 对日期字段数据进行检查
def checkDateFormat():
    global data, jsonFile, dateMatch
    tableConfig = jsonFile['table_config']
    print tableConfig.values()
    dateColumns = []
    for config in tableConfig:
        if tableConfig[config][0] == 'date':
            dateColumns.append(config)
    if len(dateColumns) == 0:
        return True
    print dateColumns
    for dateColumn in dateColumns:
        for i in data.ix[2:][dateColumn]:
            if not re.match(dateMatch, i):
                print 'error:', i

    print 'checkDateFormat() end'
    
# 对某行字段数是否符合要求进行检查
def checkDataColumnsCnt():
    global data, jsonFile
    columnsCnt = len(jsonFile['columns_title'].split(jsonFile['columns_title_split']))
    extraData = data.ix[2:, columnsCnt:]
    print extraData.columns.size
    if len(extraData) > 0:
        print 'over size: ', len(extraData)
    print 'checkDataColumnsCnt() end'





if __name__ == '__main__':
    print 'hello'
    init_config_info()
    #checkFileEncoding()
    #checkNumberOfLines()
    #checkFileDec()
    #checkColumnLength()
    #checkDateFormat()
    #checkDateFormat()
    #checkDataColumnsCnt()
    loadDataFromFile()
    '''
    var = '2017-03-22 00:00:08'
    if re.match('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-1][0-9]:[0-5][0-9]:[0-5][0-9]', var):
        print 'abc'
    print 'dddd'
    '''

