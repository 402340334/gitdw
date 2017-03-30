#coding: gb2312
import pandas as pd
import chardet
import json, re
import sys


# ��ǰ�����ļ�·����json�����ļ�·��
global chainFilePath, jsonFilePath

# ��ǰ�ļ�����
global chainFile, jsonFile

# ��ǰ�ļ����ݶ�άdataframe����
global data

# �ı��ж�Ӧ���б�
global columns

# ��������ƥ��
global dateMatch

# ��ʼ����Ҫ���ݣ�Ϊ����check������������׼��
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

# �ж������е��ֶ����Ƿ����Ҫ�����������ܽ����س�dataframe
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
        print '���ݼ��سɹ�'
    else:
        print '����ĳ�в����� Ҫ��ļ�¼'
    print 'loadDataFromFile() end'


# ���ļ�������м��
def checkFileEncoding():
    '''
            ���ļ�������м��
    '''
    global chainFile
    encoding = chardet.detect(chainFile.read())['encoding']
    if encoding in ['GBK', 'GB2312']:
        print '������ȷ'
    else:
        print '������󣬵�ǰ����Ϊ��', encoding

# ���ļ���¼�������м��
def checkNumberOfLines():
    global data
    lineTag = data['meid'][0]
    if int(lineTag) == int(len(data) - 2):
        print '�ı����ȱ�ʶ��ȷ'
    else:
        print '�ı����ȱ�ʶ����'

# ���ı���ʽ˵�����ֽ��м��
def checkFileDec():
    global jsonFile
    fileDeclare = data['meid'][1]
    columnsTitle = jsonFile['columns_title']
    #print fileDeclare
    #print jsonFile['columns_title']
    if fileDeclare == columnsTitle:
        print '�ļ���ʽ����У����ȷ'
    else:
        print '�ļ���ʽ����У�����'

# ���ı���ʽ�Ƿ񳬳����м��
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
            print '����Ϊ������Ϣ'

    print 'checkColumnLength() end'

# �������ֶ����ݽ��м��
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
    
# ��ĳ���ֶ����Ƿ����Ҫ����м��
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

