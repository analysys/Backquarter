import sys
import os
import datetime as dt
import time

print dt.datetime.now()
currentTimestamp = dt.datetime.strftime(dt.datetime.now(),"%m%d%H%M%S")
snappyPostfix = "tmp_nappy"
checkSnappyPostfix = "tmp_check_nappy"

if(len(sys.argv)!=4):
    print "python snappy_file.py [sourceFilePath]  [targetFilePath]  [c|d]"
    print "c compress \t  d decompress"
    sys.exit()

def intFileSizeThreshold(fileMap): 
    fileMap["h5jssdk"]=1024*200
    fileMap["app_2_0_HDFS"]=1024*1024*10
    fileMap["default"]=1024*1024*500

def getFileSizeThreshold(fileMap,str1):
    count1= str1.find('.')
    queue1=str1[0:count1]
    threshold=0
    day=""
    if(str1.find(".")>0):
        day=queue1[queue1.rfind("_")+1:len(queue1)]
        isday=day.isdigit()
        if (isday == True and len(day) == 8):
            queue1=queue1[:queue1.rfind("_")]
    if(fileMap.has_key("%s"%(queue1))==False):    
        threshold=fileMap["default"]
    else:
        threshold=fileMap[queue1]
    return threshold

def getFileLineCntFromName(fileName):
    return fileName.split(".")[2]

def getFileLineCnt(tmpAbspath):
    return len(open(tmpAbspath).readlines())

def getTmpSnappyFileName(filepath, fileName):
    lineCnt = getFileLineCnt("%s/%s"%(filepath, fileName))
    currentTime = "%s%s"%(dt.datetime.strftime(dt.datetime.today(), "%m%d"), dt.datetime.strftime(dt.datetime.now(), "%H%M%S"))
    return "%s.%s.%s.%s"%(fileName, lineCnt, currentTime,snappyPostfix)

def cleanTmpFile(sourceFilePath):
    files = os.listdir(sourceFilePath) 
    for tmpfile in files:
        if(tmpfile.find(checkSnappyPostfix)>0):
            os.remove("%s/%s"%(sourceFilePath, tmpfile))
        if(tmpfile.find(snappyPostfix)>0):
            os.remove("%s/%s"%(sourceFilePath, tmpfile))

def isCompressOk(sourceFilePath, tmpSnappyFileName):
    tmpCheckFile = "tmp."+checkSnappyPostfix
    os.system("python -m snappy -d %s/%s  %s/%s"%(sourceFilePath, tmpSnappyFileName, sourceFilePath, tmpCheckFile))
    lineCnt = getFileLineCnt("%s/%s"%(sourceFilePath, tmpCheckFile))
    fileLineCnt = getFileLineCntFromName(tmpSnappyFileName)
    return int(lineCnt) == int(fileLineCnt)
    
def doCompressFile(sourceFilePath, fileName, targetFilePath):
    tmpSnappyFileName = getTmpSnappyFileName(sourceFilePath, fileName)
    os.system("python -m snappy -c %s/%s %s/%s"%(sourceFilePath, fileName, sourceFilePath, tmpSnappyFileName))
    isOk = isCompressOk(sourceFilePath, tmpSnappyFileName)
    if(isOk):
        targetFileName=tmpSnappyFileName[:tmpSnappyFileName.rfind(".")]
        topicandday=tmpSnappyFileName[0:tmpSnappyFileName.find('.')]
        day=topicandday[topicandday.rfind("_")+1:len(topicandday)]
        isday=day.isdigit()
        if (isday == True and len(day) == 8):
            snames=tmpSnappyFileName.split(".")
            targetFileName="%s.%s.%s.%s.%s"%(topicandday[:topicandday.rfind("_")],snames[1],day,snames[2],snames[3])
        fileSize = os.path.getsize("%s/%s"%(sourceFilePath, tmpSnappyFileName))
        targetFileName = "%s.%s.%s"%(targetFileName,fileSize,"snappy")
        print "-----------%s"%(targetFileName)
        os.system("mv %s/%s  %s/%s"%(sourceFilePath,tmpSnappyFileName, targetFilePath, targetFileName))
        os.remove("%s/%s"%(sourceFilePath, fileName))
    cleanTmpFile(sourceFilePath)
    return isOk

def doDecompressFile(sourceFilePath, fileName):
    tmpCheckFile = "tmp."+checkSnappyPostfix
    os.system("python -m snappy -d %s/%s %s/%s"%(sourceFilePath, fileName, sourceFilePath, tmpCheckFile))
    lineCnt = getFileLineCnt("%s/%s"%(sourceFilePath, tmpCheckFile))
    fileLineCnt = getFileLineCntFromName(fileName)
    isOk = int(lineCnt) == int(fileLineCnt)
    if(isOk):
        os.system("mv %s/%s  %s/%s"%(sourceFilePath, tmpCheckFile, sourceFilePath, fileName[:fileName.rfind(".")]) )
        os.remove("%s/%s"%(sourceFilePath, fileName))
    cleanTmpFile(sourceFilePath)  
    return isOk

def log(content):
    v_logfile="/tmp/snappy_file.log"
    if(sys.argv[3] == "c"):
        v_logfile="/data/logs/snappy_file.log"
    currentTime = "%s%s"%(dt.datetime.strftime(dt.datetime.today(), "%Y%m%d"), dt.datetime.strftime(dt.datetime.now(), "%H%M%S"))
    if(True):
        fileHandle = open ( v_logfile, 'a+' ) 
        fileHandle.write("%s    %s\n"%(currentTime, content))
        fileHandle.close() 

def compressFile(sourceFilePath,fileSizeThresholdMap,targetFilePath):
    while(1==1):
        try:
            time.sleep(1)
            files = os.listdir(sourceFilePath) 
            for tmpfile in files:
                fileSize = os.path.getsize("%s/%s"%(sourceFilePath, tmpfile))
                threshold=getFileSizeThreshold(fileSizeThresholdMap,tmpfile)
                if(tmpfile.find(".")>0 and tmpfile.find(".snappy")==-1 and fileSize>= threshold):
                    currentTime = dt.datetime.now()
                    for i in range(5):
                        log("%s begin compress"%(tmpfile))
                        isOk = doCompressFile(sourceFilePath, tmpfile, targetFilePath)
                        if(isOk):
                            break;
                    log("%s is compressed [isOk:%s] usetime:%s"%(tmpfile, isOk, dt.datetime.now() - currentTime))
        except Exception, e:
            print "error", log(str(e))
            pass;

def discompressFile(sourceFilePath, targetFilePath):
    while(1==1):
        try:
            time.sleep(1)
            files = os.listdir(sourceFilePath) 
            for tmpfile in files:
                if( tmpfile.find(".")>0  and os.path.exists("%s/%s.done"%(sourceFilePath, str(tmpfile))) ):
                    currentTime = dt.datetime.now()
                    for i in range(5):
                        log("%s begin decompress"%(tmpfile))
                        isOk = doDecompressFile(sourceFilePath, tmpfile)
                        if(isOk):
                            os.system("rm %s/%s.done"%(sourceFilePath, tmpfile))
                            os.system("mv %s/%s  %s"%(sourceFilePath,tmpfile[:tmpfile.rfind(".")], targetFilePath) )
                            break;
                    log("%s is compressed [isOk:%s] usetime:%s"%(tmpfile, isOk, dt.datetime.now() - currentTime))
        except Exception, e:
            print "error", (e)
            pass;

def main():
    sourceFilePath = sys.argv[1]
    targetFilePath = sys.argv[2]
    processType = sys.argv[3]
    fileSizeThresholdMap={}
    intFileSizeThreshold(fileSizeThresholdMap)    
    if(processType == "c"):
        print sourceFilePath
        compressFile(sourceFilePath,fileSizeThresholdMap,targetFilePath)
    if(processType == "d"):
        discompressFile(sourceFilePath, targetFilePath)
main()
