import sys
import os
import datetime as dt
import time

print dt.datetime.now()
currentTimestamp = dt.datetime.strftime(dt.datetime.now(),"%m%d%H%M%S")

fortest = False
def getModifyFileName(fname, timestamp):
    return "%s.%s%s"%(fname[:fname.find(".")],fname[fname.find(".")+1:],timestamp)

def log(content):
    v_logfile="/data/logsynch.log"
    currentTime = "%s%s"%(dt.datetime.strftime(dt.datetime.today(), "%Y%m%d"), dt.datetime.strftime(dt.datetime.now(), "%H%M%S"))
    if(True):
        print content
        fileHandle = open ( v_logfile, 'a+' ) 
        fileHandle.write("%s    %s\n"%(currentTime, content))
        fileHandle.close() 

def getFileLineCnt(filepath):
    cnt = len(open(filepath).readlines())
    return cnt

def main():
    filepath = "/data/log"
    bakFilepath = "/data/baklog"
    targetFilepath = "root@192.168.0.15:/data/tmplog"
    scpPort ="  "
    while(1==1):
    #if(1==1):
        try:
            time.sleep(1)
            files = os.listdir(filepath) 
            for tmpfile in files:
                if( tmpfile.find(".snappy")>0 ):
                    currentDate = dt.datetime.strftime(dt.datetime.now(),"%Y%m%d")
                    currentTime = dt.datetime.now()
                    modifyFileName = tmpfile
                    modifyFileNameDone = modifyFileName + ".done"
                    fileSize = os.path.getsize("%s/%s"%(filepath, tmpfile))
                    #scpCmd = ("scp %s/%s  %s"%(filepath, modifyFileName, targetFilepath))
                    scpCmd = ("sudo scp %s %s/%s  %s"%(scpPort, filepath, modifyFileName, targetFilepath))
                    mkdirCmd = "mkdir -p %s/%s"%(bakFilepath, currentDate)
                    mvFileCmd = ("mv %s/%s  %s/%s"%(filepath, modifyFileName, bakFilepath, currentDate))
                    echoModifyFileNameDone = ("echo ''>%s/%s"%(filepath, modifyFileNameDone))
                    scpModifyFileNameDone = "sudo scp %s %s/%s  %s"%(scpPort, filepath, modifyFileNameDone, targetFilepath)
                    rmModifyFileNameDone = "rm -f %s/%s"%(filepath, modifyFileNameDone)
                    if(fortest==False and len(modifyFileName)>0):
                        log(scpCmd)
                        os.system(scpCmd)
                        log(echoModifyFileNameDone)
                        os.system(echoModifyFileNameDone)
                        log(scpModifyFileNameDone)
                        os.system(scpModifyFileNameDone)
                        log(rmModifyFileNameDone)
                        os.system(rmModifyFileNameDone)
                        log(mkdirCmd)
                        os.system(mkdirCmd)
                        log(mvFileCmd)
                        os.system(mvFileCmd)
                        syncFileInfo("%s/%s"%(bakFilepath, currentDate), modifyFileName)
                    log( "%s\t%sm\t%ss"%(modifyFileName, (float(fileSize))/(1024*1024), dt.datetime.now() - currentTime))
        except Exception, e:
            print str(e)
            pass;
main()
