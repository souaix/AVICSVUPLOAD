
import datetime
import logging
import sys
import os
sys.path.append('/home/cim')
# sys.path.append(r'C:\Users\User\Desktop\python')

#logging
import global_fun.logging_fun as logfun

# connector
import connect.connect as cc
con_cim = cc.connect('CIM_ubuntu', 'eda_csvupload')

import shutil

# #開啟log
logfun.set_logging('/home/cim/log/AVICSVUPLOAD')
logging.debug('----------------------------------------------------------')
logging.info('Start at - ' + datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

sftpPath=[]

for root, dir_list, file_list in os.walk('/home/cim/MAP/AVICSVUPLOAD/RW'):

    if(len(dir_list) == 0):
        root = root.replace("\\", "/")
        root = root.replace("./", "")
        sftpPath.append(root)

        logging.info("---START UPLOAD---")

        try:
            for p in sftpPath:
                if p != '.ipynb_checkpoints' and p != '__pycache__':
                    for root, dir_list, file_list in os.walk(p):
                        for f in file_list:
                            print(p+"/"+f)
                            os.remove(p+"/"+f)

        except Exception as E:
            logging.debug("上傳檔案失敗 : " + str(E))
            # print("上傳檔案失敗 : " + str(E))

        #shutil.rmtree('/home/cim/MAP/AVICSVUPLOAD/RW')
