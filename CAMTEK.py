#!/usr/bin/env python
# coding: utf-8

import json

import sys
import os
from ftplib import FTP
import datetime
import pandas as pd
from dateutil import parser
from glob import glob
import shutil
import logging
from sqlalchemy import text

sys.path.append('/home/cim')
# sys.path.append(r'C:\Users\User\Desktop\python')

#sftp
import global_fun.sftp_fun as SFT
# logging
import global_fun.logging_fun as logfun
# connector
import connect.connect as cc
con_cim = cc.connect('CIM_ubuntu', 'eda_csvupload')


def downloadfile(ftp, remotepath, localpath):
    bufsize = 1024  # 设置缓冲块大小
    fp = open(localpath, 'wb')  # 以写模式在本地打开文件
    ftp.retrbinary('RETR ' + remotepath, fp.write, bufsize)  # 接收服务器上文件并写入本地文件
    ftp.set_debuglevel(0)  # 关闭调试
    fp.close()  # 关闭文件

class AVICsvForEDA_Camtek :
    def __init__(self):
        
        self.FTPip = str()
        self.FTPaccount = str()
        self.FTPpassword = str()
        
        self.RootDir = str()
        
        self.ParentInfo = []
        self.ParentDir = str()
        
        self.SubjectInfo = []
        self.SubjectDir = str()
        
        self.SubSubInfo = []
        self.SubSubDir = str()
        
        self.ext = str()
        
        self.Files = []
        
        self.LastModify =  str()
        
        self.SavePathLv1 = 'RW'
        self.SavePathLv2 = 'AVI'
        self.SavePathLv3 = str()
        self.SavePathLv4 = str()
        self.SavePathLv5 = str()
        self.SavePathLv6 = str()
        self.SavePathLv7 = str()
        
        self.SFTPip = str()
        
        self.LastModify = int()
        self.UPDATELastModify = int()
        
    def DLLogic(self):
    
        logging.info("-----"+self.FTPip+"-----")

        ftp =  FTP(self.FTPip, self.FTPaccount, self.FTPpassword)
        ftp.cwd(self.RootDir)

        ftp.retrlines('MLSD', self.ParentInfo.append)

        modify_file_log = []
        
        #第一層目錄
        for i,v in enumerate(self.ParentInfo):
            if (v.split(';')[0].strip())=='type=dir':        

                #最後修改時間
                modify_parent = v.split(';')[1].strip()        
                modify_parent = int(modify_parent.split('=')[1])
                
                
                #DIR名稱
                dirname_parent = v.split(';')[2].strip()
                
                #清空第二層目錄資訊
                self.SubjectInfo=[]
                
                #測試用
#                 if(dirname_parent=='X2L3QBJL_R_08_05' and modify_parent>self.LastModify):
                if(modify_parent>self.LastModify):
                    
                    #重進入根目錄
                    ftp.cwd(self.RootDir)    
                    #進入第一層目錄
                    ftp.cwd(dirname_parent)
                    #取得第二層目錄資訊
                    ftp.retrlines('MLSD', self.SubjectInfo.append)                

                    if(len(self.SubjectInfo)>0):

                        #第二層目錄
                        for j,w in enumerate(self.SubjectInfo):

                            #logging.info("第二層資訊 : "+w)

                            cwdlv2 = 0  #若cwd錯誤則=1，不回到前一目錄

                            #清空地三層目錄資訊
                            self.SubSubInfo=[]

                            #最後修改時間
                            modify_subject = w.split(';')[1].strip()        
                            modify_subject = int(modify_subject.split('=')[1])
                            #DIR名稱
                            dirname_subject = w.split(';')[2].strip()
                            
                            #測試用
#                             if(dirname_subject=='23C01731-001' and modify_subject>self.LastModify):
                            if(modify_subject>self.LastModify):
                                self.SavePathLv5 = dirname_subject[0:4]
#                                 print(self.SavePathLv5)

                                self.SavePathLv6 = "MOLT"+dirname_subject[0:8]
#                                 print(self.SavePathLv6)                            

                                self.SavePathLv7 = "MOLT"+dirname_subject
#                                 print(self.SavePathLv7)                    

                                try:
                                    #進入第二層目錄
                                    ftp.cwd(dirname_subject)      
                                    #取得第三層目錄資訊
                                    ftp.retrlines('MLSD', self.SubSubInfo.append)      

                                except Exception as C:
                                    logging.info("進入第二層目錄錯誤 : "+str(C))
                                    cwdlv2=1

                                if(len(self.SubSubInfo)>0):

                                    #WAFERNO目錄
                                    for k,x in enumerate(self.SubSubInfo):   

                                        #logging.info("第三層資訊 : "+x)

                                        cwdlv3=0

                                        #清空第三層檔案資訊
                                        self.Files=[]                        

                                        #最後修改時間
                                        modify_subsub = x.split(';')[1].strip()        
                                        modify_subsub = int(modify_subsub.split('=')[1])
                                        #DIR名稱
                                        dirname_subsub = x.split(';')[2].strip()                                                              

                                        try:
                                            #進入第三層目錄
                                            ftp.cwd(dirname_subsub)  

                                            #取得第三層檔案資訊
                                            ftp.retrlines('MLSD', self.Files.append) 

                                        except Exception as C:
                                            logging.info("進入第三層目錄錯誤 : "+str(C))
                                            cwdlv3=1


                                        if(len(self.Files)>0 and modify_subsub>self.LastModify):
                                            #最終檔案
                                            for l,y in enumerate(self.Files):     

                                                #logging.info("最終檔案資訊 : "+y)
                                                
                                                #最後修改時間
                                                modify_file = y.split(';')[1].strip()        
                                                modify_file = int(modify_file.split('=')[1])
                                                
                                                #DIR名稱
                                                try:
                                                    filename = y.split(';')[3].strip()   

                                                    if("."+self.ext in filename and modify_file>self.LastModify):  
                                                        
                                                        modify_file_log.append(modify_file)
                                                        
                                                        PATH = "/home/cim/MAP/AVICSVUPLOAD/"+self.SavePathLv1+"/"+self.SavePathLv2+"/"+self.SavePathLv3+"/"+self.SavePathLv4+"/"+self.SavePathLv5+"/"+self.SavePathLv6+"/"+self.SavePathLv7+"/"
                                                        if not os.path.isdir(PATH):
                                                            os.makedirs(PATH)
                                                        print(self.RootDir+"/"+dirname_parent+"/"+dirname_subject+"/"+dirname_subsub+"/"+filename)
                                                        downloadfile(ftp, self.RootDir+"/"+dirname_parent+"/"+dirname_subject+"/"+dirname_subsub+"/"+filename, PATH+filename)
                                                except Exception as F:
                                                    logging.info("DIR ERROR : "+str(F))
                                        
                                        if(cwdlv3==0):
                                            ftp.cwd('../')                                                                                          
                                if(cwdlv2==0):
                                    ftp.cwd('../')    
                        
        if(len(modify_file_log)>0):                
            modify_file_log = max(modify_file_log)
        else:
            modify_file_log=''
        return modify_file_log

    def uploadfile(self):

        #提供sftp_upload使用
        sftpPath = []       #上傳完整路徑
        rootPath=[]         #上傳目錄路徑

        delPath = []
        delPass = 1 # 1=CAN DEL ; 0=CANNOT DEL

        for root, dir_list, file_list in os.walk('/home/cim/MAP/AVICSVUPLOAD/RW'):
            
            #因排程會把/home/cim/MAP/AVICSVUPLOAD吃進去，故要排除
            root = root.split("/")
            root = "/".join(root[5:])
                
            root = root.replace("\\", "/")

            for f in file_list:
                rootPath.append(root)                
                sftpPath.append(root+"/"+f)

        logging.info("---START UPLOAD---")

            
        try:                    
            for p in range(0,len(sftpPath)):

                logging.info("開始上傳 : "+sftpPath[p])
                
                SFT.sftp_upload(self.SFTPip, rootPath[p], sftpPath[p])
                os.remove("/home/cim/MAP/AVICSVUPLOAD/"+sftpPath[p])


        except Exception as E:
            logging.info("上傳檔案失敗 : " + str(E))
            delPass=0
            # print("上傳檔案失敗 : " + str(E))

        logging.info("---START UPDATE ModifyTime : "+self.SavePathLv4+" - "+str(self.UPDATELastModify))

        try:
            if(self.UPDATELastModify != ''):
                sql = "REPLACE INTO log(eqpid,lastdatetime) values ('" + \
                    self.SavePathLv4+"','" + \
                    str(self.UPDATELastModify)+"')"                        

                con_cim.execute(sql)

        except Exception as I:
            logging.info("INSERT LOG失敗 : " + str(I))
            # print("INSERT LOG失敗 : " + str(I))

        logging.info("---DELETE LOCAL FILES---")

        try:
            if delPass==1:
                shutil.rmtree('/home/cim/MAP/AVICSVUPLOAD/RW')
            else:
                logging.info("有檔案未上傳成功，禁止刪除資料夾")

        except Exception as D:
            logging.info("刪除資料夾失敗 : "+str(D))
