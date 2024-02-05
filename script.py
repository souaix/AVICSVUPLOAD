import datetime
import logging
import sys
sys.path.append('/home/cim')
# sys.path.append(r'C:\Users\User\Desktop\python')

#logging
import global_fun.logging_fun as logfun

# connector
import connect.connect as cc
con_cim = cc.connect('CIM_ubuntu', 'eda_csvupload')

# #開啟log
logfun.set_logging('/home/cim/log/AVICSVUPLOAD')
logging.debug('----------------------------------------------------------')
logging.info('Start at - ' + datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

import t_TOPCON as TPC
import CAMTEK as CMK
import pandas as pd

#撈出啟用的
sql = '''
SELECT EQUIP_TYPE,EQUIP_ID,IP,(CASE WHEN lastdatetime is null then BEGINTIME else lastdatetime END) AS lastdatetime FROM
(select EQUIP_TYPE,EQUIP_ID,IP,LASTDATETIME as BEGINTIME from equip_avi_ftp WHERE CONTROL ="1") AS A
LEFT JOIN
(select DISTINCT * from log WHERE lastdatetime IN (SELECT MAX(lastdatetime) FROM log GROUP BY eqpid)) AS B
ON A.EQUIP_ID=B.eqpid
'''

df = pd.read_sql(sql,con_cim)

logging.info('-----Topcon---------')

df_ = df[df["EQUIP_TYPE"] == "Topcon"]
df_.reset_index(inplace=True,drop=True)

for i in range(0,len(df_)):
    IP = df_["IP"]
    EQUIP_ID = df_["EQUIP_ID"]
    lastdatetime = df_["lastdatetime"]

    logging.info(EQUIP_ID[i]+" scan > "+str(lastdatetime))

    #下載參數
    TOPCON = TPC.AVICsvForEDA_Topcon()
    TOPCON.FTPip = IP[i]
    TOPCON.FTPaccount = 'ghost'
    TOPCON.FTPpassword = '123456'
    TOPCON.RootDir = '/DAT'
    TOPCON.ext = 'csv'
    TOPCON.AssignFILE = ['Lot.dat','M_Lot.dat']
    #上傳參數
    TOPCON.SavePathLv3 = 'Topcon'
    TOPCON.SavePathLv4 = EQUIP_ID[i]
    TOPCON.SFTPip = '10.21.150.42'
    TOPCON.LastModify = lastdatetime[i]

    UPDATELastModify = TOPCON.DLLogic()
    TOPCON.UPDATELastModify = UPDATELastModify

    TOPCON.uploadfile()

logging.info('End at - ' + datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
logging.debug('---------------------------------------------------------')


logging.info('-----Camtek---------')

df_ = df[df["EQUIP_TYPE"] == "Camtek2"]
df_.reset_index(inplace=True,drop=True)

if(len(df_)>0):
    for i in range(0,len(df_)):
        IP = df_["IP"]
        EQUIP_ID = df_["EQUIP_ID"]
        lastdatetime = df_["lastdatetime"]

        logging.info(EQUIP_ID[i]+" scan > "+str(lastdatetime))

        #下載參數
        CAMTEK = CMK.AVICsvForEDA_Camtek()
        CAMTEK.FTPip = IP[i]
        CAMTEK.FTPaccount = 'ghost'
        CAMTEK.FTPpassword = '123456'
        CAMTEK.RootDir = '/Temp'
        CAMTEK.ext = 'csv'
    #     TOPCON.AssignFILE = ['Lot.dat','M_Lot.dat']
        #上傳參數
        CAMTEK.SavePathLv3 = 'Camtek'
        CAMTEK.SavePathLv4 = EQUIP_ID[i]
        CAMTEK.SFTPip = '10.21.150.42'
        CAMTEK.LastModify = lastdatetime[i]

        UPDATELastModify = CAMTEK.DLLogic()
        CAMTEK.UPDATELastModify = UPDATELastModify

        CAMTEK.uploadfile()

    logging.info('End at - ' + datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    logging.debug('---------------------------------------------------------')
             
