import pysftp
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None


#def sftp_upload(sHostName,PATHS,FILES,PPK=""):

PATHS = 'RW/AVI/Topcon/AVI-20/2404/MOLT24400501/MOLT24400501-012'

FILES = '/home/cim/MAP/AVICSVUPLOAD/RW/AVI/Topcon/AVI-20/2404/MOLT24400501/MOLT24400501-012/M_OV10650_R_08_01_AVI-20_24400501-012_CLS06.csv'

    
PATHS = PATHS.split("/")

sHostName = '10.21.150.42'
sUserName = 'sftpuser'
sPassWord = 'sftpuser'
PPK = ""
        

if PPK =="":

    srv = pysftp.Connection(sHostName, username=sUserName, password=sPassWord, cnopts=cnopts) 

else:

    srv = pysftp.Connection(sHostName, username=sUserName, private_key=PPK, cnopts=cnopts) 

        
if(sUserName):
    with srv as sftp:
        print(sftp.listdir())
        print("----------------")
        for p in PATHS :
            print(p)
            
            path_exist  = True if p in sftp.listdir() else False
                
            if not path_exist  and p!= '':
                try:
                    sftp.mkdir(p)

                    print(sftp.listdir())
                    print("===================")

                except Exception as E:
                    print("mkdir fail : " +p+"--"+str(E))

            sftp.cwd(p)
            print(sftp.listdir())
            print("++++++++++++++++++++++++")

        # 上傳檔案
        if(type(FILES) == list):

            for i in FILES:
                sftp.put(i)

        elif(type(FILES) == str):
            try:
                sftp.put(FILES)

            except Exception as E:
                print(str(E))
