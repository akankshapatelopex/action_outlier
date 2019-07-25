import requests
## download raw code from Opex artifactory

## set some variables
sourceFolderPath=r"C:\\advait_actions\\"
server = {
'url':r'http://tableau.opexanalytics.com:8002',
'downloadApi':'/download',
'token':'WKuxMkls056p+0v6MWmILCSlgnt+tU7qTh7iNTsg/Qs='
}
validExtensions=['py','json']

# read all files from the source folder location
# files with extensions contained in validExtensions are only allowed  
def validateAndReturnFilesToDownload(file_list):
    files=[]
    for f in file_list:
        parts = f.split(".")
        if len(parts)>1 and parts[1].lower() in validExtensions:
            files.append(f)
        else:
            raise Exception("the file name you specified '{:s}' does not have a .py extension. Please check and rerun".format(f))
    
    return file_list


# this  will download file from the specified server in the Scripts folder of the App
# if any download fails, the script execution will stop
def download(file_name):
    print('downloading...'+file_name)
    url=server['url']+server['downloadApi']+r'?file='+sourceFolderPath+file_name
  
    with open(file_name, "wb") as fName:
        # get request
        r = requests.get(url,headers={'token':server['token']},timeout=180)
        # write to file
        fName.write(r.content)
  
  
    if r.status_code==200:
        print('Downloaded Successfully...'+file_name)
    else:
        raise Exception(r.text)


testfile = 'setup_actions.py'

download(testfile)