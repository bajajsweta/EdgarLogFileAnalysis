
# coding: utf-8

# In[47]:

# import urllib.request
import urllib
import os
import sys
import pandas as pd
import numpy as np
import datetime 
import glob
import logging 
import zipfile as z
from datetime import datetime
# import matplotlib.pyplot as plt
# from pylab import plot, ylim, xlim, show, xlabel, ylabel, grid
from numpy import linspace, loadtxt, ones, convolve


# In[48]:

# create logger
logger = logging.getLogger('Log files Logger')
logger.setLevel(logging.DEBUG)


# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

fh = logging.FileHandler('logs.log')
fh.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# add ch to logger
logger.addHandler(fh)
logger.addHandler(ch)

# 'application' code
logger.debug('debug message')
logger.info('info message')
logger.warn('warn message')
logger.error('error message')
logger.critical('critical message')


# In[49]:

def user_input_validation(value):
    while True:
        try:
            curr_yr = int(input(value))
            if curr_yr not in range(2003,2017):
                print("Sorry, Incorrect Year! Please provide a valid year") 
                continue
                #better try again... Return to the start of the loop
        except ValueError:
            print("Sorry, Incorrect Year! Please provide a valid year")
            #better try again... Return to the start of the loop
            continue
        else:
            #year was successfully parsed!
            #we're ready to exit the loop.
            break
    return value

curr_yr = user_input_validation('Input Year:')


# In[50]:

def check_year(curr_yr):
    year = ['2003','2004','2005','2006','2007','2009','2010','2011','2012','2013','2014','2015','2016']
    if curr_yr in year:
        return curr_yr
    
    else:
        
        curr_yr = '2003'
        return curr_yr
    logger.warn(' Year not present on the website, default value set to 2003 for processing')

year = check_year(curr_yr)


# In[51]:

def calculate_QTR(mon):
    if mon in range(1,4):
        qtr = 'Qtr1'
        return qtr
    elif mon in range(4,7):
            qtr='Qtr2'
            return qtr
    elif mon in range(7,10):
            qtr='Qtr3'
            return qtr
    elif mon in range(10,13):
        qtr='Qtr4'
        return qtr
        


# In[52]:

# first validation, handling empty column values for each column
def empty_fill(df):
    
    df = df.fillna({
    'ip': '000.00.00.abc',
    'zone': '0.0',
    'cik': 'missing data',
    'accession': 'missing data',
    'extension': '-index.htm',
    'code': '403.0',
    'size': '0.0',
    'idx': '0.0',
    'norefer': '0.0',
    'noagent': '0.0',
    'find':'-1.0',
    'browser':'Browser not provided'
         
})
    return df 


# In[19]:

for mon in range(1,13):
        qtr = calculate_QTR(int(mon))
        if (mon <=9):
            url = 'http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/'+year+'/'+qtr+'/log'+year+'0'+str(mon)+'01.zip'
            download= urllib.request.urlretrieve(url , "log"+year+'0'+str(mon)+'01'".zip")
            print(download)
            logger.info(' Download of files started, Logging files download list:')
            logger.info(download)
        else:
            url = 'http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/'+year+'/'+qtr+'/log'+year+str(mon)+'01.zip'
            download= urllib.request.urlretrieve(url ,"log"+year+str(mon)+'01'".zip")
            print(download)
            logger.info(' Download of files started, Logging files download list:')
            logger.info(download)


# In[53]:

# working code
zip_files = glob.glob('*.zip')
for zip_filename in zip_files:
    zf = z.ZipFile(zip_filename,'r')
    zip_handler = z.ZipFile(zip_filename, "r")
    zip_handler.extractall()
    
    all_data = pd.DataFrame()


CsvFileList= []   
for f in glob.glob("*.csv"):
    CsvFileList.append(f)

print(CsvFileList)
    #for all csv files in pwd
for file in CsvFileList:        
    df= pd.read_csv(file)
    df  =  empty_fill(df) 
    all_data = all_data.append(df, ignore_index=True)
    all_data.to_csv("Combined"+year+".csv", index=None)
        
    
print("completed")
logger.info('Missing / Null  values inserted in the files with relevant details.')
logger.info('Combined zip file created for all the month for the year')
    
    


# In[67]:

df = pd.read_csv("Combined"+year+".csv")
### Validations
##Second 


# In[79]:

IP = df['code'].value_counts()
IP


# In[68]:

################## Second validation ####################################
# if code is equal to zero, then replace it qith 403, 403: 
def code_valid(df):
    if (any(df.code == 0)) == True:
        df['code'].replace(0, 403,inplace=True)
        logger.info('Code column with value 0 replaced with 403')
    return df


################################ Third Validation ####################################################
#   if find 
def find_valid(df):    
    if (all((df['find'] >= 0) & (df['find'] <= 10))) == True:
        return df
    else:
        logger.error('Incorrect value present in find column') 
        logger.error(df['find'])
        return df

########################### Fourth Validation ######################################################
df['date'].fillna(method='bfill', inplace=True)
df['time'].fillna(method='bfill', inplace=True)


################################ Fifth Validation #################################

def crawler_valid(df):
    if (any((df['code'] == 404) & (df['crawler'] == 1)) ) == True:
        return df
    else :
        df['crawler'].replace(0,1) 
        logger.warn('Crawler value incorrect, replaced with 1 for code 404')
        return df


############################### Sixth Validation #########################################

def extension_valid(df1):
    df1['extention'] = np.where(df1['extention'].str.startswith('-'),df1['accession'] + df1['extention'],df1['extention'])
    df1['extention'] = np.where(df1['extention'].str.startswith('.'),df1['accession'] + df1['extention'],df1['extention'])
    logger.info('Specific extension replaced with accession concat with the extensuin for data starting with - or .')
    return df1


########################## Seventh Validation ##################################

def time_format_valid(df2):
    for col in df2['time']:
        try:
            if datetime.strptime(col, '%H:%M:%S'):
                logger.info("time column has specified date format")
                return df
            else:
                print("Null Value")### Log here
        except ValueError:
            logger.error('Time column value not in correct format :%H:%M:%S')
            
            
            
            
def date_format_valid(df1):
    for col in df1['date']:
        try:
            if datetime.strptime(col, '%Y-%m-%d'):
                logger.info("date column has specified date format")
                return df
            else:
                print("Null Value")### Log here
        except ValueError:
            logger.error('Date column value not in correct format: %Y-%m-%d')
            

 


# In[69]:

df['date'].fillna(method='bfill', inplace=True)
df['time'].fillna(method='bfill', inplace=True)
df = code_valid(df)
df = crawler_valid(df)
df = find_valid(df)
df = extension_valid(df)
df = time_format_valid(df)
df = date_format_valid(df)

logger.info('Validation completed')



# In[ ]:

################################## DATA ANALYSIS ###########################


# In[73]:

#################### FIRST ###########################
CountofIP = df.groupby(['ip','accession','time']).size()    
CountofIP.to_csv('IPCount.csv',header=['count']) 


# In[74]:

#################### Second ###########################
CIKByIpDateTime =  df[['cik','ip','date','time']].groupby(['cik','ip','date','time']).size()  
CIKByIpDateTime.to_csv('CIKByIpDateTime.csv',header=['count'])


# In[ ]:

Countofcik = df.groupby(['cik','date']).size()    
Countofcik.to_csv('CountOFcik.csv',header=['count'])


# In[ ]:

CIKdf = pd.read_csv('CountOFcik.csv')
CIKdf['Month'] = CIKdf['date'].str[5:-3]
##CIKdf

df1 = CIKdf.loc[CIKdf['cik'] == 1022080.0]

#### plot graph #######

plt.xlabel('Date')
plt.ylabel('Count Of Cik')

plt.plot(df1['Month'],df1['count'])

#plt.show()


# In[ ]:

CIKdf = pd.read_csv('CountOFcik.csv')

def movingaverage(interval, window_size):
    window= np.ones(int(window_size))/float(window_size)
    return np.convolve(interval, window, 'same')

x= df1['Month']
y =df1['count']


#plot(x,y,"k.")
#y_av = movingaverage(y, 3)
#plot(x, y_av,"r")
#xlim(0,12)
#xlabel("month")
#ylabel("Count Of CIK")
#grid(True)
#show()


# In[ ]:

SizeByTime = df[['cik','size','date','time']].groupby(['cik','size','date','time']).size()
SizeByTime.to_csv('SizeByTime.csv',header=['count'])

SizeByTime1 = pd.read_csv('SizeByTime.csv')


# In[ ]:

SizeByTime1['Month'] = SizeByTime1['date'].str[5:-3]
SizeByTime1['Hour'] = SizeByTime1['time'].str[:2]

SizeByTime2 = SizeByTime1.loc[SizeByTime1['Month'] == '12']
SizeByTime2 = SizeByTime1.loc[SizeByTime1['cik'] == 97349.0]


x= SizeByTime2['Month']
y =SizeByTime2['size']


#plot(x,y,"k.")
#xlim(0,12)
#xlabel("Month")
#ylabel("Size")
#grid(True)
#show()


# In[ ]:

############################### Second #######################
Countofaccession = df.groupby(['accession','time']).size()
Countofaccession.to_csv('Countofaccession.csv',header=['count'])


# In[ ]:

########################## THRID ############################

IP = df['ip'].value_counts()
IP.to_csv('IPCount.csv',header=['count'])


# In[78]:

############################ FOURTH ######################

CountofCode = df.groupby(['ip','code']).size()
CountofCode.to_csv('CountOfCode.csv',header=['count'])
#anamoly : code = 0 : there is no http code =0


# In[ ]:

#############################  SEVENTH ########################
count_of_browser = df['browser'].value_counts()
count_of_browser.to_csv('CountofBrowser.csv',header=['count'])


# In[ ]:

#################### NINETH ############################

AccessionByIP =df[['ip','accession']].groupby(['accession','ip']).size()
AccessionByIP.to_csv('AccessionByIP.csv',header=['count'])


# In[ ]:

####################### TENTH #####################

count_find = df['find'].value_counts()
count_find.to_csv('CountOfFind.csv',header=['count'])
print('half way through....')


# In[ ]:

########################### ELEVENTH ########################
#### no records with find = 5
CikByAccessionSizeCode=df[['cik','accession','size','code']].groupby(['cik','accession','size','code']).size()
CikByAccessionSizeCode.to_csv('CikByAccessionSizeCode.csv',header=['count'])


# In[ ]:

###################### TWELVTH ########################
# Number of accession per month

AccessionnByMonth= df[['accession','date']].groupby(['accession','date']).size()
AccessionnByMonth.to_csv('AccessionnByMonth.csv',header=['count'])


# In[ ]:

####################### FOURHTEEN #################

AccessionByCrawlerCodeFind=df[['accession','crawler','code','find']].groupby(['accession','crawler','code','find']).size()
AccessionByCrawlerCodeFind.to_csv('AccessionByCrawlerCodeFind.csv',header=['count'])


# In[ ]:

######################### FIFHTEEN ###############

count_crawler = df['crawler'].value_counts()
count_crawler.to_csv('CrawlerCount.csv',header=['count'])


# In[ ]:

######################## SIXHTEEN ################

AccessionBySizeCode = df[['size','code','accession']].groupby(['size','accession','code']).size()
AccessionBySizeCode.to_csv('AccessionBySizeCode.csv',header=['count'])


# In[ ]:

######################### EGIHTEEN ############################

count_size = df['size'].value_counts()
count_size.to_csv('CountOfSize.csv',header=['count'])


# In[ ]:

############################ NINTEEN ##########################

AccessionBySize = df[['size','accession']].groupby(['size','accession']).size()
AccessionBySize.to_csv('AccessionBySize.csv',header=['count'])


# In[ ]:

############################ TWENTY ####################

AccessionByFineSize =df[['size','accession','find']].groupby(['size','accession','find']).size()
AccessionByFineSize.to_csv('AccessionByFineSize.csv',header=['count'])


# 

# In[38]:

content_list = []
for content in os.listdir("."): # "." means current directory
    #print(content)
    if not content.endswith(".ipynb") and not content.endswith(".zip") and not content.endswith(".py") and not content.startswith('log') and not content.startswith('Combined') and not content.startswith(".ipynb_checkpoints") and not content.startswith(".DS_Store"):
        content_list.append(content)

print (content_list)


# In[ ]:




# In[39]:

#First Create the Bucket and Check if It Exists or No
import boto
from boto.s3.connection import S3Connection
import tinys3


# In[ ]:

#Connection for Boto
conn = S3Connection('AKIAJW7GUWVQDH7OGRVA', 'YCGmOrYtjkuuV8KTeqmBTF+oOHBZFnDmBdmSnWdM')


# In[ ]:

#Create a Bucket and Check if Bucket Exists or No
bucket = conn.lookup('some-value')
if bucket is None:
    print ("This bucket doesn't exist.")
    bucket = conn.create_bucket('bucket-logcsv')
    print ("Bucket Created")
#     Else if bucket is there and use that bucket


# In[ ]:

bucketobj = conn.get_bucket(bucket)
bucketobj


# In[ ]:

#Create tinys3 Connection
connt3 = tinys3.Connection('AKIAJW7GUWVQDH7OGRVA','YCGmOrYtjkuuV8KTeqmBTF+oOHBZFnDmBdmSnWdM',tls=True)


# In[ ]:

#Get the Zip File from the Current Directory and Upload it on AmazonS3
#Example #f = open('H:/Advance Data Science/Assignment - 1/R/csv1.zip','rb')

f = open(os.getcwd()+'/' + "Combined"+year+".csv",'rb')
connt3.upload("Combined"+year+".csv",f,'bucket-logcsv')

for file in content_list:
    f = open(os.getcwd()+'/' + file,'rb')
    connt3.upload(file,f,'bucket-logcsv')


print ("File Uploaded to the S3 Storage")


# In[ ]:


# In[ ]:

logger.info('Session completed!!')


# In[ ]:

print("Process Completed!!!")

