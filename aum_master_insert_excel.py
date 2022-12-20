import pandas as pd
import pymysql
from datetime import datetime
import os
import shutil
import time
import re
from pathlib import Path
# from dbfread import DBF

def tuple_converter(tuplerow):
	x=tuple(x if x != '' else None for x in tuplerow)
	return x

def connect_db():
    return pymysql.connect(
        host='localhost', user = 'root', passwd = 'password', database = 'mf_system',			#change the database name
        autocommit = True, charset = 'utf8mb4',
        cursorclass = pymysql.cursors.DictCursor)

cursor = connect_db().cursor()

PROJECT_ROOT = os.getcwd()
PATH_DIR = PROJECT_ROOT + "/duplicate/aum/insert"
DEST_PATH=PROJECT_ROOT + "/uploads/aum_master/archive/"

for txt_path in Path(PATH_DIR).glob("*.xlsx"):
  #print(txt_path)
  filename = txt_path
  
  break


print("filename.........",filename)


name_of_textfile=str(filename).split("/")[-1]
print(name_of_textfile)
#aum_duplicate_report_karvy20220210-191211.xlsx
pat1 = re.compile(r"aum_duplicate_report_karvy+\d{8}-\d{6}.xlsx")

if re.fullmatch(pat1, name_of_textfile):
	print("process for karvy_ducument")
else:
	print("file not match")
if filename !='':
	df= pd.read_excel(filename,engine='openpyxl')
	#print(df)
	test_file=df.fillna("")

	test_file=test_file.drop(['Unnamed: 0'], axis = 1)
	Shape=test_file.shape
	#print(test_file)
	for i, d in test_file.iterrows():
		tupleRow = tuple(d[i] for i in range (Shape[1]))
		#print("tupleRow                 >>>>>>>>>",tupleRow)
		new_tupleRow=tuple_converter(tupleRow)
		#print("new_tupleRow   >>>>>>>>>>>",new_tupleRow)
		#print(len(new_tupleRow))
		sql_queries="INSERT INTO aum_master (foliochk,product,bal_date,clos_bal,rupee_bal,brokcode,last_trxn,euin,pan,amc_code,folio_old,scheme_fol,scheme,divopt,funddesc,bal_units,pldg,trdate,trdesc,moh,sbcode,pout,inv_id,invname,add1,add2,add3,city,inv_pn,rphone,ophone,fax,email,valinv,inav,crdate,crtime,todate,data_source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
		cursor.execute(sql_queries,new_tupleRow)

	timestr = time.strftime("%Y%m%d-%H%M%S")
	archive_name = DEST_PATH+'aum_archive_insert' + str(timestr) + '.xlsx'
	shutil.move(filename,archive_name)
	print("*********** inserted and file move to archive ****************")

print("****************done*********************")



