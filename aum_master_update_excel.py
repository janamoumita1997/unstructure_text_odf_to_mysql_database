import pandas as pd
import pymysql
from datetime import datetime
import os
import shutil
import time
import re
from pathlib import Path
# from dbfread import DBF
start = datetime.now()
def tuple_converter(tuplerow):
	x=tuple(x if x != '' else None for x in tuplerow)
	return x
def dynamic_quries(tupleRow):
  tuple_column_name=('foliochk','product','bal_date','clos_bal','rupee_bal')
  a_zip = zip(list(tupleRow), list(tuple_column_name))
  y=list(a_zip)
  queries=''
  for i in y:
    if i[0] != None:
      query=f'{i[1]} =%s AND '
    else:
      query=f'{i[1]} IS NULL AND '
    queries +=query

  sql_queries=f'SELECT * FROM aum_master WHERE {queries}'
  return sql_queries[:-5]
def valuefromtuple(tupleRow):
  lst=[]
  for i in tupleRow:
    if i is not None:
      lst.append(i)
  return tuple(lst)

def connect_db():
    return pymysql.connect(
        host='localhost', user = 'root', passwd = 'password', database = 'mf_system',			#change the database name
        autocommit = True, charset = 'utf8mb4',
        cursorclass = pymysql.cursors.DictCursor)

cursor = connect_db().cursor()

PROJECT_ROOT = os.getcwd()
PATH_DIR = PROJECT_ROOT + "/duplicate/aum/update"
DEST_PATH=PROJECT_ROOT + "/uploads/aum_master/archive/"
for txt_path in Path(PATH_DIR).glob("*.xlsx"):
  #print(txt_path)
  filename = txt_path
  
  break


print("filename.........",filename)


name_of_textfile=str(filename).split("/")[-1]
# print(name_of_textfile)
pat1 = re.compile(r"aum_duplicate_report_karvy+\d{8}-\d{6}.xlsx")
if re.fullmatch(pat1, name_of_textfile):
  print("process for karvy_ducument")
else:
  print("file not match")
if filename !='':
  df= pd.read_excel(filename,engine='openpyxl')
  Shape=df.shape
  test_file=df.fillna("")

  #print(test_file)
  timestr1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  dSql = "UPDATE aum_master SET modified_at=%s WHERE delete_flag=0"
  cursor.execute(dSql,(timestr1))
  for i, d in test_file.iterrows():
    tupleRow = tuple(d[i] for i in range (Shape[1]))
    #print("tupleRow                 >>>>>>>>>",tupleRow)
    new_tupleRow=tuple_converter(tupleRow[1:])
    #print("new_tupleRow   >>>>>>>>>>>",new_tupleRow)
    #print(len(new_tupleRow))

    queries="SELECT index_name FROM read_config WHERE file_type='aum'" 
    cursor.execute(queries)
    x=cursor.fetchall()
    lst1=[]
    for i in range(5):

      #print(x[i]['index_name'])
      if x[i]['index_name']=='foliochk':
        lst1.append(d['foliochk'])
      if x[i]['index_name']=='product':
        lst1.append(d['product'])
      if x[i]['index_name']=='bal_date':
        lst1.append(['bal_date'])
      if x[i]['index_name']=='clos_bal':
        lst1.append(d['clos_bal'])
      if x[i]['index_name']=='rupee_bal':
        lst1.append(d['rupee_bal'])

    tupleRow2=tuple(lst1)
    #print("tupleRow2    >>>>>>>",tupleRow2)

    quries=dynamic_quries(tupleRow2)
    value=valuefromtuple(tupleRow2)

    query_1=f"{quries}"
    cursor.execute(query_1,value)
    rows=cursor.fetchall()
    length_row=len(rows)

    ab=dynamic_quries(x)
    cd=ab.split("aum_master")[1]
    ef=cd[1:]

    queries3=f"UPDATE aum_master SET foliochk =%s,product =%s,bal_date =%s,clos_bal =%s,rupee_bal =%s,brokcode =%s,last_trxn =%s,euin =%s,pan =%s,amc_code =%s,folio_old =%s,scheme_fol =%s,scheme =%s,divopt =%s,funddesc =%s,bal_units =%s,pldg =%s,trdate =%s,trdesc =%s,moh =%s,sbcode =%s, =%s,inv_id =%s,invname =%s,add1 =%s,add2 =%s,add3 =%s,city =%s,inv_pn =%s,rphone =%s,ophone =%s,fax =%s,email =%s,valinv =%s,inav =%s,crdate =%s,crtime =%s,todate =%s,data_source =%s {ef}"


    sql_queries="INSERT INTO aum_master (foliochk,product,bal_date,clos_bal,rupee_bal,brokcode,last_trxn,euin,pan,amc_code,folio_old,scheme_fol,scheme,divopt,funddesc,bal_units,pldg,trdate,trdesc,moh,sbcode,pout,inv_id,invname,add1,add2,add3,city,inv_pn,rphone,ophone,fax,email,valinv,inav,crdate,crtime,todate,data_source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    tuple_duplicate_replace=new_tupleRow+value
    #print(tuple_duplicate_replace)
    if length_row == 0:
      cursor.execute(sql_queries,new_tupleRow)
      #print("********************")
    if length_row > 0:
      #print("******")
      cursor.execute(queries3,tuple_duplicate_replace)

timestr = time.strftime("%Y%m%d-%H%M%S")
archive_name = DEST_PATH+'aum_archive_update' + str(timestr) + '.xlsx'
shutil.move(filename,archive_name)
print("*****************updated and file move to archive*********************")

total_time_taken = datetime.now()-start
print("total_time_taken           -",total_time_taken)
print("****************done*********************")
