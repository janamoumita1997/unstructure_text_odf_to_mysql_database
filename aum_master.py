import pandas as pd
import pymysql
from datetime import datetime
import datetime
import os
import shutil
import time
import re
from pathlib import Path
from dbfread import DBF
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText




PROJECT_ROOT = os.getcwd()
PATH_DIR = PROJECT_ROOT + "/uploads/aum_master/"
DEST_PATH = PROJECT_ROOT + "/uploads/aum_master/archive/"
start = datetime.datetime.now()

subject='aum File processing status ; Duplicate rows'
receiver_email=['jana21physics@gmail.com','jana12moumita@gmail.com']
body='Following are the duplicate found in tranjuction processing. Please find attach.'


def sendMailWithAttachments(subject, filePath, receiver_email, body):
    sender_email = 'information.smartbroker@gmail.com'
    password = 'Jatin@1234'


    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    # message["Bcc"] = receiver_email  # Recommended for mass emails

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    filename = filePath  # In same directory as script

    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    # Log in to server using secure context and send email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, password)
    server.sendmail(sender_email,receiver_email, text)
    server.quit()
        # except Exception as e:
        #     print(e)

def get_val(index ,dict, default):
	if index in dict:
		return dict[index]
	else:
		return default
def tuple_converter(tuplerow):
	x=tuple(x if x != '' else None for x in tuplerow)
	return x

def time_format_change(date):
	if date !='':
		x=datetime.datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
	else:
		x=None
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

for txt_path in Path(PATH_DIR).glob("*.dbf"):
  #print(txt_path)
  filename = txt_path

  break


print("filename.........",filename)

name_of_textfile=str(filename).split("/")[-1]
pat1 = re.compile(r"KARVY_MFSD+\d{3}.dbf")
pat2=re.compile(r"CAMS_WBR+\d{2}.dbf")
name_check=False

column_lst=['foliochk','product','bal_date','clos_bal','rupee_bal','brokcode','last_trxn','euin','pan','amc_code','folio_old','scheme_fol','scheme','divopt','funddesc','bal_units','pldg','trdate','trdesc','moh','sbcode','pout','inv_id','invname','add1','add2','add3','city','inv_pn','rphone','ophone','fax','email','valinv','inav','crdate','crtime','todate','data_source']


if re.fullmatch(pat1, name_of_textfile):

#print(name_of_textfile)
	duplicate=[]
	dbf = DBF(filename,encoding='iso-8859-1')
	df = pd.DataFrame(iter(dbf))
	lst12=df.columns

	if lst12[0]=='PRCODE' and lst12[1]=='FUND' and lst12[2]=='ACNO':
		tupleRow=''
		timestr1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		dSql = "UPDATE aum_master SET delete_flag = 1,modified_at=%s WHERE delete_flag=0"
		cursor.execute(dSql,(timestr1))
		m=0
		for each_dbf in dbf:
			#print(m)
			m+=1
			if m==100:
				break
			#print("each_dbf",each_dbf)
			foliochk=get_val('ACNO',each_dbf,None)
			product=get_val('PRCODE',each_dbf,None)
			bal_date=get_val('',each_dbf,None)
			clos_bal=get_val('',each_dbf,None)
			rupee_bal=get_val('',each_dbf,None)

			brok_dlr_c=get_val('BROKCODE',each_dbf,None)
			last_trxn=get_val('',each_dbf,None)
			euin=get_val('',each_dbf,None)
			pan=get_val('',each_dbf,None)
			amc_code=get_val('FUND',each_dbf,None)
			folio_old=get_val('',each_dbf,None)
			scheme_fol=get_val('',each_dbf,None)
			scheme=get_val('SCHEME',each_dbf,None)
			divopt=get_val('DIVOPT',each_dbf,None)
			funddesc=get_val('FUNDDESC',each_dbf,None)
			balunits=get_val('BALUNITS',each_dbf,None)
			pldg=get_val('PLDG',each_dbf,None)
			trdate=get_val('TRDATE',each_dbf,None)
			trdesc=get_val('TRDESC',each_dbf,None)
			moh=get_val('MOH',each_dbf,None)
			sbcode=get_val('SBCODE',each_dbf,None)
			pout=get_val('POUT',each_dbf,None)
			inv_id=get_val('INV_ID',each_dbf,None)
			invname=get_val('INVNAME',each_dbf,None)
			add1=get_val('ADD1',each_dbf,None)
			add2=get_val('ADD2',each_dbf,None)
			add3=get_val('ADD3',each_dbf,None)
			city=get_val('CITY',each_dbf,None)
			inv_pin=get_val('INV_PIN',each_dbf,None)
			rphone=get_val('RPHONE',each_dbf,None)
			ophone=get_val('OPHONE',each_dbf,None)
			fax=get_val('FAX',each_dbf,None)
			email=get_val('EMAIL',each_dbf,None)
			valinv=get_val('VALINV',each_dbf,None)
			inav=get_val('LNAV',each_dbf,None)
			crdate=get_val('CRDATE',each_dbf,None)
			crtime=get_val('CRTIME',each_dbf,None)
			todate=get_val('TODATE',each_dbf,None)
			data_source='KARVY'

			tupleRow1=(foliochk,product,bal_date,clos_bal,rupee_bal,brok_dlr_c,last_trxn,euin,pan,amc_code,folio_old,scheme_fol,scheme,divopt,funddesc,balunits,pldg,time_format_change(trdate),trdesc,moh,sbcode,pout,inv_id,invname,add1,add2,add3,city,inv_pin,rphone,ophone,fax,email,valinv,inav,time_format_change(crdate),crtime,time_format_change(todate),data_source)

			queries="SELECT index_name FROM read_config WHERE file_type='aum'AND rta_agent='KARVY'" 
			cursor.execute(queries)
			x=cursor.fetchall()
			lst1=[]
			for i in range(5):

				#print(x[i]['index_name'])
				if x[i]['index_name']=='foliochk':
					lst1.append(foliochk)
				if x[i]['index_name']=='product':
					lst1.append(product)
				if x[i]['index_name']=='bal_date':
					lst1.append(bal_date)
				if x[i]['index_name']=='clos_bal':
					lst1.append(clos_bal)
				if x[i]['index_name']=='rupee_bal':
					lst1.append(rupee_bal)

			tupleRow2=tuple(lst1)
			#print("tupleRow2                ....................",tupleRow2)

			quries=dynamic_quries(tupleRow2)
			#print("quries ",quries)
			value=valuefromtuple(tupleRow2)
			tupleRow=tuple_converter(tupleRow1)
			
			
			query_1=f"{quries}"
			cursor.execute(query_1,value)
			rows=cursor.fetchall()
			length_row=len(rows)
			#print('length_row                 ......................',length_row)
			#print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",rows)


			sql_queries="INSERT INTO aum_master (foliochk,product,bal_date,clos_bal,rupee_bal,brokcode,last_trxn,euin,pan,amc_code,folio_old,scheme_fol,scheme,divopt,funddesc,bal_units,pldg,trdate,trdesc,moh,sbcode,pout,inv_id,invname,add1,add2,add3,city,inv_pn,rphone,ophone,fax,email,valinv,inav,crdate,crtime,todate,data_source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
			if length_row ==0:
				cursor.execute(sql_queries,tupleRow)
			else:
				duplicate.append(tupleRow)
				#print(duplicate)
				

		# try:
		# 	timestr = time.strftime("%Y%m%d-%H%M%S")
		# 	archive_name = DEST_PATH+'txn_archive_' + str(timestr) + '.dbf'
		# 	shutil.move(filename,archive_name)
		# except Exception as e:
		# 	print(e)
	else:
		print("file not karvy")
	print(duplicate)
	timestr = time.strftime("%Y%m%d-%H%M%S")
	df1 = pd.DataFrame(duplicate,columns =column_lst)
	no_of_row=df1.shape[0]
	excel_name='aum_duplicate_report_karvy'+str(timestr)+'.xlsx'
	datatoexcel=pd.ExcelWriter(excel_name)
	df1.to_excel(datatoexcel)
	datatoexcel.save()
	filename2=PROJECT_ROOT+f"/{excel_name}"
	archive_name2=PROJECT_ROOT+'/duplicate/aum'
	shutil.move(filename2,archive_name2)
	if no_of_row >0:
		print(f"{no_of_row} Duplicate rows found.Report send to {receiver_email}")
		filePath=archive_name2+f"/{excel_name}"
		for each_mail_id in receiver_email:
			sendMailWithAttachments(subject, filePath,each_mail_id, body)
		print("********mail send**********")


elif re.fullmatch(pat2, name_of_textfile):


	dbf = DBF(filename)
	df = pd.DataFrame(iter(dbf))
	lst2=df.columns
	if lst2[0]=='FOLIOCHK' and lst2[1]=='PRODUCT' and lst2[2]=='BAL_DATE':
		timestr1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		dSql = "UPDATE aum_master SET delete_flag = 1,modified_at=%s WHERE delete_flag=0"
		cursor.execute(dSql,(timestr1))
		for each_dbf in dbf:
			#print("each_dbf>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",each_dbf)
			foliochk=get_val('FOLIOCHK',each_dbf,None)
			product=get_val('PRODUCT',each_dbf,None)
			bal_date=get_val('BAL_DATE',each_dbf,None)
			clos_bal=get_val('CLOS_BAL',each_dbf,None)
			rupee_bal=get_val('RUPEE_BAL',each_dbf,None)

			brok_dlr_c=get_val('BROK_DLR_C',each_dbf,None)
			last_trxn=get_val('LAST_TRXN_',each_dbf,None)
			euin=get_val('EUIN',each_dbf,None)
			pan=get_val('pan',each_dbf,None)
			amc_code=get_val('AMC_CODE',each_dbf,None)
			folio_old=get_val('FOLIO_OLD',each_dbf,None)
			scheme_fol=get_val('SCHEME_FOL',each_dbf,None)
			scheme=get_val('',each_dbf,None)
			divopt=get_val('',each_dbf,None)
			funddesc=get_val('',each_dbf,None)
			balunits=get_val('',each_dbf,None)
			pldg=get_val('',each_dbf,None)
			trdate=get_val('',each_dbf,None)
			trdesc=get_val('',each_dbf,None)
			moh=get_val('',each_dbf,None)
			sbcode=get_val('',each_dbf,None)
			pout=get_val('',each_dbf,None)
			inv_id=get_val('',each_dbf,None)
			invname=get_val('',each_dbf,None)
			add1=get_val('',each_dbf,None)
			add2=get_val('',each_dbf,None)
			add3=get_val('',each_dbf,None)
			city=get_val('',each_dbf,None)
			inv_pin=get_val('',each_dbf,None)
			rphone=get_val('',each_dbf,None)
			ophone=get_val('',each_dbf,None)
			fax=get_val('',each_dbf,None)
			email=get_val('',each_dbf,None)
			valinv=get_val('',each_dbf,None)
			inav=get_val('',each_dbf,None)
			crdate=get_val('',each_dbf,None)
			crtime=get_val('',each_dbf,None)
			todate=get_val('',each_dbf,None)	
			data_source='CAMS'

			tupleRow1=(foliochk,product,bal_date,clos_bal,rupee_bal,brok_dlr_c,last_trxn,euin,pan,amc_code,folio_old,scheme_fol,scheme,divopt,funddesc,balunits,pldg,trdate,trdesc,moh,sbcode,pout,inv_id,invname,add1,add2,add3,city,inv_pin,rphone,ophone,fax,email,valinv,inav,crdate,crtime,todate,data_source)

			tupleRow=tuple_converter(tupleRow1)
			
			sql_queries="INSERT INTO aum_master (foliochk,product,bal_date,clos_bal,rupee_bal,brokcode,last_trxn,euin,pan,amc_code,folio_old,scheme_fol,scheme,divopt,funddesc,bal_units,pldg,trdate,trdesc,moh,sbcode,pout,inv_id,invname,add1,add2,add3,city,inv_pn,rphone,ophone,fax,email,valinv,inav,crdate,crtime,todate,data_source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
			cursor.execute(sql_queries,tupleRow)



		try:
			timestr = time.strftime("%Y%m%d-%H%M%S")
			archive_name = DEST_PATH+'aum_archive_' + str(timestr) + '.dbf'
			shutil.move(filename,archive_name)
		except Exception as e:
			print(e)
	else:
		print("not match cams file")
	

else:
	print('file not match')
#print("duplicate                 ",duplicate)
total_time_taken = datetime.datetime.now()-start
print("total_time_taken           -",total_time_taken)

print("*********************************finised processing************************************************")


































