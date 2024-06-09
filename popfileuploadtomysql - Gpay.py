import datetime
import logging
from pathlib import Path
import pandas as pd
import pymysql
from pymysql.constants import CLIENT
import pathlib

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 200)

# change filepath
pathold = r"C:\Users\rohan\Downloads\247 - 499s - prefix pop upload date 9jun 5lac.csv"
pathold = pathlib.PureWindowsPath(pathold)
path = pathold.as_posix()
df = pd.read_csv(path, header=None, encoding='unicode_escape')
df = pd.DataFrame(df)
print(df.head())
print(df.shape)
# POPLIP399IA6JZWI

# run if needed
# remove columns
df = df.drop([0], axis=1)
df = df.drop([1], axis=1)
df = df.drop([2], axis=1)
df = df.drop([3], axis=1)
df = df.drop([4], axis=1)
df = df.drop([5], axis=1)
df = df.drop([6], axis=1)
df = df.drop([7], axis=1)
df = df.drop([8], axis=1)
df = df.drop([9], axis=1)
df = df.drop([10], axis=1)
# POPLIP399IA6JZWI

# df.columns = ('vouchers', 'filesource')
# df.columns = ('vouchers', 'filesource', 'couponsource','filename')
df.columns = ['vouchers']
df = df.iloc[1:, :]
print(df.head())

# GPay 500off_6Dec22_Affinity  Gpay MAH299_12Dec22  Gpay 250off_1Nov22_Open  Gpay 200off_1Dec22_Open
# change offername
df['filesource'] = "Gpay_Buy_3_499"  # GPay 400off_01May22   example: gpay_299off_Oct # Gpay 333off_15Apr22
# couponsource
df['couponsource'] = 'Gpay'  # example : "Gpay", "Phone Pe"
# df['filesource'] = "Store Visit Activity"  # example: gpay_299off_Oct # Gpay 333off_15Apr22
# df['couponsource'] = 'Store Visit Activity'  # example : "Gpay"

df['filename'] = Path(path).name
print('character length for filename is ' + str(len(Path(path).name)))
df['last_updated_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
cols = ('vouchers', 'filesource', 'couponsource', 'filename', 'last_updated_date')
df.columns = cols
print(df.head())
print(df.shape)
# df2 = df.iloc[12500001:, :]
# df2.shape
cols = ",".join([str(i) for i in df.columns.tolist()])
df.shape

# run all codes below
print('raw data processed')
rds_endpoint = "deliveryrdsdatabase.c13rd4d5lne0.ap-south-1.rds.amazonaws.com"
username = "admin"
password = "sugarbwa"  # RDS Mysql password
db_name = "vouchers"  # RDS MySQL DB name
conn = None

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_endpoint, user=username, passwd=password, db=db_name, connect_timeout=5,
                           client_flag=CLIENT.MULTI_STATEMENTS)
except pymysql.MySQLError as e:
    print("ERROR: Unexpected error: Could not connect to MySQL instance.")

# bulk upload
cur = conn.cursor()
# csv_data = csv.reader(file('file_name'))
my_data = []
for i, row in df.iterrows():
    my_data.append(tuple(row))

type(my_data)
query = f"INSERT INTO vouchers.popvoucherstable ({cols}) VALUES (" + "%s," * (
        len(row) - 1) + "%s) ON DUPLICATE KEY UPDATE filesource = VALUES(filesource), couponsource = VALUES(couponsource), filename = VALUES(filename),last_updated_date = VALUES(last_updated_date)"

cur.executemany(query, my_data)
conn.commit()
cur.close()
conn.close()
print('execute many completed')

################################################      END     ##########################################3


# check = df[df['vouchers']=='SP599ZQ40M22']

#
# #check if updated
# cur = conn.cursor()
# query = f"select count(vouchers), filename from ordersdb.voucherstable group by filename;"
# cur.execute(query)
# conn.commit()
# cur.close()


# print('waiting 30 seconds')
# time.sleep(30)
# print('trying')
#
# # upload row by row
#
# with conn.cursor() as cur:
#     for i, row in df.iterrows():
#         sql = f"INSERT INTO ordersdb.voucherstable ({cols}) VALUES (" + "%s," * (
#                 len(row) - 1) + "%s) ON DUPLICATE KEY UPDATE filesource = VALUES(filesource), couponsource = VALUES(couponsource), filename = VALUES(filename)"
#         cur.execute(sql, tuple(row))
#         # the connection is not autocommitted by default, so we must commit to save our changes
#         conn.commit()
# print('data inserted in rds temp')
# cur.close()
# print('Process completed')


# import pandas as pd
# import numpy as np
# offer_analysis = pd.read_excel('D://PRASHANT//Add_on_works//rinu//offer_analysis_raw_data(mar22 to apr 23).xlsx')
# offer_analysis['month'] = pd.to_datetime(offer_analysis['created_at']).dt.strftime('%Y-%m')
# offer_analysis['year'] = pd.to_datetime(offer_analysis['created_at']).dt.strftime('%Y')
# offer_analysis['new_repeat'] = np.where(pd.to_numeric(offer_analysis['customer_orders_count'])>1,'repeat','new')
#
# offer_analysis.to_excel('D://PRASHANT//Add_on_works//rinu//offer_analysis_raw_data(mar22 to apr 23)c.xlsx')
