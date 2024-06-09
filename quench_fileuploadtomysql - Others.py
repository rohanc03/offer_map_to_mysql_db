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

pathold = "D://PRASHANT//file_upload_to_mysql//QUENCH//dt_24_08_2023//Voucher Quench buy 3 mini at 499 Q3MA499 27th July.csv"
pathold = pathlib.PureWindowsPath(pathold)
path = pathold.as_posix()
# df = pd.read_excel(path, header=None)
df = pd.read_csv(path, header=None, encoding='unicode_escape')
df = pd.DataFrame(df)
print(df.head())
# remove columns
df = df.drop([0], axis=1)
df = df.drop([1], axis=1)
# df = df.drop([2], axis=1)
df = df.drop([3], axis=1)
df = df.drop([4], axis=1)
# df = df[0].append(df[1])


# df.columns = ('vouchers', 'filesource')
# df.columns = ('vouchers', 'filesource', 'couponsource','filename')
df.columns = ['vouchers']
df = df.iloc[1:, :]
print(df.head())
# df['vouchers'] = df['vouchers'].str.replace('\t','',regex=False)
# print(df.head())
df[
    'filesource'] = "GPay_3MinisAt499_27July_23"  # example: gpay_299off_Oct # Gpay 333off_15Apr22    --- #CDT Team Manual
df['couponsource'] = "GPay"  # example : "Gpay" NAME OF PUBLISHER --- #CDT Team
# df['filesource'] = "Store Visit Activity"  # example: gpay_299off_Oct # Gpay 333off_15Apr22
# df['couponsource'] = 'Store Visit Activity'  # example : "Gpay"

df['filename'] = Path(path).name
df['last_updated_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('character length for filename is ' + str(len(Path(path).name)))
cols = ('vouchers', 'filesource', 'couponsource', 'filename', 'last_updated_date')
df.columns = cols
print(df.head())
print(df.shape)
# df2.shape
cols = ",".join([str(i) for i in df.columns.tolist()])
df.shape
# df2 = df.iloc[12500001:, :]

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

query = f"INSERT INTO vouchers.quenchvoucherstable ({cols}) VALUES (" + "%s," * (
        len(row) - 1) + "%s) ON DUPLICATE KEY UPDATE filesource = VALUES(filesource), couponsource = VALUES(couponsource), filename = VALUES(filename),last_updated_date = VALUES(last_updated_date)"

cur.executemany(query, my_data)
conn.commit()
cur.close()
conn.close()
print('execute many completed')

#
# M8904320736965
# M8904320717582
# M8904320717643
# M8904320722395
# M8904320724849
# M8904320724887
# M8904320724894
# M8904320717339
# M8904320717421
# M8904320717483
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
