import psycopg2
import datetime as dt
import pandas as pd


conn = psycopg2.connect(database='',user="", password="",host="", port="")
cur = conn.cursor()

def query(datetime,account):
    datetime = "' " + datetime + "'"
    s = "SELECT usdt_value , balance , margin_rate, heding_rate,xbt_rate,eth_rate" \
            "  from net_value where account = "+ account + \
          "and datetime <= "+datetime+"order by datetime desc limit 1;"
    cur.execute(s)
    rows = cur.fetchall()
    usd_value = rows[0][0]
    btc_value = rows[0][1]

    return usd_value,btc_value
date = "2019-03-14"
time = "20:00:00"
datetime = date+ ' ' + time
datetime = dt.datetime.strptime(datetime, '%Y-%m-%d %H:%M:%S')
datetime1 = (datetime + dt.timedelta(hours = -8)).strftime("%Y-%m-%d %H:%M:%S")
date_end = "2019-4-17 14:00:00"
date_end = dt.datetime.strptime(date_end, '%Y-%m-%d %H:%M:%S')

usd_list = []
btc_list = []
date_list = []

#datetime = datetime.strftime("%Y-%m-%d %H:%M:%S")
while(datetime <= date_end):
    usd_value,btc_value = query(datetime1,"'bitmex_666a'")
    usd_list.append(usd_value)
    btc_list.append(btc_value)
    date_list.append(datetime.strftime("%Y-%m-%d %H:%M:%S"))

    datetime = (datetime+dt.timedelta(hours = 24))
    datetime1 = (datetime + dt.timedelta(hours=-8)).strftime("%Y-%m-%d %H:%M:%S")



df = pd.DataFrame()
df['date'] = date_list
df['usd_value'] = usd_list
df['btc_value'] = btc_list

#df.to_csv('666a_netValue.csv')

