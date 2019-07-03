import psycopg2
import datetime as dt
#填写生成文件后存放路径 filepath
#填写检查时间的：date和time
# 参照时间按照"yyyy-mm-dd hh:mm:ss"格式填写，若默认参照时间为12小时前，datetime_compare = ""
filepath = ""
date = ""
time = ""
datetime_compare = ""


conn = psycopg2.connect(database='',user="", password="",host="", port="")
cur = conn.cursor()

def query(datetime,account):
    datetime = "' " + datetime + "'"
    s = "SELECT usdt_value , balance , margin_rate,realized_pnl_rate,xbt_rate,eth_rate" \
            "  from net_value where account = "+ account + \
          "and datetime <= "+datetime+"order by datetime desc limit 1;"
    cur.execute(s)
    rows = cur.fetchall()
    if len(rows)==0 :
        print(account+ '在net_value上没有记录')
    usd_value = rows[0][0]
    btc_value = rows[0][1]
    margin_rate = rows[0][2]
    realized_pnl_rate = rows[0][3]
    xbt_rate = rows[0][4]
    eth_rate = rows[0][5]
    query = "SELECT liq_pct FROM pos_misc" \
            " where account = "+ account + \
            " and typ = 'XBT_USD'" \
            " and datetime<="+datetime+" order by datetime desc limit 1"
    cur.execute(query)
    rows = cur.fetchall()
    xbt_liq_pct = rows[0][0]  if len(rows)>0 else 0
    s = "SELECT liq_pct FROM pos_misc" \
            " where account = "+ account + \
            " and typ = 'ETH_USD'" \
            " and datetime<="+datetime+" order by datetime desc limit 1"
    cur.execute(s)
    rows = cur.fetchall()
    eth_liq_pct = rows[0][0] if len(rows)>0 else 0

    return usd_value,btc_value,margin_rate,realized_pnl_rate,xbt_rate,eth_rate,xbt_liq_pct,eth_liq_pct

datetime = date+ ' ' + time
datetime = dt.datetime.strptime(datetime, '%Y-%m-%d %H:%M:%S')
datetime1 = (datetime + dt.timedelta(hours = -8)).strftime("%Y-%m-%d %H:%M:%S")
if datetime_compare == "":
    datetime_compare = (datetime + dt.timedelta(hours = -12)).strftime("%Y-%m-%d %H:%M:%S")
datetime_compare = dt.datetime.strptime(datetime_compare, '%Y-%m-%d %H:%M:%S')
datetime2 = (datetime_compare + dt.timedelta(hours = -8)).strftime("%Y-%m-%d %H:%M:%S")
datetime2 = "' " + datetime2 + "'"
datetime_compare = datetime_compare.strftime("%Y-%m-%d %H:%M:%S")

#bitmex_4a:
usd_value,btc_value,margin_rate,realized_pnl_rate,xbt_rate,eth_rate,xbt_liq_pct,eth_liq_pct = query(datetime1,"'bitmex_4a'")
s = "SELECT usdt_value" \
        "  from net_value where account = "+ "'bitmex_4a'" + \
      "and datetime <= "+datetime2+" order by datetime desc limit 1;"
cur.execute(s)
rows = cur.fetchall()
last_usd_value =  rows[0][0]
dict1 = {
    "时间":datetime,
    "账户":"bitmex_4a",
    "权益本位":"USD",
    "净值": usd_value/36753.75,
    "净值增减": (usd_value-last_usd_value)/36753.75,
    "净资产":usd_value,
    "保证金率":"%.2f%%" % (margin_rate*100),
    "对冲率(realized_pnl_rate)":"%.2f%%" % (realized_pnl_rate*100),
    "XBTUSD距强平率":"%.2f%%" % (xbt_liq_pct*100),
    "ETHXBT距强平率":"%.2f%%" % (eth_liq_pct*100),
    "无套保程序化最大仓位限制":"%.2f%%" % (200),
    "当前无套保程序化最大总仓位":"%.2f%%" % ((xbt_rate+eth_rate)*100),
    "XBT无套保程序化最大仓位":"%.2f%%" % (xbt_rate*100),
    "ETH无套保程序化最大仓位":"%.2f%%" % (eth_rate*100),
    "手工仓位":'-%',
}


#bitmex_4b:
usd_value,btc_value,margin_rate,realized_pnl_rate,xbt_rate,eth_rate,xbt_liq_pct,eth_liq_pct = query(datetime1,"'bitmex_4b'")
s = "SELECT balance" \
        "  from net_value where account = "+ "'bitmex_4b'" + \
      "and datetime <= "+datetime2+" order by datetime desc limit 1;"
cur.execute(s)
rows = cur.fetchall()
last_btc_value =  rows[0][0]
dict2 = {
    "时间":datetime,
    "账户":"bitmex_4b",
    "权益本位":"XBT",
    "净值": btc_value/53.55526,
    "净值增减": (btc_value-last_btc_value)/53.55526,
    "净资产":btc_value,
    "保证金率":"%.2f%%" % (margin_rate*100),
    "对冲率":'-',
    "XBTUSD距强平率":"%.2f%%" % (xbt_liq_pct*100),
    "ETHXBT距强平率":"%.2f%%" % (eth_liq_pct*100),
    "无套保程序化最大仓位限制":"%.2f%%" % (200),
    "当前无套保程序化最大总仓位":"%.2f%%" % ((xbt_rate+eth_rate)*100),
    "XBT无套保程序化最大仓位":"%.2f%%" % (xbt_rate*100),
    "ETH无套保程序化最大仓位":"%.2f%%" % (eth_rate*100),
    "手工仓位":'-%',
}


#bitmex_666a:
usd_value,btc_value,margin_rate,realized_pnl_rate,xbt_rate,eth_rate,xbt_liq_pct,eth_liq_pct = query(datetime1,"'bitmex_666a'")
s = "SELECT usdt_value" \
        "  from net_value where account = "+ "'bitmex_666a'" + \
      "and datetime <= "+datetime2+" order by datetime desc limit 1;"
cur.execute(s)
rows = cur.fetchall()
last_usd_value =  rows[0][0]
dict3 = {
    "时间":datetime,
    "账户":"bitmex_666a",
    "权益本位":"USD",
    "净值": usd_value/8906,
    "净值增减": (usd_value-last_usd_value)/8906,
    "净资产":usd_value,
    "保证金率":"%.2f%%" % (margin_rate*100),
    "对冲率(realized_pnl_rate)":"%.2f%%" % (realized_pnl_rate*100),
    "XBTUSD距强平率":"%.2f%%" % (xbt_liq_pct*100),
    "ETHXBT距强平率":"%.2f%%" % (eth_liq_pct*100),
    "无套保程序化最大仓位限制":"%.2f%%" % (200),
    "当前无套保程序化最大总仓位":"%.2f%%" % ((xbt_rate+eth_rate)*100),
    "XBT无套保程序化最大仓位":"%.2f%%" % (xbt_rate*100),
    "ETH无套保程序化最大仓位":"%.2f%%" % (eth_rate*100),
    "手工仓位":'-%',
}

#bitmex_5:
usd_value,btc_value,margin_rate,realized_pnl_rate,xbt_rate,eth_rate,xbt_liq_pct,eth_liq_pct = query(datetime1,"'bitmex_5'")
s = "SELECT balance" \
        "  from net_value where account = "+ "'bitmex_5'" + \
      "and datetime <= "+datetime2+" order by datetime desc limit 1;"
cur.execute(s)
rows = cur.fetchall()
if len(rows) == 0:
    print('bitmex_5 在net_value上没有记录, 按初始值10.01个btc计算')
    last_btc_value = 10.01
else :
    last_btc_value = rows[0][0]
dict4 = {
    "时间":datetime,
    "账户":"bitmex_5",
    "权益本位":"XBT",
    "净值": btc_value/10.01,
    "净值增减": (btc_value-last_btc_value)/10.01,
    "净资产":btc_value,
    "保证金率":"%.2f%%" % (margin_rate*100),
    "对冲率":'-',
    "XBTUSD距强平率":"%.2f%%" % (xbt_liq_pct*100),
    "ETHXBT距强平率":"%.2f%%" % (eth_liq_pct*100),
    "无套保程序化最大仓位限制":"%.2f%%" % (400),
    "当前无套保程序化最大总仓位":"%.2f%%" % ((xbt_rate+eth_rate)*100),
    "XBT无套保程序化最大仓位":"%.2f%%" % (xbt_rate*100),
    "ETH无套保程序化最大仓位":"%.2f%%" % (eth_rate*100),
    "手工仓位":'-%',
}

#bitmex_a1:
usd_value,btc_value,margin_rate,realized_pnl_rate,xbt_rate,eth_rate,xbt_liq_pct,eth_liq_pct = query(datetime1,"'bitmex_a1'")
s = "SELECT balance" \
        "  from net_value where account = "+ "'bitmex_a1'" + \
      "and datetime <= "+datetime2+" order by datetime desc limit 1;"
cur.execute(s)
rows = cur.fetchall()
if len(rows) == 0:
    print('bitmex_a1 在net_value上没有记录, 按初始值15个btc计算')
    last_btc_value = 15.0
else:
    last_btc_value = rows[0][0]
dict5 = {
    "时间":datetime,
    "账户":"bitmex_a1",
    "权益本位":"XBT",
    "净值": btc_value/15,
    "净值增减": (btc_value-last_btc_value)/15,
    "净资产":btc_value,
    "保证金率":"%.2f%%" % (margin_rate*100),
    "对冲率":'-',
    "XBTUSD距强平率":"%.2f%%" % (xbt_liq_pct*100),
    "ETHXBT距强平率":"%.2f%%" % (eth_liq_pct*100),
    "无套保程序化最大仓位限制":"%.2f%%" % (200),
    "当前无套保程序化最大总仓位":"%.2f%%" % ((xbt_rate+eth_rate)*100),
    "XBT无套保程序化最大仓位":"%.2f%%" % (xbt_rate*100),
    "ETH无套保程序化最大仓位":"%.2f%%" % (eth_rate*100),
    "手工仓位":'-%',
}

#bitmex_5a:
usd_value,btc_value,margin_rate,realized_pnl_rate,xbt_rate,eth_rate,xbt_liq_pct,eth_liq_pct = query(datetime1,"'bitmex_5a'")
s = "SELECT balance" \
        "  from net_value where account = "+ "'bitmex_5a'" + \
      "and datetime <= "+datetime2+" order by datetime desc limit 1;"
cur.execute(s)
rows = cur.fetchall()
if len(rows) == 0:
    print('bitmex_5a 在net_value上没有记录, 按初始值7.8440个btc计算')
    last_btc_value = 7.8440
else:
    last_btc_value = rows[0][0]
dict6 = {
    "时间":datetime,
    "账户":"bitmex_5a",
    "权益本位":"XBT",
    "净值": btc_value/7.8440,
    "净值增减": (btc_value-last_btc_value)/7.8440,
    "净资产":btc_value,
    "保证金率":"%.2f%%" % (margin_rate*100),
    "对冲率":'-',
    "XBTUSD距强平率":"%.2f%%" % (xbt_liq_pct*100),
    "ETHXBT距强平率":"%.2f%%" % (eth_liq_pct*100),
    "无套保程序化最大仓位限制":"%.2f%%" % (200),
    "当前无套保程序化最大总仓位":"%.2f%%" % ((xbt_rate+eth_rate)*100),
    "XBT无套保程序化最大仓位":"%.2f%%" % (xbt_rate*100),
    "ETH无套保程序化最大仓位":"%.2f%%" % (eth_rate*100),
    "手工仓位":'-%',
}



#datetime = dt.datetime.strptime(datetime, '%Y-%m-%d %H:%M:%S')
fname = dt.datetime.strftime(datetime,"%Y-%m-%d-%H-%M-%S")
f = open(filepath+fname+".txt",'a')
f.write('bitmex 账户常规检查   参照时间：'+datetime_compare+'\n')
f.write("--------------------\n")
for key,value in dict1.items():
    f.write(key+' : '+str(value))
    f.write("\n")
f.write("--------------------\n")
for key,value in dict2.items():
    f.write(key+' : '+str(value))
    f.write("\n")
f.write("--------------------\n")
for key,value in dict3.items():
    f.write(key+' : '+str(value))
    f.write("\n")
f.write("--------------------\n")
for key,value in dict4.items():
    f.write(key+' : '+str(value))
    f.write("\n")
f.write("--------------------\n")
for key,value in dict5.items():
    f.write(key+' : '+str(value))
    f.write("\n")
f.write("--------------------\n")
for key,value in dict6.items():
    f.write(key+' : '+str(value))
    f.write("\n")
f.write("--------------------\n")
f.close()
print(datetime,datetime_compare,datetime1,datetime2)