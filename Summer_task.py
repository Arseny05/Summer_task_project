import sys
import pyspark
import xml.etree.ElementTree as ET
from datetime import datetime, date, time,timedelta
import csv
import os
from pyspark.sql import SparkSession

Default={
    "candle.width":300000,
    "candle.date.from":date(1900,1,1),
    "candle.date.to":date(2020,1,1),
    "candle.time.from":time(10,0),
    "candle.time.to":time(18,0)
    }

# Парсинг xml-файла в словарь Config
def xml_Parse(str):
        try:
            tree=ET.parse(str)
            root=tree.getroot()
        except Exception:
             return Default
        if root.find("candle.width")==None:
             width=Default["candle.width"]
        else:
          width=int(root.find("candle.width").text)
        if root.find("candle.date.from")==None:
          date_from=Default["candle.date.from"]
        else:
          date_from=datetime.strptime(root.find("candle.date.from").text,"%Y%m%d").date()
        if root.find("candle.date.to")==None:
             date_to=Default["candle.date.to"]
        else:
          date_to=datetime.strptime(root.find("candle.date.to").text,"%Y%m%d").date()
        if root.find("candle.time.from")==None:
            time_from=Default["candle.time.from"]
        else:
          time_from=datetime.strptime(root.find("candle.time.from").text,"%H%M").time()
        if root.find("candle.time.to")==None:
            time_to=Default["candle.time.to"]
        else:
          time_to=datetime.strptime(root.find("candle.time.to").text,"%H%M").time()
        Config={
                "candle.width":width,
                "candle.date.from":date_from,
                "candle.date.to":date_to,
                "candle.time.from":time_from,
                "candle.time.to":time_to
            }
        return Config

if len(sys.argv)>1:
    Config=xml_Parse(sys.argv[1])
else:
     Config=Default

# Запуск Spark сессии и переход от датафрейма к RDD
spark=SparkSession.builder.appName("Cndl").getOrCreate()
data=spark.read.csv("/home/arseny/MSU/Python_language/fin_sample.csv",header=True,inferSchema=True)
#data.printSchema()
RDD=data.rdd

# Функция, маппирующая RDD в ((#SYMBOL,interval),(PRICE_DEAL,ID_DEAL,Moment))
def map_func(deal,Config):
     Symbol=deal["#SYMBOL"]
     Price=round(float(deal["PRICE_DEAL"]),1)
     Id=int(deal["ID_DEAL"])
     Moment=datetime.strptime(str(deal["MOMENT"]),"%Y%m%d%H%M%S%f")
     begin=datetime.combine(Config["candle.date.from"],Config["candle.time.from"])
     end=datetime.combine(Config["candle.date.to"],Config["candle.time.to"])
     if not(begin<=Moment<end): # Попадает ли сделка в рассматриваемый интервал?
          return None
     delta=Moment-begin
     delta_milli=delta.total_seconds()*1000
     interval_milli=(delta_milli// Config["candle.width"])*Config["candle.width"]
     interval=begin+timedelta(milliseconds=interval_milli)
     return ((Symbol,interval),(Price,Id,Moment))

RDD=RDD.map(lambda x: map_func(x,Config))
RDD=RDD.filter(lambda x: x is not None)

# Функция-обработчик сделок, входящих в одну свечь
def reduce_func(key,deals):
     symbol=key[0]
     moment=key[1].strftime("%Y%m%d%H%M%S")+"000"
     sorted_elem=sorted(deals,key=lambda x: (x[2],x[1])) # Сортируем сделки по времени + по Id
     start=sorted_elem[0][0]
     end=sorted_elem[-1][0]
     high=max(deal[0] for deal in sorted_elem)
     low=min(deal[0] for deal in sorted_elem)
     return f"{symbol},{moment},{start:.1f},{high:.1},{low:.1},{end:.1}"

RDD=RDD.groupByKey()
result=RDD.map(lambda x: reduce_func(x[0],x[1]))

# Функция, вычленяющая первый символ для группировки по отдельным файлам получившейся последовательности
def get_symbol(str):
     return str.split(",")[0]

os.makedirs("Solution",exist_ok=True) # Создаем директорию для файлов со свечами
result=result.map(lambda x:(get_symbol(x),x)) # Преобразуем последовательность свечей в (Symbol,Candle)
result=result.groupByKey()

# Функция, сохраняющая последовательности свечей с одинаковым символом в файл
def in_file(symbol,lines,out_dir):
     filename=os.path.join(out_dir,f"{symbol}.csv")
     with open(filename,"w") as f:
          lines=sorted(lines)
          for i in lines:
               f.write(i+"\n")

result.foreach(lambda x: in_file(x[0],x[1],"Solution"))
spark.stop()







     


     

     
     
     
     