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
        Config={
             "candle.width":None,
             "candle.date.from":None,
             "candle.date.to":None,
             "candle.time.from":None,
             "candle.time.to":None
        }# Создаем словарь, содержащий параметры; будем постепенно заполнять его, читая конфигурационный файл
        for prop in root.findall("property"):
          name=prop.find("name").text
          if name=="candle.width":
               Config[name]=int(prop.find("value").text)
          elif name=="candle.date.from" or name=="candle.date.to":
               Config[name]=datetime.strptime(prop.find("value").text,"%Y%m%d").date()
          elif name=="candle.time.from" or name=="candle.time.to":
               Config[name]=datetime.strptime(prop.find("value").text,"%H%M").time()
        # Заполним недостающие значения по умолчанию
        for el in Config:
             if Config[el]==None:
                  Config[el]=Default[el]
        return Config

if len(sys.argv)>1:
    Config=xml_Parse(sys.argv[1])
else:
     Config=Default
# Запуск Spark сессии и переход от датафрейма к RDD
spark=SparkSession.builder.appName("Cndl").getOrCreate()
data=spark.read.csv(sys.argv[2],header=True,inferSchema=True)
#data.printSchema()
RDD=data.rdd

# Функция, округляющая до десятых числа (round работал не всегда корректно)
def accur(elem):
     if int(elem*100)%10<5:
          return int(elem*10)/10
     else:
          return int(elem*10+1)/10

# Функция, маппирующая RDD в ((#SYMBOL,interval),(PRICE_DEAL,ID_DEAL,Moment))
def map_func(deal,Config):
     Symbol=deal["#SYMBOL"]
     Price=accur(float(deal["PRICE_DEAL"]))
     Id=int(deal["ID_DEAL"])
     Moment=datetime.strptime(str(deal["MOMENT"]),"%Y%m%d%H%M%S%f")
     begin=datetime.combine(Config["candle.date.from"],Config["candle.time.from"])
     end=datetime.combine(Config["candle.date.to"],Config["candle.time.to"])
     if not(begin<=Moment<end): # Попадает ли сделка в рассматриваемый интервал?
          return None
     delta=Moment-begin
     delta_milli=int(delta.total_seconds()*1000)
     interval_milli=(delta_milli//Config["candle.width"])*Config["candle.width"]
     interval=begin+timedelta(milliseconds=interval_milli)
     return ((Symbol,interval),(Price,Id,Moment))

RDD=RDD.map(lambda x: map_func(x,Config))
RDD=RDD.filter(lambda x: x is not None)

# Функция-обработчик сделок, входящих в одну свечь
def reduce_func(key,deals):
     symbol=key[0]
     moment=key[1].strftime("%Y%m%d%H%M%S%f")[:-3]
     sorted_elem=sorted(deals,key=lambda x: (x[2],x[1])) # Сортируем сделки по времени + по Id
     start=(sorted_elem[0][0])
     end=(sorted_elem[-1][0])
     high=max((deal[0]) for deal in sorted_elem)
     low=min((deal[0]) for deal in sorted_elem)
     return f"{symbol},{moment},{float(start):.1f},{float(high):.1f},{float(low):.1f},{float(end):.1f}"

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







     


     

     
     
     
     