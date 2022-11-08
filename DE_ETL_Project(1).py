import pandas as pd
import mysql.connector as me

#Import global air pollution datasets
data_frame=pd.read_csv('global air pollution dataset.csv').dropna()

#Convert DataFrame into List of Tupels
Plist=[]
for i,row in data_frame.iterrows():
    Plist.append(tuple(row))


try:
    # Connect with Database
    con=me.connect(host='localhost',user='root',password='12345',database='uditdb')
except Exception as e:
    print(e)
finally:
    cur=con.cursor()


try:
    # Drop Table if Exist and create new table in MYSQL Database
    cur.execute('drop table if exists air_pollution')
    cur.execute('create table if not exists air_pollution(Country varchar(2000),City varchar(200),AQI_Value int(100),AQI_Category varchar(200),CO_AQI_Value int(100),'
            'CO_AQI_Category varchar(200),Ozone_AQI_Value int(100),Ozone_AQI_Category varchar(200),'
            'NO2_AQI_Value int(100),NO2_AQI_Category varchar(200),PM25_AQI_Value int(100),PM25_AQI_Category varchar(200));')
    #parameterized query
    query=('insert into air_pollution(Country,City,AQI_Value,AQI_Category,CO_AQI_Value,CO_AQI_Category,Ozone_AQI_Value,Ozone_AQI_Category,'
        'NO2_AQI_Value,NO2_AQI_Category,PM25_AQI_Value,PM25_AQI_Category) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
except Exception as e:
    print(e)


try:
    #Insert DataFrame in to DataBase
    cur.executemany(query,Plist)
except Exception as e:
    print(e)
finally:
    #Commit Operation
    con.commit()
print('Data Load Successfully into Database')