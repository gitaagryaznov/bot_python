import telebot
from random import randint
import time
import pandas as pd
import requests
import re
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import os
#import gspread as gs
from sqlalchemy import create_engine,Column,String,Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import psycopg2
token = "6884325914:AAG-MCRsr6dGSB9LdXQHeRXTS7lnJAwfpaM"
bot = telebot.TeleBot(token)
# парсинг истории

@bot.message_handler(commands=['report'])
def st_message(message):
    if message.text=='/report':
        g=r"https://docs.google.com/spreadsheets/d/e/2PACX-1vSbX2ImtqkoEh1wOdFSMVEucwLkp0FF01KvntQexnieNgSaNPl4Tc8Z5HrHBF1GqQ/pub?gid=1769046576&single=true&output=csv"
        df = pd.read_csv(g)
        #Кол-во ШК  перевешено
        df=df[~df['ШК'].isnull()]
        df['ШК']=df['ШК'].astype('int64')
        google=r'https://docs.google.com/spreadsheets/d/e/2PACX-1vR8od_JorrGKUjGhGlv2hXSsSG5Wsj1Y69E7qfko0gugbfLbq0o5T04D2lPCrFVrckV1hxrN0B62bJX/pub?gid=0&single=true&output=csv'
        dbg=pd.read_csv(google)
        df.loc[(df['ШК'].isin(dbg['shk_id']))&(df['ID виновного'].isin(dbg['contragent_id'])),'перевес']=1
        df=df.merge(dbg,how='left',left_on=['ШК','ID виновного'], right_on=['shk_id','contragent_id'])
        df=df[~df['ШК'].isnull()]
        df['operation_dt']=pd.to_datetime(df['operation_dt'].fillna(pd.to_datetime('1990-01-01')))
        df['operation_dt']=pd.to_datetime(df['operation_dt']).apply(pd.to_datetime)
        df['operation_dt']=df['operation_dt'].dt.date
        df=df[pd.to_datetime(df['operation_dt'])>pd.to_datetime('2000-01-01')]
        df['operation_dt']=pd.to_datetime(df['operation_dt']).apply(pd.to_datetime)
        df=df[pd.to_datetime(df['operation_dt'])>pd.to_datetime('2000-01-01')]
        df=df.fillna(0)
        df['operation_dt']=pd.to_datetime(df['operation_dt']).apply(pd.to_datetime)
        df['month']=df['operation_dt'].dt.month_name(locale='Russian')
        df['month_n']=df['operation_dt'].dt.month
        df['year']=df['operation_dt'].dt.year
        df=df.sort_values('operation_dt')
        gr=df.groupby(['month','Кто от ФД внес данные о планируемом перевесе','month_n','year']).count().reset_index()
        gr=gr[(gr['month_n']==gr['month_n'].max())&(gr['year']==gr['year'].max())]
        gr=gr[['year','month','Кто от ФД внес данные о планируемом перевесе','ШК']]
        gr.columns=['year','month','фио','кол-во ШК']
        gr=gr.sort_values(['year','month','кол-во ШК'])

        host='fin-srv-greenplum-master.el.wb.ru'
        db='dwh'
        login='gp_dwh_usr_gryaznov'
        pwd='fg39408oyh3of4gbwioefg2iweygfvi2yfg3o4fugw'
        try:
            bot.send_message(message.chat.id, gr.to_string())
        except:
            pass
        shk_disput=pd.read_excel(r'BD.xlsx')
        shk_disput=shk_disput['shk_id']
        a=r'report.xlsx'
        shk_disput.to_excel(a,index=False)
        upfile = open(a, "rb")    # Октрытие файлов
        try:
            bot.send_document(message.chat.id, upfile)
        except:
            pass
        try:
            upfile.close()        # Закрытие всего
            os.remove(a)
        except:
            pass
        #try:
            #conn = psycopg2.connect(f"dbname='{db}' user='{login}' host='{host}' password='{pwd}'")
            #cur = conn.cursor()
            #sql=f"""select * from sdb.shk_disput"""
            #cur.close()
            #shk_disput=pd.read_sql(sql,conn)
            #a=r'report.xlsx'
            #shk_disput.to_excel(a)
            #upfile = open(a, "rb")    # Октрытие файлов
            #bot.send_document(message.chat.id, upfile)
            #upfile.close()        # Закрытие всего
            #os.remove(a)
        #except psycopg2.OperationalError:
                #pass
        #bot.send_document(message.chat.id, 'привет')

@bot.message_handler(content_types='text')
def start_message(message):
    #scope = ['https://www.googleapis.com/auth/spreadsheets',
     #    "https://www.googleapis.com/auth/drive"]
    #CREDENTIALS_FILE=r'creds.json'
  #  CREDENTIALS_FILE=CREDENTIALS_FILE.replace('\u202a','')
  #  credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
  #  client = gs.authorize(credentials)
    aaa=message.text
    aaa=aaa.lower().replace('шк','')
    aaa=aaa.split()
    a=[]
    add_to_work=[]
    for i in aaa:
        i=re.sub('\D', '', i)
        try:
            int(i)
            if int(i)>100000000:
                a.append(int(i))
        except ValueError:
            pass
    if len(a)==0:
        pass
    else:
        host='fin-srv-greenplum-master.el.wb.ru'
        db='dwh'
        login='gp_dwh_usr_gryaznov'
        pwd='fg39408oyh3of4gbwioefg2iweygfvi2yfg3o4fugw'
        a=list(set(a))
        g=r"https://docs.google.com/spreadsheets/d/e/2PACX-1vSbX2ImtqkoEh1wOdFSMVEucwLkp0FF01KvntQexnieNgSaNPl4Tc8Z5HrHBF1GqQ/pub?gid=1769046576&single=true&output=csv"
        df = pd.read_csv(g)
        df=df[~df['ШК'].isnull()]
        df['ШК']=df['ШК'].astype('int64')
        df=df.fillna(0)
        google=r'https://docs.google.com/spreadsheets/d/e/2PACX-1vR8od_JorrGKUjGhGlv2hXSsSG5Wsj1Y69E7qfko0gugbfLbq0o5T04D2lPCrFVrckV1hxrN0B62bJX/pub?gid=0&single=true&output=csv'
        dbg=pd.read_csv(google)
        dbg['operation_dt']=dbg['operation_dt'].fillna(pd.to_datetime('1990-01-01'))
        dbg['operation_dt']=dbg['operation_dt'].apply(pd.to_datetime)
        dbg['operation_dt']=pd.to_datetime(dbg['operation_dt']).apply(pd.to_datetime)
        dbg['operation_dt']=pd.to_datetime(dbg['operation_dt'],dayfirst=True)
        dbg['operation_dt']=pd.to_datetime(dbg['operation_dt']).apply(pd.to_datetime)
        dbg['operation_dt']=dbg['operation_dt'].dt.date
        dbg['operation_dt']=pd.to_datetime(dbg['operation_dt']).apply(pd.to_datetime)
        df.loc[(df['ШК'].isin(dbg['shk_id']))&(df['ID виновного'].isin(dbg['contragent_id'])),'перевес']=1
        df=df.merge(dbg,how='left',left_on=['ШК','ID виновного'], right_on=['shk_id','contragent_id'])
        df=df[~df['ШК'].isnull()]
        df['operation_dt']=pd.to_datetime(df['operation_dt']).apply(pd.to_datetime)
        df['operation_dt']=pd.to_datetime(df['operation_dt'],dayfirst=True)
        df['operation_dt']=pd.to_datetime(df['operation_dt']).apply(pd.to_datetime)
        #conn = psycopg2.connect(f"dbname='{db}' user='{login}' host='{host}' password='{pwd}'")
        #cur = conn.cursor()
        sql=f"""select * from sdb.shk_disput"""
        #shk_disput=pd.read_sql(sql,conn)
        shk_disput=pd.read_excel(r'BD.xlsx')
        #shk_disput=shk_disput['shk_id']
        # вот тут
        df['ШК']=df['ШК'].astype('int64')
        shk_disput['shk_id']=shk_disput['shk_id'].astype('int64')
        if len(shk_disput[shk_disput['shk_id'].isin(df['ШК'])]['shk_id'])>0:
            shk_for_del=list(shk_disput[~shk_disput['shk_id'].isin(df['ШК'])]['shk_id'])
            #shk_for_del=",".join(str(element) for element in shk_for_del)
            os.remove(r'BD.xlsx')
            pd.DataFrame({'shk_id':shk_for_del}).to_excel(r'BD.xlsx',index=False)
            DEL=f'delete from sdb.shk_disput where shk_id in ({shk_for_del})'
            #query=cur.execute(DEL)
        shk_disput=shk_disput[~shk_disput['shk_id'].isin(df['ШК'])]
        l=list(shk_disput.rename(columns={'shk_id':'ШК'})['ШК'])
        l=[int(re.sub('\D', '', str(i))) for i in l]
        df=pd.concat([df,pd.DataFrame({'ШК':l})],ignore_index= True).fillna(0)


        v_rab=[]
        gotovo=[]
        prinyali=[]
        for i in a:
            if len(df[df['ШК']==i])>0:
                vr=df[df['ШК']==i]['Кто от ФД внес данные о планируемом перевесе']
                vr_date=df[df['ШК']==i]['operation_dt']
                vr='ШК'+str(i)+' перевешено : '+str(list(vr)[0])
                vr_date=list(vr_date)[0]
                if vr_date==0:
                    v_rab.append(str(i)+ ' уже в работе ')
                    #bot.send_message(message.chat.id, str(i)+ ' уже в работе')
                else:
                    gotovo.append(vr+' дата: '+str(vr_date))
                    #bot.send_message(message.chat.id, vr+' перевешено: '+str(vr_date) )
            else:
                prinyali.append(str(i)+' - приняли в работу')
                add_to_work.append(i)
                #bot.send_message(message.chat.id, str(i)+' Не в работе')
        if len(gotovo)>0:
            gotovo="\n".join(str(element) for element in gotovo)
            try:
                bot.send_message(message.chat.id, gotovo )
            except:
                pass
        if len(v_rab)>0:
            v_rab="\n".join(str(element) for element in v_rab)
            try:
                bot.send_message(message.chat.id, v_rab )
            except:
                pass
        if len(prinyali)>0:
            prinyali="\n".join(str(element) for element in prinyali)
            prin=",".join((re.sub('\D', '',str(element))) for element in add_to_work)

            sql=f"""select operation_dt ,shk_id,new_shk_id,final_amount
                    ,defect_name,reason_descr,reason_descr_writeoff
                    ,reason_descr_writeoff_description,bu_type,state_id
                    from sdb.shk_lost_post_analytics_history
                    where shk_id in ({prin})"""
            #shk_lost_post_analytics_history=pd.read_sql(sql,conn)
            shk_lost_post_analytics_history=pd.read_excel(r'BD.xlsx')
            connection_str = f'postgresql://{login}:{pwd}@{host}:{5432}/{db}'
            df=pd.concat([shk_lost_post_analytics_history,pd.DataFrame({'shk_id':add_to_work})],ignore_index= True).fillna(0)
            #engine = create_engine(connection_str)
            df=df.drop_duplicates()
            os.remove(r'BD.xlsx')
            df.to_excel(r'BD.xlsx',index=False)
            #shk_lost_post_analytics_history.to_sql(name='shk_disput', con=engine, if_exists='append',schema='sdb', index=False)
            try:
                bot.send_message(message.chat.id, prinyali)
            except:
                pass
        #conn.close()


def infinity_polling(self, *args, **kwargs):
    while not self._stop_polling.is_set():
        try:
            self.polling(none_stop=True)
        except Exception as e:
            time.sleep(1)
            pass

bot.infinity_polling()