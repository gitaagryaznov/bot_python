import telebot
from random import randint
import datetime

import time
import pandas as pd
import requests
import re
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import os
import gspread as gs
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import psycopg2

token = "6884325914:AAG-MCRsr6dGSB9LdXQHeRXTS7lnJAwfpaM"
bot = telebot.TeleBot(token)


# запрос на предоставления информации
@bot.message_handler(commands=['report'])
def st_message(message):
    if message.text == '/report':
        ##########################################################################################################################################
        ##########################################################################################################################################
        # Грузим инфу все что добавили на перевес
        g = r"https://docs.google.com/spreadsheets/d/e/2PACX-1vR8od_JorrGKUjGhGlv2hXSsSG5Wsj1Y69E7qfko0gugbfLbq0o5T04D2lPCrFVrckV1hxrN0B62bJX/pub?gid=50571130&single=true&output=csv"
        df = pd.read_csv(g)  # Все что добавили на перевес
        # Кол-во ШК  перевешено
        df = df[~df['ШК'].isnull()]
        df['ШК'] = df['ШК'].astype('int64')
        ##########################################################################################################################################
        ##########################################################################################################################################
        # Грузим инфу все что перевесили
        google = r'https://docs.google.com/spreadsheets/d/e/2PACX-1vR8od_JorrGKUjGhGlv2hXSsSG5Wsj1Y69E7qfko0gugbfLbq0o5T04D2lPCrFVrckV1hxrN0B62bJX/pub?gid=0&single=true&output=csv'
        dbg = pd.read_csv(google)  # все что перевешено
        ##########################################################################################################################################
        ##########################################################################################################################################
        # Чистим инфу в таблицах гугла
        df.loc[(df['ШК'].isin(dbg['shk_id'])) & (df['ID виновного'].isin(dbg['contragent_id'])), 'перевес'] = 1
        df = df.merge(dbg, how='left', left_on=['ШК', 'ID виновного'], right_on=['shk_id', 'contragent_id'])
        df = df[~df['ШК'].isnull()]
        df['operation_dt'] = pd.to_datetime(df['operation_dt'].fillna(pd.to_datetime('1990-01-01')))
        df['operation_dt'] = pd.to_datetime(df['operation_dt']).apply(pd.to_datetime)
        df['operation_dt'] = df['operation_dt'].dt.date
        df = df[pd.to_datetime(df['operation_dt']) > pd.to_datetime('2000-01-01')]
        df['operation_dt'] = pd.to_datetime(df['operation_dt']).apply(pd.to_datetime)
        df = df[pd.to_datetime(df['operation_dt']) > pd.to_datetime('2000-01-01')]
        df = df.fillna(0)
        df['operation_dt'] = pd.to_datetime(df['operation_dt']).apply(pd.to_datetime)
        df['month'] = df['operation_dt'].dt.month_name()
        df['month_n'] = df['operation_dt'].dt.month
        df['year'] = df['operation_dt'].dt.year
        df = df.sort_values('operation_dt')
        gr = df.groupby(
            ['month', 'Кто от ФД внес данные о планируемом перевесе', 'month_n', 'year']).count().reset_index()
        gr = gr[(gr['month_n'] == gr['month_n'].max()) & (gr['year'] == gr['year'].max())]
        gr = gr[['year', 'month', 'Кто от ФД внес данные о планируемом перевесе', 'ШК']]
        gr.columns = ['year', 'month', 'фио', 'кол-во ШК']
        gr = gr.sort_values(['year', 'month', 'кол-во ШК'])  # подготовили группировку для рассылки письма

        ##########################################################################################################################################
        ##########################################################################################################################################
        # Отправляем кто сколько отправил на перевес
        try:
            bot.send_message(message.chat.id, gr.to_string())
        except:
            pass
        ##########################################################################################################################################
        ##########################################################################################################################################
        # Готовим документ все что надо отправить на перевес
        shk_disput = pd.read_excel(r'BD.xlsx')
        shk_disput = shk_disput['shk_id']
        a = r'report.xlsx'
        shk_disput.to_excel(a, index=False)
        upfile = open(a, "rb")  # Октрытие файлов
        try:
            bot.send_document(message.chat.id, upfile)
        except:
            pass
        try:
            upfile.close()  # Закрытие всего
            os.remove(a)
        except:
            pass


##########################################################################################################################################
##########################################################################################################################################
##########################################################################################################################################
##########################################################################################################################################
##########################################################################################################################################
##########################################################################################################################################

# обработка поступающих сообщений 
@bot.message_handler(content_types='text')
def start_message(message):
    ##########################################################################################################################################
    ##########################################################################################################################################

    ##########################################################################################################################################
    ##########################################################################################################################################
    # делим сообщение и вытаскиваем номера
    aaa = message.text
    aaa = aaa.lower().replace('шк', '')
    aaa = aaa.split()
    a = []
    b = []
    add_to_work = []
    for i in aaa:
        i = i  # re.sub('\D', '', i)
        try:
            int(i.strip())
            if int(i.strip()) > 100000000:
                a.append(int(i.strip()))
        except ValueError:
            b.append(i)
            pass
    ##########################################################################################################################################
    ##########################################################################################################################################
    # если нет ШК то пасуем если есть редачим и херачим дальше 
    if len(a) == 0:
        pass
    else:

        ##########################################################################################################################################
        ##########################################################################################################################################
        # Подключаемся к данным что добавили на перевес
        a = list(set(a))  # ШК  в списке
        g = r"https://docs.google.com/spreadsheets/d/e/2PACX-1vR8od_JorrGKUjGhGlv2hXSsSG5Wsj1Y69E7qfko0gugbfLbq0o5T04D2lPCrFVrckV1hxrN0B62bJX/pub?gid=50571130&single=true&output=csv"
        df = pd.read_csv(g)
        df = df[~df['ШК'].isnull()]
        df['ШК'] = df['ШК'].astype('int64')
        df = df.fillna(0)
        ##########################################################################################################################################
        ##########################################################################################################################################
        # Подключаемся к данным которые уже перевесили
        # чистим и преобразуем
        google = r'https://docs.google.com/spreadsheets/d/e/2PACX-1vR8od_JorrGKUjGhGlv2hXSsSG5Wsj1Y69E7qfko0gugbfLbq0o5T04D2lPCrFVrckV1hxrN0B62bJX/pub?gid=0&single=true&output=csv'
        dbg = pd.read_csv(google)
        dbg['operation_dt'] = dbg['operation_dt'].fillna(pd.to_datetime('1990-01-01'))
        dbg['operation_dt'] = dbg['operation_dt'].apply(pd.to_datetime)
        dbg['operation_dt'] = pd.to_datetime(dbg['operation_dt']).apply(pd.to_datetime)
        dbg['operation_dt'] = pd.to_datetime(dbg['operation_dt'], dayfirst=True)
        dbg['operation_dt'] = pd.to_datetime(dbg['operation_dt']).apply(pd.to_datetime)
        dbg['operation_dt'] = dbg['operation_dt'].dt.date
        dbg['operation_dt'] = pd.to_datetime(dbg['operation_dt']).apply(pd.to_datetime)
        df.loc[(df['ШК'].isin(dbg['shk_id'])) & (df['ID виновного'].isin(dbg['contragent_id'])), 'перевес'] = 1
        df = df.merge(dbg, how='left', left_on=['ШК', 'ID виновного'], right_on=['shk_id', 'contragent_id'])
        df = df[~df['ШК'].isnull()]
        df['operation_dt'] = pd.to_datetime(df['operation_dt']).apply(pd.to_datetime)
        ##########################################################################################################################################
        ##########################################################################################################################################
        ##########################################################################################################################################
        # Грузим инфу  все что добавили в разбор
        na_razbor = r"https://docs.google.com/spreadsheets/d/e/2PACX-1vR8od_JorrGKUjGhGlv2hXSsSG5Wsj1Y69E7qfko0gugbfLbq0o5T04D2lPCrFVrckV1hxrN0B62bJX/pub?gid=478612961&single=true&output=csv"
        na_razbor = pd.read_csv(na_razbor)
        na_razbor = na_razbor[~na_razbor['ШК'].isnull()]
        na_razbor['проверка_числа'] = na_razbor['ШК'].apply(lambda x: str(x).isdigit())
        na_razbor = na_razbor[na_razbor['проверка_числа'] == True]
        na_razbor['ШК'] = na_razbor['ШК'].astype('int64')
        na_razbor = na_razbor.fillna(0)
        # приводим к нормальным форматам
        df['ШК'] = df['ШК'].astype('int64')
        na_razbor['ШК'] = na_razbor['ШК'].astype('int64')
        # Смотрим есть ли ШК на разбор уже в разобранных

        shk_disput = na_razbor[~na_razbor['ШК'].isin(df['ШК'])]
        l = list(shk_disput.rename(columns={'shk_id': 'ШК'})['ШК'])

        l = [int(str(i)) for i in l]
        df = pd.concat([df, pd.DataFrame({'ШК': l})], ignore_index=True).fillna(0)
        ##########################################################################################################################################
        ##########################################################################################################################################
        ##########################################################################################################################################
        # Смотрим все что в работе все что на перевес, все что приняли в работу
        v_rab = []
        gotovo = []
        prinyali = []
        for i in a:
            if len(df[df['ШК'] == i]) > 0:
                vr = df[df['ШК'] == i]['Кто от ФД внес данные о планируемом перевесе']
                vr_date = df[df['ШК'] == i]['operation_dt']
                vr = 'ШК' + str(i) + ' перевешено : ' + str(list(vr)[0])
                vr_date = list(vr_date)[0]
                if vr_date == 0:
                    v_rab.append(str(i) + ' уже в работе ')
                    # делаем список ШК, которые уже в работе
                else:

                    gotovo.append(vr + ' дата: ' + str(vr_date))
                    # готовим список тех, кого уже перевесили
            else:
                prinyali.append(str(i) + ' - приняли в работу')
                add_to_work.append(i)
                # готовим список тех, кого приняли в работу
        ##########################################################################################################################################
        ##########################################################################################################################################
        ##########################################################################################################################################
        ##########################################################################################################################################
        # проверяем каждый список
        ##########################################################################################################################################
        # Готово
        if len(gotovo) > 0:
            gotovo = "\n".join(str(element) for element in gotovo)
            try:
                bot.send_message(message.chat.id, gotovo)
            except:
                pass
        ##########################################################################################################################################
        # уже в работе
        if len(v_rab) > 0:
            v_rab = "\n".join(str(element) for element in v_rab)
            try:
                bot.send_message(message.chat.id, v_rab)
            except:
                pass
        ##########################################################################################################################################
        # Приняли в работу
        if len(prinyali) > 0:

            prinyali = "\n".join(str(element) for element in prinyali)
            prin = ",".join(str(element) for element in add_to_work)
            ##########################################################################################################################################
            ##########################################################################################################################################
            # читаем все что есть в списках на разбор

            add_to_work = pd.DataFrame({'ШК': add_to_work})
            add_to_work = add_to_work.merge(na_razbor, how='left', on='ШК')

            add_to_work = add_to_work[add_to_work['Дата старта оспаривания'].isnull()]
            # Добавляем дату, коммент
            mess_time = datetime.datetime.now()
            mess_time = mess_time.strftime('%d.%m.%Y %H:%M:%S')
            add_to_work['Дата добавления'] = mess_time
            add_to_work['коммент'] = ' '.join(b)

            # Если есть что добавить, добавляем в конец списка
            if add_to_work.shape[0] > 0:
                scope = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive"]
                CREDENTIALS_FILE = r'creds.json'
                CREDENTIALS_FILE = CREDENTIALS_FILE.replace('\u202a', '')
                credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
                client = gs.authorize(credentials)
                google_sheet = client.open("NewDatabase")
                # Ищем лист в который надо добавлять ШК
                for sh in google_sheet.worksheets():
                    if sh.title == 'Работа с обращениями':
                        razbor = sh
                for i in add_to_work[['ШК', 'Дата добавления', 'коммент']].values.tolist():
                    razbor.append_row(i, table_range='A:C')
            try:

                bot.send_message(message.chat.id, prinyali)
            except:
                pass
        # conn.close()


def infinity_polling(self, *args, **kwargs):
    while not self._stop_polling.is_set():
        try:
            self.polling(none_stop=True)
        except Exception as e:
            time.sleep(1)
            pass


bot.infinity_polling()
