from datetime import datetime

import requests
import json
import time
# from tkinter import *
# import tkinter.ttk as ttk
import sqlite3
import csv
import pandas as pd


def get_info():
    response = requests.get('https://codeforces.com/api/contest.list?gym=false')
    print("response.status_code = ", response.status_code)
    json_cf_page = response.json()
    # print(type(json_cf_page))
    return json_cf_page


def parse_info(cf_page_dict):
    # print(cf_page_dict)
    result = cf_page_dict['result']
    to_save = []
    print(type(result))
    for inst in result:
        if(inst['phase']=='BEFORE'):
            to_save.append({'id':inst['id'],'name':inst['name'],
                            'duration':str(int(inst['durationSeconds']/3600))+'   hrs  '+
                               str(int((inst['durationSeconds'] % 3600)/60)) + '   mins',
                            'startTime':datetime.utcfromtimestamp(inst['startTimeSeconds']).strftime('%d-%m-%Y %H:%M:%S')})
            # datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            # 'startTime':time.ctime(inst['startTimeSeconds'])
            # print('startTimeSeconds    =   ' , time.ctime(inst['startTimeSeconds']))
    return(to_save)
    # print(to_save)


def save_info(contest_list):
    df = pd.DataFrame(contest_list)
    df.to_csv('new_data_abt_contests.csv')


def check_info(local_csv, scraped_csv):
    new_data = scraped_csv[scraped_csv['name'].apply(lambda x: x not in local_csv['name'].values)]
    print(new_data)
    to_drop = local_csv[local_csv['name'].apply(lambda x: x not in scraped_csv['name'].values)]
    print(to_drop)
    #local_csv = local_csv.drop(to_drop)
    #local_csv = local_csv.append(new_data)
    #return local_csv


"""def show_table(filename):
    root = Tk()

    with open(filename, newline="") as file:
        reader = csv.reader(file)

        r = 0
        for col in reader:
            c = 0
            for row in col:
                # i've added some styling
                label = Label(root, width=35, height=2,
                                  text=row, relief=RIDGE)
                label.grid(row=r, column=c)
                c += 1
            r += 1
    root.mainloop()
"""

def add_create_db(new_csv):
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        sqlite_create_table_query = '''CREATE TABLE cf_contests_app (
                                    id INTEGER PRIMARY KEY,
                                    name TEXT NOT NULL,
                                    duration TEXT NOT NULL,
                                    startTime datetime);'''

        cursor = sqlite_connection.cursor()
        print("База данных создана и успешно подключена к SQLite")

        sqlite_select_query = "select sqlite_version();"
        cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='cf_contests_app' ''')
        if cursor.fetchone()[0] == 1:
            for name in enumerate(new_csv['name']):
                cursor.execute("SELECT rowid FROM cf_contests_app WHERE name = ?", (name[1],))
                data = cursor.fetchall()
                if len(data) == 0:
                    cursor.execute('''INSERT INTO cf_contests_app(name, duration, startTime) VALUES(?,?,?)''',
                                   (new_csv.iloc[name[0]]['name'], new_csv.iloc[name[0]]['duration'],
                                    new_csv.iloc[name[0]]['startTime']))
                    print('There is no component named %s' %name[1])
        else:
            cursor.execute(sqlite_create_table_query)

        sqlite_connection.commit()
        ## print("Таблица SQLite создана")
        #record = cursor.fetchall()
        #print("Версия базы данных SQLite: ", record)
        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


def del_db(new_csv):
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        ### cursor.execute('create table if not exists projects(id integer, name text)')
        cursor.execute(''' SELECT * FROM cf_contests_app ''')
        # теперь пройти по строкам таблицы и найти те, в которых имя не встречается в новом csv
        records = cursor.fetchall()
        ### print(records)
        ids_to_delete = []
        for row in records:
            if not any(new_csv['name'] == row[1]):
                 ids_to_delete.append(row[0])
        print(ids_to_delete)
        for i in ids_to_delete:
            cursor.execute("DELETE FROM cf_contests_app where id = ?", (i,))


        sqlite_connection.commit()
        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


def main():
    parsed = parse_info(get_info())
    save_info(parsed)
    cur = pd.read_csv('current_contests.csv',sep=',')
    neww = pd.read_csv('new_data_abt_contests.csv',sep=',')
    #print(cur)
    """
    show_table('current_contests.csv')
    show_table('new_data_abt_contests.csv')
    # print((9000%3600)/60)

    res_df = check_info(cur,neww)
    ##print(res_df)
    """
    ## add_create_db(cur)
    add_create_db(neww)
    del_db(neww)


if __name__ == "__main__":
    main()