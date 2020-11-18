import json
import psycopg2
from pprint import pprint
from tabulate import tabulate
import os
import time


class DatatoJson:
    def __init__(self):
        self.creator = "Beks"
        self.path = None
        self.connection = psycopg2.connect(user="postgres",
                                           password="admin",
                                           host="127.0.0.1",
                                           port="5432",
                                           database="json")
        self.cursor = self.connection.cursor()
        self.choose_number()
        self.create_table()
        self.into_to_database()

    def chek(self):
        self.cursor.execute("select * from information_schema.tables where table_name=%s", ('client_info',))
        self.name = bool(self.cursor.rowcount)
        return self.name

    def create_table(self):
        print('succes')
        print(self.chek())

        create_table_query1 = '''CREATE TABLE client_info
            ( 
              man_id int PRIMARY KEY,
              updated text not null,
              foreign_part varchar(100),
              registr_code text ,
              stat_sub_code text,
              tin text,
              region varchar(100),
              city varchar(100),
              district varchar(100),
              village varchar (100),
              microdistrict varchar (100),
              street varchar(100),
              house varchar (100),
              room varchar(50),
              phones varchar(100),
              email1 varchar(100),
              email2 boolean ,
              capital numeric ,
              order_date text ,  
              order_num varchar(100), 
              first_order_date boolean, 
              ind_founders integer ,
              jur_founders int ,
              total_founders int , 
              base_bus_code varchar(100),
              description text, 
              full_name_gl text, 
              full_name_ol text, 
              short_name_gl text, 
              short_name_ol text, 
              category int, 
              category_name text, 
              shief varchar(100),
              reorganization varchar(100),
              screate int, 
              screate_name varchar(50),
              reorg_type varchar(100),
              reorg_type_name  varchar(100),
              ownership int, 
              ownership_name varchar(100)
              );'''

        create_table_query2 = '''CREATE TABLE founders
              (   id serial primary key,
                  subject int references client_info(man_id), 
                  pin varchar(100),
                  full_name text,
                  citizenship varchar(100)
              );'''
        with self.connection:
            if not self.chek():
                self.cursor.execute(create_table_query1)
                self.cursor.execute(create_table_query2)
                self.connection.commit()
            else:
                print('I haved this tables')

    # def get_data_from_file(self):
    #     print('get data')
    #     with open(self.path, 'r', encoding='utf-8') as lst:
    #         data = json.loads(lst.read())
    #         data1 = data[0]
    #         pprint(data1)

    def get_client_id(self):
        query = "select man_id from client_info"
        self.cursor.execute(query)
        print("Запрос на базу 1 выполнен успешно")
        client = self.cursor.fetchall()
        client_id = []
        for a in range(len(client)):
            client_id.append(client[a][0])
        return client_id

    def get_founders_id(self):
        query2 = "select subject from founders"
        self.cursor.execute(query2)
        print("Запрос на базу 2 выполнен успешно")
        founder = self.cursor.fetchall()
        founders_id = []
        for a in range(len(founder)):
            founders_id.append(founder[a][0])
        return founders_id

    def into_to_database(self):
        with open('va.json', 'r', encoding='utf-8') as lst:
            data = json.loads(lst.read())
            length = len(data)
        for a in data:
            if a['id'] not in self.get_client_id():
                c = a['id']
                self.cursor.execute(
                    f"insert into client_info (man_id,updated,foreign_part,registr_code,stat_sub_code,tin,region,city,district,village,microdistrict,street,house,room,phones,email1,email2,capital,order_date,order_num,first_order_date,ind_founders,jur_founders,total_founders,base_bus_code,description,full_name_gl,full_name_ol,short_name_gl,short_name_ol,category,category_name,shief,reorganization,screate,screate_name,reorg_type,reorg_type_name,ownership,ownership_name) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                    (a['id'], a['updated'], a['foreign_part'], a['registr_code'], a['stat_sub_code'],
                     a['tin'], a['region'], a['city'],
                     a['district'], a['village'], a['microdistrict'], a['street'], a['house'],
                     a['room'], a['phones'],
                     a['email1'], a['email2'], a['capital'], a['order_date'], a['order_num'],
                     a['first_order_date'],
                     a['ind_founders'], a['jur_founders'], a['total_founders'],
                     a['base_bus_code'], a['description'], a['full_name_gl'], a['full_name_ol'],
                     a['short_name_gl'], a['short_name_ol'],
                     a['category'], a['category_name'], a['shief'], a['reorganization'], a['screate'],
                     a['screate_name'], a['reorg_type'], a['reorg_type_name'],
                     a['ownership'], a['ownership_name']
                     ))
                with self.connection:
                    if not self.chek():
                        self.connection.commit()
                    else:
                        print('You haved this tables')

        for size_of_data in range(length):
            if data[size_of_data]['founders'] is None:
                print('true')
                size_of_data += 1
            # elif data[size_of_data]['founders'] not in get_founders_id():
            for sc_tb in data[size_of_data]['founders']:
                if sc_tb['subject'] not in self.get_founders_id():
                    print(sc_tb['subject'])
                    subject_id = sc_tb['subject']
                    pin = sc_tb['pin']
                    full_name = sc_tb['full_name']
                    citzenship = sc_tb['citizenship']
                    self.cursor.execute(
                        f"insert into founders (subject,pin,full_name,citizenship) values (%s,%s, %s, %s);",
                        (subject_id, pin, full_name, citzenship))
                    with self.connection:
                        self.connection.commit()

        # for size_of_data in range(length):
        #     if data[size_of_data]['founders'] is None:
        #         print('true')
        #         size_of_data += 1
        #     elif data[size_of_data]['founders'] not in self.get_client_id():
        #         for sc_tb in data[size_of_data]['founders']:
        #             # s = sc_tb['subject']
        #             subject_id = sc_tb['subject']
        #             pin = sc_tb['pin']
        #             full_name = sc_tb['full_name']
        #             citzenship = sc_tb['citizenship']
        #             # print(subject_id, pin, full_name, citzenship)
        #             self.cursor.execute(
        #                 f"insert into founders (subject,pin,full_name,citizenship) values (%s,%s, %s, %s);",
        #                 (subject_id, pin, full_name, citzenship))
        #         with self.connection:
        #             if not self.chek():
        #                 self.connection.commit()
        #             else:
        #                 print('You haved this tables')

        # for size_of_data in range(length):
        #     if data[size_of_data]['founders'] is None:
        #         print('true')
        #         size_of_data += 1
        #     elif data[size_of_data]['founders'] not in self.get_client_id():
        #         for sc_tb in data[size_of_data]['founders']:
        #             # s = sc_tb['subject']
        #             subject_id = sc_tb['subject']
        #             pin = sc_tb['pin']
        #             full_name = sc_tb['full_name']
        #             citzenship = sc_tb['citizenship']
        #             # print(subject_id, pin, full_name, citzenship)
        #             self.cursor.execute(
        #                 f"insert into founders (subject,pin,full_name,citizenship) values (%s,%s, %s, %s);",
        #                 (subject_id, pin, full_name, citzenship))
        #             with self.connection:
        #                 if not self.chek():
        #                     self.connection.commit()
        #                 else:
        #                     print('You haved this tables')

    def file(self):
        filename = input("Введите путь к файлу ")
        if os.path.exists(filename):
            print(f"{filename} указанный файл существует\n {filename} обработан")
        elif filename == '3':
            print('')
            return self.choose_number()
        else:
            print(f"{filename}файл не существует \n")
            return self.file()

    def choose_number(self):
        self.about()
        print("1) Показать файлы\n2) Выбрать файл\n3) Вернуться в меню\n4) чтобы выйти нажмите любую клавишу")
        choose = input()
        if choose == '1':
            pprint(os.listdir())
            print('\n')
            time.sleep(0.3)
            return self.choose_number()
        elif choose == '2':
            print('')
            return self.file()
        elif choose == '3':
            return self.choose_number()
        else:
            print('Exit')

    def about(self):
        a = tabulate([('Hey guys это программа для конвертации из json в Postgresql базу.'),
                      (' Для того чтобы использовать воспользуйтесь с нижеприведенным меню '),
                      f'"Create by {self.creator}"'])
        print(a)


data = DatatoJson()
