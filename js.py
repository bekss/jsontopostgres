import json
import psycopg2
from pprint import pprint
import ast

path = 'va.json'


class DatatoJson:
    def __init__(self, path, table_name='client_info'):
        self.path = path
        self.name = None
        self.names = None
        self.connection = psycopg2.connect(user="postgres",
                                           password="admin",
                                           host="127.0.0.1",
                                           port="5432",
                                           database="json")
        self.cursor = self.connection.cursor()
        # self.get_data_from_file()
        self.create_table()
        # self.print()
        self.into_to_database()

    def chek(self):
        self.cursor.execute("select * from information_schema.tables where table_name=%s", ('client_info',))
        self.name = bool(self.cursor.rowcount)
        return self.name

    def create_table(self):
        # self.names = self.cursor.execute(f"select * from information_shema.tables where table_name = {client_info}")
        # self.name = bool(self.cursor.rowcount)
        print('succes')
        print(self.chek())

        create_table_query1 = '''CREATE TABLE client_info
            ( 
              man_id int PRIMARY KEY ,
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

    def into_to_database(self):
        print('get data')
        with open(self.path, 'r', encoding='utf-8') as lst:
            data = json.loads(lst.read())

            # data1 = data[1]['founders']
            # print(data1[0])
            # for a in data[2]['founders']:
            #     cmd = a['pin']
            #     print(cmd)

            print(len(data))
            length = len(data)
            print(length)

        for a in range(length):
            data1 = data[a]
            self.cursor.execute(
                f"insert into client_info (man_id,updated,foreign_part,registr_code,stat_sub_code,tin,region,city,district,village,microdistrict,street,house,room,phones,email1,email2,capital,order_date,order_num,first_order_date,ind_founders,jur_founders,total_founders,base_bus_code,description,full_name_gl,full_name_ol,short_name_gl,short_name_ol,category,category_name,shief,reorganization,screate,screate_name,reorg_type,reorg_type_name,ownership,ownership_name) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                (data1['id'], data1['updated'], data1['foreign_part'], data1['registr_code'], data1['stat_sub_code'],
                 data1['tin'], data1['region'], data1['city'],
                 data1['district'], data1['village'], data1['microdistrict'], data1['street'], data1['house'],
                 data1['room'], data1['phones'],
                 data1['email1'], data1['email2'], data1['capital'], data1['order_date'], data1['order_num'],
                 data1['first_order_date'],
                 data1['ind_founders'], data1['jur_founders'], data1['total_founders'],
                 data1['base_bus_code'], data1['description'], data1['full_name_gl'], data1['full_name_ol'],
                 data1['short_name_gl'], data1['short_name_ol'],
                 data1['category'], data1['category_name'], data1['shief'], data1['reorganization'], data1['screate'],
                 data1['screate_name'], data1['reorg_type'], data1['reorg_type_name'],
                 data1['ownership'], data1['ownership_name']
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
            for sc_tb in data[size_of_data]['founders']:
                # s = sc_tb['subject']
                subject_id = sc_tb['subject']
                pin = sc_tb['pin']
                full_name = sc_tb['full_name']
                citzenship = sc_tb['citizenship']
                # print(subject_id, pin, full_name, citzenship)
                self.cursor.execute(
                    f"insert into founders (subject,pin,full_name,citizenship) values (%s,%s, %s, %s);",
                    (subject_id, pin, full_name, citzenship))

                with self.connection:
                    if not self.chek():
                        self.connection.commit()
                    else:
                        print('You haved this tables')






    # def print(self):
    #     database = self.get_data_from_file()
    #     print(database[id])


data = DatatoJson('../../va.json')

# for key, value in data[0][1].items():
#     print(key, value)

# pprint(data[0])
# if type(data) == list:
#     first_record = data[0]
# pprint(first_record)

# columns = [list(x.keys()) for x in data][0]
# pprint(type(data))

# connection = psycopg2.connect(user="postgres",
#                               password="admin",
#                               host="127.0.0.1",
#                               port="5432",
#                               database="json")
# cursor = connection.cursor()
#
# create_table_query = '''CREATE TABLE json_data
#       (ID INT PRIMARY KEY     NOT NULL,
#       MODEL           TEXT    NOT NULL,
#       PRICE         REAL);'''
#
# cursor.execute(create_table_query)
# connection.commit()
# print("Table created successfully in PostgreSQL ")


# data = json.load(open('MU2.json', 'r'))
# jtopy=json.dumps(data) #json.dumps take a dictionary as input and returns a string as output.
# dict_json=json.loads(jtopy) # json.loads take a string as input and returns a dictionary as output.
# print(dict_json[0])

# x =  '{ "name":"John", "age":30, "city":"New York"}'
# y = json.loads(x)
# print(y['age'])

# with open(path, 'r') as f:
#     data = json.loads(f)
#     for i in data:
#         print(i)
#
#
# x = {
#   "name": "John",
#   "age": 30,
#   "married": True,
#   "divorced": False,
#   "children": ("Ann","Billy"),
#   "pets": None,
#   "cars": [
#     {"model": "BMW 230", "mpg": 27.5},
#     {"model": "Ford Edge", "mpg": 24.1}
#   ]
# }
#
# print(json.dumps(x))

# with open('data.json', 'w' , encoding ='utf-8') as file:
#     file.write(jsonData)