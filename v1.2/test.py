import tkinter as tk
import psycopg2
import json

path = 'va.json'




connection = psycopg2.connect(user="postgres",
                              password="admin",
                              host="127.0.0.1",
                              port="5432",
                              database="json")
cursor = connection.cursor()
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
#
with connection:
    cursor.execute(create_table_query1)
    cursor.execute(create_table_query2)
    connection.commit()




postgreSQL_select_Query = "select man_id from client_info"

cursor.execute(postgreSQL_select_Query)
client = cursor.fetchall()

def get_client_id():
    query1 = "select man_id from client_info"
    cursor.execute(query1)
    client = cursor.fetchall()
    client_id = []
    for a in range(len(client)):
      client_id.append(client[a][0])
    return client_id

def get_founders_id():
    query2 = "select subject from founders"
    cursor.execute(query2)
    founder = cursor.fetchall()
    founders_id = []
    for a in range(len(founder)):
        founders_id.append(founder[a][0])
    return founders_id
print(get_founders_id())

# client_id = []
# for a in range(length):
#     client_id.append(client[a][0])
#
# data_id = []
# for a in data:
#     data_id.append(a['id'])

with open('va.json', 'r', encoding='utf-8') as lst:
    data = json.loads(lst.read())
    length = len(data)

#
for a in data:
    if a['id'] not in get_client_id():
        c = a['id']
        print(a['id'])
        # a = data[a]
        cursor.execute(
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
        with connection:
            connection.commit()
# #
# for size_of_data in range(length):
#     if data[size_of_data]['founders'] is None and data[size_of_data]['founders'] not in get_client_id():
#         print('true')
#         size_of_data += 1
#         for sc_tb in data[size_of_data]['founders']:
#             # s = sc_tb['subject']
#             subject_id = sc_tb['subject']
#             pin = sc_tb['pin']
#             full_name = sc_tb['full_name']
#             citzenship = sc_tb['citizenship']
#             # print(subject_id, pin, full_name, citzenship)
#             cursor.execute(
#                 f"insert into founders (subject,pin,full_name,citizenship) values (%s,%s, %s, %s);",
#                 (subject_id, pin, full_name, citzenship))
#         with connection:
#             connection.commit()

for size_of_data in range(length):
    if data[size_of_data]['founders'] is None:
        print('true')
        size_of_data += 1
    # elif data[size_of_data]['founders'] not in get_founders_id():
    for sc_tb in data[size_of_data]['founders']:
        # if sc_tb['subject'] not in get_founders_id():
            print(sc_tb['subject'])
            subject_id = sc_tb['subject']
            pin = sc_tb['pin']
            full_name = sc_tb['full_name']
            citzenship = sc_tb['citizenship']
            cursor.execute(
                f"insert into founders (subject,pin,full_name,citizenship) values (%s,%s, %s, %s);",
                (subject_id, pin, full_name, citzenship))
            with connection:
                connection.commit()




 #
 # for size_of_data in range(length):
 #            if data[size_of_data]['founders'] is None:
 #                print('true')
 #                size_of_data += 1
 #            # elif data[size_of_data]['founders'] not in get_founders_id():
 #            for sc_tb in data[size_of_data]['founders']:
 #                if sc_tb['subject'] not in self.get_founders_id():
 #                    print(sc_tb['subject'])
 #                    subject_id = sc_tb['subject']
 #                    pin = sc_tb['pin']
 #                    full_name = sc_tb['full_name']
 #                    citzenship = sc_tb['citizenship']
 #                    self.cursor.execute(
 #                        f"insert into founders (subject,pin,full_name,citizenship) values (%s,%s, %s, %s);",
 #                        (subject_id, pin, full_name, citzenship))
 #                    with self.connection:
 #                        self.connection.commit()
 #



#
# def get_client_id():
#     client_id = []
#     for a in range(len(client)):
#         client_id.append(client[a][0])
#     return client_id

# with open(path, 'r', encoding='utf-8') as lst:
#     data = json.loads(lst.read())
#     length = len(data)
#
# for a in data:
#     # print(a['id'])
#     if a['id'] not in get_client_id():
#         print(a['id'])


# print(get_client_id())
# print(length)

#
# def get_data_id():
#     data_id = []
#     for c in data:
#        data_id.append(c['id'])
#     return data_id
#
# def filter_duplicate(string_to_chek):
#   if string_to_chek in ll:
#     return False
#   else:
#     return True
#
#
# ll = get_data_id()
# out_filter = list(filter(filter_duplicate, get_client_id()))
# ll = get_client_id()
# out_filter += list(filter(filter_duplicate, get_data_id()))
#
#
# print('Filterided a list', out_filter)

# print('в базе сущесвтует',get_client_id(),'\n')
#








































# window = tk.Tk()
#
# class Leute:
#     name = 'beka'
#
#     def hello():
#         print("hello")
# print(Leute.hello())
# print(Leute.__dict__)

#
# test = {'id':1212,'room':22, 'beks': 22}
#
# team = [0,1,2,3]
#
# print(type(team))
# for a in team:
#     print(team[a])

# print(test['id'])

# for a in test:
#     print('test["%d"]' %a )

# length = len(test)
# for a in range(length):
#     if key == 'beks':
#         break
#     #@ a ="test['{}']".format(key)
#     #@ print(f"test[{key}]")
#     print(test[a])
    # print(f'{test[key]}')

# connection = psycopg2.connect(user="postgres",
#                                            password="admin",
#                                            host="127.0.0.1",
#                                            port="5432",
#                                            database="json")
# cursor = connection.cursor()
# cursor.execute("select * from information_schema.tables where table_name=%s", ('client_info',))
# name = bool(cursor.rowcount)
# print(name)
# while name == False:
#     print('1')
# print('sdfasdf')

#
# mass = [11,12,14,15,16]
# a = [11,12,14]
# for i in mass:
#     if i not in a:
#         print(i)

# list1 = [12,13,14,15,16,17]
# list2 = [14,16,17,18,19,20,21]
#
# def filter_duplicate(string_to_chek):
#     if string_to_chek in ll:
#         return False
#     else:
#         return True
# ll = list2
# out_filter = list(filter(filter_duplicate, list1))
# ll = list1
# out_filter = list(filter(filter_duplicate,list2))
#
# print('Filterided a list', out_filter)
#
