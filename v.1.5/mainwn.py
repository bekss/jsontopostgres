import datetime
import json
import psycopg2
import requests

from datetime import timedelta
from tkinter import *
from tkinter import messagebox
from tkinter import ttk as tk
from tkcalendar import DateEntry
from tksheet import Sheet


class Get_Data:
    def __init__(self, url=None, header=None):
        self.connection = psycopg2.connect(user="postgres",
                                           password="admin",
                                           host="127.0.0.1",
                                           port="5432",
                                           database="json")
        self.cursor = self.connection.cursor()
        self.url = url
        self.header = header

    def chek(self):
        self.cursor.execute("select * from information_schema.tables where table_name=%s", ('client_info',))
        self.name = bool(self.cursor.rowcount)
        return self.name

    def create_table(self):
        if self.chek():
            print('У вас есть таблицы ')
        else:
            print('Создаются таблицы')

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
              ownership_name varchar(100),
              date_from date not null,
              date_to date not null   
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
                print('В существующие таблицы добавляются данные ')

    def get_data_ip(self):
        response_data = requests.get(self.url, headers=self.header)
        data_json = json.loads(response_data.text)
        print(data_json)

    def get_client_id(self):
        query_id = "select man_id from client_info"
        self.cursor.execute(query_id)
        client_id = self.cursor.fetchall()
        for a in client_id:
            if type(a) == tuple:
                client_id[client_id.index(a)] = a[0]
        return client_id

    def get_client_tin(self):
        query_tin = "select tin from client_info"
        self.cursor.execute(query_tin)
        client = self.cursor.fetchall()
        for a in range(len(client)):
            if client[a] == ('',):
                client[a] = 0
        for i in client:
            if type(i) == tuple:
                client[client.index(i)] = str(i[0])
        return client

    def get_founders_all(self):
        query2 = "select subject, pin,full_name, citizenship from founders"
        self.cursor.execute(query2)
        founder = self.cursor.fetchall()
        founders_id = []
        for a in range(len(founder)):
            founders_id.append(founder[a])
        return founders_id

    def get_client_update(self):
        query_updated = "select updated from client_info "
        self.cursor.execute(query_updated)
        client_udpated = self.cursor.fetchall()
        for i in client_udpated:
            if type(i) == tuple:
                client_udpated[client_udpated.index(i)] = i[0]
        return client_udpated

    def get_client_chek_id(self):
        query_id = "select man_id from client_info"
        self.cursor.execute(query_id)
        client_id = self.cursor.fetchall()
        for a in client_id:
            if type(a) == tuple:
                client_id[client_id.index(a)] = a[0]
        return client_id


class GUI(Frame, Get_Data):
    def __init__(self, master=None, url=None, header=None):
        Frame.__init__(self, master)
        Get_Data.__init__(self, url, header)
        self.create_table()
        self.url = url
        self.header = header
        self.master = master
        self.pack()
        self.master.title("Beks Ltd")
        self.main_frame()
        self.exit()
        self.sec_frame()
        # self.bottom_frame()

    def main_frame(self):
        global date_from, date_to
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day - 1
        style = {}
        style['button'] = tk.Style()
        style['button'].configure('TButton', font=
        ('Arial MT', 12, 'bold'),
                                  borderwidth='4')

        style['label'] = tk.Style()
        style['label'].configure('Label', font=('Arial MT', 12, 'bold'), borderwidth='4')

        frame1 = Frame(self.master, background='#21acfc', width=500, height=500)  # Первый фрейм (окно)
        frame1.pack(side=TOP, pady=10, )

        date_from = DateEntry(frame1, background='#e03aca', font="Helvetica 14", year=year, month=month, day=day,
                              locale='KY')
        date_from.grid(row=0, column=1, columnspan=2, padx=50, pady=20)

        date_to = DateEntry(frame1, background='#533feb', font="Helvetica 14 ", year=year, month=month, day=day,
                            locale='KY')
        print(date_to.get_date())
        date_to.grid(row=0, column=4, padx=50, pady=20)

        label = tk.Label(frame1, text=' Баштапкы кун:', style='Label', background='#21acfc', )
        label.grid(row=0, column=0, padx=50, pady=20)

        label1 = tk.Label(frame1, text=' Акыркы кун:', style='Label', background='#21acfc', )
        label1.grid(row=0, column=3, padx=50, pady=20)

        get_data_button = tk.Button(frame1, text='Маалыматты КРнын Юстиция Мин-нен алуу', style='TButton',
                                    command=self.get_data_button)
        get_data_button.grid(row=1, column=0, columnspan=5, rowspan=5, pady=50, )

        url_label = tk.Label(frame1, text='URLды жазыныз', style='Label', background='#21acfc')
        url_label.grid(row=7, column=0, pady=20)

        header_label = tk.Label(frame1, text='HEADERSти жазыныз', style='Label', background='#21acfc')
        header_label.grid(row=7, column=3, pady=20)

        url_entry = tk.Entry(frame1, background='lightgreen')
        url_entry.grid(row=7, column=1, pady=20)

        header_entry = tk.Entry(frame1, background='lightgreen')
        header_entry.grid(row=7, column=4, pady=20)

        # Русс
        # Загрузить данные с Мин.Юстиции.КР

    def message_error(self):
        mess_error = messagebox.showerror(' Ката ', ' Кунду туура киргизиниз! ')

    def get_data_button(self):
        date1 = date_from.get_date()
        date2 = date_to.get_date() + timedelta(days=1)
        date3 = date2 - timedelta(days=1)
        print('data3', date3)
        print(date2)
        print(f'{date1} {date2}')
        url = f'http://212.42.101.115/r1/central-server/GOV/70000024/minjust-inn-services/SubjectFromTo?datefrom={date1}&dateto={date2}'
        print(url)
        headers = {'X-Road-Client': 'central-server/GOV/70000010/nsk-classificators'}
        response_data = requests.get(url, headers=headers)
        if response_data.text == 'Column is null':
            self.message_error()
        data = json.loads(response_data.text)
        print(data)
        length = len(data)
        for a in data:
            if a['tin'] not in self.get_client_tin() and a['id'] not in self.get_client_chek_id():
                print(a['tin'])
                print(a['id'])
                self.cursor.execute(
                    f"insert into client_info (man_id,updated,foreign_part,registr_code,stat_sub_code,tin,region,city,district,village,microdistrict,street,house,room,phones,email1,email2,capital,order_date,order_num,first_order_date,ind_founders,jur_founders,total_founders,base_bus_code,description,full_name_gl,full_name_ol,short_name_gl,short_name_ol,category,category_name,shief,reorganization,screate,screate_name,reorg_type,reorg_type_name,ownership,ownership_name, date_from, date_to) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
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
                     a['ownership'], a['ownership_name'], date1, date3
                     ))
                with self.connection:
                    self.connection.commit()
            else:
                if a['tin'] in self.get_client_tin() and a['updated'] not in self.get_client_update():
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
                            print('В существующие таблицы добавляются данные ')
        for size_of_data in range(length):
            if data[size_of_data]['founders'] is None or len(data[size_of_data]['founders']) == 0:
                size_of_data += 1
            for sc_tb in data[size_of_data]['founders']:
                j_founders = (sc_tb['subject'], sc_tb['pin'], sc_tb['full_name'],
                              sc_tb['citizenship'])
                if sc_tb['subject'] in self.get_client_chek_id() and j_founders not in self.get_founders_all():
                    subject_id = sc_tb['subject']
                    pin = sc_tb['pin']
                    full_name = sc_tb['full_name']
                    citzenship = sc_tb['citizenship']
                    self.cursor.execute(
                        f"insert into founders (subject,pin,full_name,citizenship) values (%s,%s, %s, %s);",
                        (subject_id, pin, full_name, citzenship))
                    with self.connection:
                        self.connection.commit()
        messagebox.showinfo('Суйунчу', 'Маалыматтар сакталды!')

    def sec_frame(self):
        global date_from1, date_from2, table, bott_frame
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day - 1
        sec_frame1 = Frame(self.master, background='#21acfc', )
        sec_frame1.pack()
        query_title = tk.Label(sec_frame1, style='Label', text='Баштапкы кун', background='#21acfc')
        query_title.pack(side=LEFT, padx=50, pady=50)

        date_from1 = DateEntry(sec_frame1, background='#e03aca', font="Helvetica 14", year=year, month=month,
                               day=day - 1, locale='KY')
        date_from1.pack(side=LEFT, padx=50)

        date_from2 = DateEntry(sec_frame1, background='#e03aca', font="Helvetica 14", year=year, month=month, day=day,
                               locale='KY')
        date_from2.pack(side=LEFT)
        query_button = tk.Button(sec_frame1, text='Маалыматы алуу', width=200, command=self.get_datas)
        query_button.pack(side=LEFT, padx=50, pady=40)

        bott_frame = Frame(self.master, )
        table = Sheet(bott_frame, page_up_down_select_row=True, column_width=120, startup_select=(0, 1, "rows"),
                      data=[],
                      height=1000,
                      width=1920
                      )
        # table.set_sheet_data(data=[[get_datas[r][c] for c in range(len1)] for r in range(lenn)], verify=False)
        table.enable_bindings(("single_select",
                               "drag_select",
                               "column_drag_and_drop",
                               "row_drag_and_drop",
                               "column_select",
                               "row_select",
                               "column_width_resize",
                               "double_click_column_resize",
                               "arrowkeys",
                               "row_height_resize",
                               "double_click_row_resize",
                               "right_click_popup_menu",
                               "rc_select",
                               "rc_insert_column",
                               "rc_delete_column",
                               "rc_insert_row",
                               "rc_delete_row",
                               "hide_columns",
                               "copy",
                               ))
        # # "cut",
        # # "paste",
        # # "delete",
        # # "undo",
        # # "edit_cell"

    def get_datas(self):
        table.set_sheet_data(data=[])
        date_to_year = date_from2.get_date()
        date_from_year = date_from1.get_date()
        print(date_to_year)
        if date_from_year and date_to_year not in self.get_today():
            print(date_from_year, date_to_year)
            query_datas = f"select client_info.man_id,client_info.updated,client_info.foreign_part,client_info.registr_code, client_info.stat_sub_code,  client_info.tin,  client_info.region,client_info.city,client_info.district, client_info.village,  client_info.microdistrict, client_info.street, client_info.house, client_info.room,client_info.phones, client_info.email1, client_info.email2, client_info.capital, client_info.order_date,client_info.order_num,client_info.first_order_date,client_info.ind_founders,client_info.jur_founders,client_info.total_founders,client_info.base_bus_code,client_info.description,client_info.full_name_gl,client_info.full_name_ol,client_info.short_name_gl,client_info.short_name_ol,client_info.category,client_info.category_name,client_info.shief,client_info.reorganization,client_info.screate,client_info.screate_name,client_info.reorg_type,client_info.reorg_type_name,client_info.ownership,client_info.ownership_name, founders.subject,founders.pin, founders.full_name, founders.citizenship  from client_info, founders where date_trunc('day', client_info.updated::date)::date between '{date_from_year}'::date and '{date_to_year}'::date and client_info.man_id = founders.subject order by client_info.updated asc"
            self.cursor.execute(query_datas)
            get_datas = self.cursor.fetchall()
            print(query_datas)
            try:
                get_datas[0]
            except IndexError:
                messagebox.showerror('Ката', 'Бул кундор жок башка кунду танданыз')
            print(get_datas)
            lenn = len(get_datas)
            len1 = len(get_datas[0])
            table.set_sheet_data(data=[[get_datas[r][c] for c in range(len1)] for r in range(lenn)])
            bott_frame.pack()
            table.pack(side=TOP)

        else:
            messagebox.showinfo('Кунду киригизиниз')

    def get_today(self):
        query_today = 'select distinct date_from, date_to from client_info'
        self.cursor.execute(query_today)
        today = self.cursor.fetchall()
        return today

    def exit(self):
        exit_button = tk.Button(self.master, text='Выйти', command=self.master.destroy)
        exit_button.pack(side=BOTTOM)


def main():
    root = Tk()
    root.geometry("950x800+550+80")
    gui = GUI(master=root)
    root.mainloop()


main()