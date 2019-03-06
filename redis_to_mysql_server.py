from ast import literal_eval
import pymysql
import redis
import time

class SettingsDefault():
    redis_conn = dict(
        host = 'localhost',
        password = None,
        port = 6379,
        db = 0,
        encoding = 'utf-8'
    )
    mysql_conn = dict(
        host = 'localhost',
        port = 3306,
        user = 'root',
        passwd = 'a110190',
        db = 'spider',
        charset = 'utf8'
    )
    redis_key = 'item'
    mysql_table = 'spider_item'
    max_spop_count = 10
    max_wait_time = 2
    filter_column_s = ['id']

class Server:
    def __init__(self,settings=SettingsDefault):
        self.settings = settings
        self.conn_redis = redis.StrictRedis( **settings.redis_conn )
        self.conn_mysql = pymysql.connect( **settings.mysql_conn )
        self.cs = self.conn_mysql.cursor()
        self.column_name_s = []
        self.total_success_count = 0
        self.current_success_count = 0
        self.total_failure_count = 0

    def get_table_column(self):
        cs = self.conn_mysql.cursor()
        sql = 'show columns from %s'%self.settings.mysql_table
        cs.execute(sql)
        for col in cs.fetchall():
            if col[0] not in self.settings.filter_column_s:
                self.column_name_s.append(col[0])
            else:
                pass
        cs.close()

    def get_redis_data(self,redis_sql):
        members = self.conn_redis.execute_command(redis_sql)
        value_str = ''
        self.current_success_count = 0
        for i in range(len(members)):
            try:
                item = literal_eval(members[i].decode('utf-8'))
                item_str = '('
                for j,column_name in enumerate(self.column_name_s):
                    item_str += '"%s",'%item.get(column_name,'-1')
                item_str = item_str[0:-1] + ')'
                self.current_success_count += 1
            except:
                self.total_failure_count += 1
            value_str += item_str + ','
        return value_str[0:-1]

    def insert_into_mysql(self,mysql_sql_base,values):

        sql = mysql_sql_base +values
        try:
            self.cs.execute(sql)
            self.conn_mysql.commit()
            self.total_success_count += self.current_success_count
        except:
            self.total_failure_count += self.current_success_count
            print(values)

    def run(self):
        self.get_table_column()
        time_existence = time.clock()
        redis_sql = 'spop %s %d'%(self.settings.redis_key,self.settings.max_spop_count)
        mysql_sql_base = 'insert into %s (%s) values'%(self.settings.mysql_table,','.join(self.column_name_s))
        while True:
            value = self.get_redis_data(redis_sql)
            if value:
                self.insert_into_mysql(mysql_sql_base,value)
                print('total success count:',self.total_success_count)
                time_existence = time.clock()
            else:
                time_empty = time.clock()
                time.sleep(1)
                if time_empty - time_existence >= self.settings.max_wait_time:
                    self.cs.close()
                    self.conn_mysql.close()
                    print('total failure count:',self.total_failure_count)
                    break

if __name__ == '__main__':
    settings = SettingsDefault()
    settings.max_spop_count = 100
    server = Server(settings)
    server.run()
