# -*- coding: utf-8 -*-
import mysql.connector,os
import logging
from dotenv import load_dotenv

load_dotenv()

class BaseModel():
        def __init__(self):
                self.table_name = False
                self.where = ""
                self.limit = 1000
                self.offset = 0
                self.force_unique = False
                self.host = os.environ['dbhost']
                self.db = os.environ['dbdb']
                self.user = os.environ['dbuser']
                self.password = os.environ['dbpass']
                self.mysql = mysql
                self.cnx = None
                self.fields = {}
                self.cached_searches = {}

        def db_connect(self, reuse=False):
                import os
                if reuse and self.cnx != None:
                        logging.debug(f"Reusing Connection: {self.cnx}")
                        return self.cnx
                config = {
                        "host": self.host,
                        "database": self.db,
                        "user": self.user,
                        "password": self.password
                }
                if reuse:
                        self.cnx = mysql.connector.connect(**config, pool_name=os.environ['DB_POOL_NAME'], pool_size=int(os.environ['DB_POOL_SIZE']))
                else:
                        self.cnx = mysql.connector.connect(**config)
                logging.debug(f"DB Connection: {self.cnx}")
                # this will break the connection
                # self.cnx.set_charset_collation('utf8mb4')
                return self.cnx

        def getAll(self, limit=None, order_by='id', order="DESC", published=False):
                if limit: self.limit = limit
                where = "WHERE 1=1"
                query = f"SELECT * FROM {self.table_name} {where} ORDER BY {order_by} {order} LIMIT {self.offset}, {self.limit}"
                print(query)
                cnx = self.db_connect()
                cursor = cnx.cursor(dictionary=True)
                cursor.execute(query)
                results = self.format(cursor.fetchall())
                return results

        def getSet(self, where="1=1", limit=1000, offset=0, order="desc", order_by="id"):
                self.limit = limit
                query = f"SELECT * FROM {self.table_name} WHERE {where} ORDER BY {order_by} {order} LIMIT {self.offset}, {self.limit}"
                cnx = self.db_connect()
                cursor = cnx.cursor(dictionary=True)
                cursor.execute(query)
                results = self.format(cursor.fetchall())
                return results

        def format(self, results, additional_fields={}):
                import datetime
                data = []
                fields = self.fields
                fields.update(additional_fields)
                for result in results:
                        for k, t in fields.items():
                                if k in result.keys():
                                        if type(result[k]) == bytes and k == 'content':
                                                result[k] = result[k].decode('utf-8')
                                if k in result.keys() and isinstance(result[k], datetime.datetime):
                                        result[k] = result[k].strftime('%Y-%m-%d %H:%M:%S')
                                if k in result.keys() and t == 'float':
                                        if '' == result[k] or 'none' == result[k] or ' ' == result[k] or '?' == result[k] or None == result[k]:
                                                result[k] = 0.0
                                        result[k] = float(result[k])
                                if k in result.keys() and t == 'string':
                                        if type(result[k]) == 'bytes':
                                                result[k] = result[k].decode('utf-8')
                                if k in result.keys() and t == 'int':
                                        if '' == result[k] or 'none' == result[k] or ' ' == result[k] or '?' == result[k] or 'None' == result[k]:
                                                result[k] = 0
                        data.append(result)
                return data

        def create(self, obj):
                data = {}
                keys = [key for key in obj.keys()]
                if self.force_unique:
                        if self.search(self.force_unique, obj.get(self.force_unique)):
                                raise Exception(f"An entry already exists for {obj[self.force_unique]}")
                for name, ftype in self.fields.items():
                        if name not in keys: continue
                        # data[name] = "'"+str(obj[name])+"'"
                        data[name] = str(obj[name])
                columns = ",".join(["`"+key+"`" for key in data.keys()])
                values = [val for val in data.values()]
                subs = self.subFormat(data.keys())
                query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({subs});"
                try:
                    cnx = self.db_connect(reuse=False)
                    cursor = cnx.cursor(dictionary=True)
                    cursor.execute(query, tuple(values))
                    cnx.commit()
                    return cursor.lastrowid
                except Exception as e:
                    logging.debug(f"[ERROR] {e}")


        def subFormat(self, columns):
                subs = []
                for col in columns:
                                subs.append('%s')
                return ",".join(subs)

        def update(self, obj, reuse=False):
                data = {}
                if type(obj) != list:
                        obj = [obj]
                obj = self.format(obj)[0]
                keys = [key for key in obj.keys()]
                for name, ftype in self.fields.items():
                        if name not in keys: continue
                        data[name] = '"'+str(obj[name])+'"'
                columns = ",".join(["`"+key+"`" for key in data.keys()])
                values = ",".join([val for val in data.values()])
                query = f"REPLACE INTO {self.table_name} ({columns}) VALUES ({values});"
                cnx = self.db_connect(reuse=reuse)
                cursor = cnx.cursor(dictionary=True)
                cursor.execute(query)
                cnx.commit()
                return cursor.lastrowid

        def update_field(self, id, field, value):
                data = {}
                query = f"UPDATE {self.table_name} set {field} = %s WHERE id = {id};"
                cnx = self.db_connect()
                cursor = cnx.cursor(dictionary=True)
                cursor.execute(query, [value])
                cnx.commit()
                return cursor.lastrowid

        def search(self, field, match, one_entry=False, operator='=', key=None, group_by=None, bypass_cache=False):
                try:
                        result = self.cached_searches[f"{field}:{match}"]
                        if bypass_cache:
                                raise Exception("Skip cache")
                except Exception as e:
                  logging.info(e)
                user = False
                query = f"SELECT * from {self.table_name} where `{field}` {operator} %s"
                if group_by:
                        query = f"{query} GROUP BY {group_by}"
                query = f"{query};"
                cnx = self.db_connect()
                cursor = cnx.cursor(dictionary=True)
                cursor.execute(query, (match,))
                result = self.format(cursor.fetchall())
                if user:
                        for i in range(0, len(result)):
                                result[i]['user'] = user
                if len(result) < 1:
                        result = False
                elif one_entry:
                        if key:
                                result = result[0][key]
                        else:
                                result = result[0]
                self.cached_searches[f"{field}:{match}"] = result
                return result

        def delete(self, id):
                query = f"DELETE FROM {self.table_name} where `id` = %s"
                cnx = self.db_connect(reuse=False)
                cursor = cnx.cursor(dictionary=True)
                cursor.execute(query, (id,))
                cnx.commit()
                return True

        def getResult(self, query, query_args=(), additional_fields={}):
                """ Just run a simple query and return a result """
                cnx = self.db_connect()
                cursor = cnx.cursor(dictionary=True)
                cursor.execute(query, query_args)
                result = cursor.fetchall()
                cnx.commit()
                if result:
                        return self.format(result, additional_fields=additional_fields)
                return None

class MyConverter(mysql.connector.conversion.MySQLConverter):
    def row_to_python(self, row, fields):
        row = super(MyConverter, self).row_to_python(row, fields)

        def to_unicode(col):
            if isinstance(col, bytearray):
                return col.decode('utf-8')
            return col

        return[to_unicode(col) for col in row]
