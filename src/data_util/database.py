import sqlite3
import pathlib
import json

from . import config

class data_description:
    def __init__(self,row):
        self.row = row
        self.id = row[0]
        self.parent = row[1]
        self.source = row[2]
        self.data_path = pathlib.Path(row[3])
        self.timestamp = row[4]
        pass
    
    def json(self):
        return json.dumps(
            {
            "id":self.id,
            "parent":self.parent,
            "source":self.source,
            "data_path":str(self.data_path),
            "timestamp":self.timestamp
            }
        )

    def __repr__(self):
        return self.json()
    pass


class db:
    __instance__ = None
    table_data_description = "data_description"

    @classmethod
    def instance(self):
        if self.__instance__ == None:
            self.__instance__ = db()
            pass
        return self.__instance__


    def __init__(self):     
        self.create_table()
        pass

    def connect(self):
        data_path = config.Configuration.instance().db_path()
        connection = sqlite3.connect(str(data_path))
        return connection

    def create_table(self):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS {0} (id INTEGER PRIMARY KEY AUTOINCREMENT, parent INTEGER, source TEXT, data_path TEXT, timestamp timestamp)".format(self.table_data_description))
        connection.close()
        pass    

    def insert(self,source,parent,data_path,timestamp):
        connection = self.connect()
        cursor = connection.cursor()
        statement = """INSERT INTO '{0}' ('source', 'parent', 'data_path', 'timestamp') VALUES (?, ?, ?, ?)""".format(self.table_data_description)
        result = cursor.execute(statement,(str(source), parent, str(data_path), timestamp))
        id = result.lastrowid
        print("insert({0},{1},{2}) -> {3}".format(str(source), parent, str(data_path), timestamp,id))
        connection.commit()
        connection.close()
        return id
    
    def update(self,id,source,parent,data_path,timestamp):
        connection = self.connect()
        cursor = connection.cursor()
        statement = """UPDATE {0} SET source = ? , parent = ? , data_path = ? , timestamp = ? WHERE id=?""".format(self.table_data_description)
        result = cursor.execute(statement,(str(source), parent, str(data_path), timestamp,id))
        id = result.lastrowid
        print("update({0},{1},{2},{3}) -> {4}".format(str(source), parent, str(data_path), timestamp,id))
        connection.commit()
        connection.close()
        return id

    def select(self,source = None,parent = None,id=None,unique=False):
        data_descriptions = []
        connection = self.connect()
        cursor = connection.cursor()
        if id != None:
            statement = """SELECT * FROM {0} where id=?""".format(self.table_data_description)
            cursor.execute(statement,(id,))
            pass
        elif source != None:
            statement = """SELECT * FROM {0} where source=?""".format(self.table_data_description)
            cursor.execute(statement,(source,))
            pass
        elif parent != None:
            statement = """SELECT * FROM {0} where parent=?""".format(self.table_data_description)
            cursor.execute(statement,(parent,))
            pass
        else:
            statement = """SELECT * FROM {0}""".format(self.table_data_description)
            cursor.execute(statement)
            pass
        rows = cursor.fetchall()
        for row in rows:
            data_descriptions.append(data_description(row))
        connection.close()
        if (unique == True):
            if len(data_descriptions):
                data_descriptions = data_descriptions[0]
                pass
            else:
                data_descriptions = None
        return data_descriptions

