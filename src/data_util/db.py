import sqlite3
import pathlib
import json
import logging

from . import config

class MetaData:
    def __init__(self,row):
        self.__id__ = row[0]
        self.__source__ = row[1]
        self.__parent__ = row[2]
        self.__path__ = pathlib.Path(row[3])
        self.__hash__ = row[4]
        self.__timestamp__ = row[5]
        pass
    
    def id(self):
        return self.__id__
    
    def source(self):
        return self.__source__
    
    def parent(self):
        return self.__parent__
    
    def path(self):
        return self.__path__

    def hash(self):
        return self.__hash__
    
    def timestamp(self):
        return self.__timestamp__
    
    def to_map(self):
        return {
            f"{Database.column_id}":self.id(),
            f"{Database.column_source}":self.source(),
            f"{Database.column_parent}":self.parent(),
            f"{Database.column_path}":str(self.path()),
            f"{Database.column_hash}":str(self.hash()),
            f"{Database.column_timestamp}":self.timestamp()
        }

    def to_list(self):
        return [self.__id__,self.__source__,self.__path__,self.__hash__,self.__timestamp__]

    def to_json(self):
        return json.dumps(self.to_map())
    
    def __repr__(self):
        return self.to_json()
    pass


class Database:
    __instance__ = None
    table_meta_data = "meta_data"
    column_id = "id"
    column_source = "source"
    column_parent = "parent"
    column_path = "path"
    column_hash = "hash"
    column_timestamp = "timestamp"

    columns = None

    @classmethod
    def get_columns(self):
        if self.columns == None:
            self.columns = [
                self.column_id,
                self.column_source,
                self.column_parent,
                self.column_path,
                self.column_hash,
                self.column_timestamp
            ]
            pass
        return self.columns

    @classmethod
    def instance(self,reload=False):
        if self.__instance__ == None or reload == True:
            self.__instance__ = Database()
            pass
        return self.__instance__


    def __init__(self):     
        self.create_table()
        pass

    def connect(self):
        config.Configuration.instance().setup()
        db_path = config.Configuration.instance().get_db_path()
        logging.debug(f"database.connect db_path:{db_path}")
        connection = sqlite3.connect(str(db_path))
        return connection

    def create_table(self):
        connection = self.connect()
        cursor = connection.cursor()
        statement = f"CREATE TABLE IF NOT EXISTS {self.table_meta_data} ({self.column_id} INTEGER PRIMARY KEY AUTOINCREMENT, {self.column_source} INTEGER, {self.column_parent} TEXT, {self.column_path} TEXT, {self.column_hash} TEXT,{self.column_timestamp} timestamp)"
        cursor.execute(statement)
        connection.close()
        pass    

    def insert(self,source,parent,path,hash,timestamp):
        connection = self.connect()
        cursor = connection.cursor()
        statement = f"""INSERT INTO '{self.table_meta_data}' ('{self.column_source}', '{self.column_parent}', '{self.column_path}', '{self.column_hash}', '{self.column_timestamp}') VALUES (?, ?, ?, ?, ?)"""
        result = cursor.execute(statement,(str(source), parent, str(path), hash, timestamp))
        id = result.lastrowid
        logging.debug("insert({0},{1},{2},{3}) -> {4}".format(str(source), parent, str(path), str(hash), timestamp,id))
        connection.commit()
        connection.close()
        return id
    
    def update(self,id,source,parent,path,hash,timestamp):
        connection = self.connect()
        cursor = connection.cursor()
        column_statement = ""
        statement = f"""UPDATE {self.table_meta_data} SET {self.column_source} = ? , {self.column_parent} = ? , {self.column_path} = ? , {self.column_hash} = ?, {self.column_timestamp} = ? WHERE id=?"""
        result = cursor.execute(statement,(str(source), parent, str(path), hash, timestamp,id))
        id = result.lastrowid
        logging.debug("update({0},{1},{2},{3},{4}) -> {5}".format(str(source), parent, str(path), hash, timestamp,id))
        connection.commit()
        connection.close()
        return id

    def select(self,source = None,parent = None,id=None,unique=False):
        meta_data_list = []
        connection = self.connect()
        cursor = connection.cursor()
        if id != None:
            statement = """SELECT * FROM {0} where id=?""".format(self.table_meta_data)
            cursor.execute(statement,(id,))
            pass
        elif source != None:
            statement = """SELECT * FROM {0} where source=?""".format(self.table_meta_data)
            cursor.execute(statement,(source,))
            pass
        elif parent != None:
            statement = """SELECT * FROM {0} where parent=?""".format(self.table_meta_data)
            cursor.execute(statement,(parent,))
            pass
        else:
            statement = """SELECT * FROM {0}""".format(self.table_meta_data)
            cursor.execute(statement)
            pass
        rows = cursor.fetchall()
        for row in rows:
            meta_data_list.append(MetaData(row))
            pass
        connection.close()
        if (unique == True):
            if len(meta_data_list):
                meta_data_list = meta_data_list[0]
                pass
            else:
                meta_data_list = None
        return meta_data_list

