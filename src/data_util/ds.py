import pathlib
import pandas as pd
import tarfile
import urllib.request
import urllib.parse 
import os
import datetime
import numpy as np
from zlib import crc32
import shutil 
import logging
import hashlib

from . import db
from . import config

class Dataset:
    def __init__(self):
        self.meta_data = None
        self.data = {}
        pass
    pass

class Util:
    @classmethod
    def file_hash(self,filename):
        path = pathlib.Path(filename)
        sha256_hash = None
        if path.is_file():
            sha256_hash = hashlib.sha256()
            with open(filename,"rb") as f:
                for byte_block in iter(lambda: f.read(4096),b""):
                    sha256_hash.update(byte_block)
                    pass
                pass
            sha256_hash = sha256_hash.hexdigest()
            pass
        return sha256_hash
    pass

class Loader:
    __instance__ = None
    
    @classmethod
    def instance(self,reload=False):
        if self.__instance__ == None or reload == True:
            self.__instance__ = Loader()
            pass
        return self.__instance__ 

    def __init__(self):
        pass

    def load_url(self,source,refresh=False):
        logging.debug(f"load_url source:{source} refresh:{refresh}")
        database = db.Database.instance()
        configuration = config.Configuration.instance()
        ds = None
        if source != None:
            ds = Dataset()
            ds.meta_data = database.select(source=source,unique=True)
            
            cache_path = configuration.get_cache_path(source)
            file_hash = Util.file_hash(cache_path)

            should_retrieve = (cache_path.is_file() == False) or (refresh == True) or (ds.meta_data == None) or (file_hash == None)
            logging.debug(f"\tload_url should_retrieve:{should_retrieve}")
            logging.debug(f"\tload_url cache_path:{cache_path}")
            logging.debug(f"\tload_url file_hash:{file_hash}")
            logging.debug(f"\tload_url ds.meta_data :{ds.meta_data }")
            if should_retrieve:
                cache_path = self.retrieve_file(source)
                timestamp = datetime.datetime.now()
                file_hash = Util.file_hash(cache_path)
                if ds.meta_data == None:
                    id = database.insert(source,None,cache_path,file_hash,timestamp)
                    ds.meta_data = database.select(id=id,unique=True)
                else:
                    database.update(ds.meta_data.id,source,None,cache_path,file_hash,timestamp)
                    ds.meta_data = database.select(id=ds.meta_data.id,unique=True)
                logging.debug(f"\t\tload_url ds.meta_data :{ds.meta_data }")
                pass
        print(ds.meta_data)
        return ds

    def retrieve_file(self,source,destination = None):
        retrieved_file = None
        try:
            logging.debug(f"retrieve_file source:{source}")
            (retrieved_file,response) = urllib.request.urlretrieve(source)
            if retrieved_file != None:
                retrieved_file = pathlib.Path(retrieved_file)
                if destination == None:
                    destination = config.Configuration.instance().setup_cache_path(source)
                shutil.move(str(retrieved_file),str(destination))
                retrieved_file = destination
                logging.debug(f"retrieve_file destination:{destination}")
        except Exception as e:
            print(f"[exception] retrieve_file: {e}")
        return retrieved_file
    
class __dataset:
    def __init__(self):
        self.data_descriptions = {}
        self.data = {}
        pass

    def __load_csv__(self,file_path):
        return pd.read_csv(file_path)

    def load(self,description):
        if type(description) == int:
            description = self.data_descriptions[description]
            pass
        if (description.data_path.suffix.lower()) == ".csv":
            self.data[description.id] = self.__load_csv__(description.data_path)
            pass
        return self

    def load_all(self,refresh=False):
        for description in self.data_descriptions:
            if not (description.id in self.data) or (refresh == True):
                self.load(description)
                pass
            pass
        return self

    def get(self,id):
        if not (id in self.data):
            self.load(id)
        if id in self.data:
            return self.data[id]
        return None

    def describe(self,id):
        if id in self.data_descriptions:
            return self.data_descriptions[id]
        return None
        

class __loader:
    def __init__(self):
        self.db = db.db.instance()
        self.data_description = None
        self.children_descriptions = None
        self.dataset = None
        pass

    def __load_csv__(self,file_path):
        return pd.read_csv(file_path)

    def __get_data_key__(self,data_path,root_path,index=None):
        key = str(data_path.relative_to(root_path)).replace(os.sep,"/")
        if index == None:
            if key in self.data:
                return self.__get_data_key__(data_path,root_path,1)
            return key
        key = "{0}-{1}".format(key,index)
        if key in self.data:
            return self.__get_data_key__(data_path,root_path,index+1)
        return key

    def load_csv(self):
        self.data = {}
        if self.data_description:
            if (self.data_description.data_path.suffix.lower()) == ".csv":
                self.data[self.__get_data_key__(self.data_description.data_path)] = self.__load_csv__(self.data_description.data_path)
                pass
            self.generated_data_descriptions = self.db.select_generated(source_id = self.data_description.id)
            print(f"generated_data_descriptions : {self.generated_data_descriptions}  self.data_description.data_path: {self.data_description.data_path}")
            if self.generated_data_descriptions:
                root_path = path.get_data_generated_path(self.data_description.data_path)
                for generated_data_description in self.generated_data_descriptions:
                    if (generated_data_description.data_path.suffix.lower()) == ".csv":
                        self.data[self.__get_data_key__(generated_data_description.data_path,root_path)] = self.__load_csv__(generated_data_description.data_path)
                        pass
                    pass
                pass
        return self

    def retrieve_file(self,source,destination = None):
        retrieved_file = None
        try:
            (retrieved_file,response) = urllib.request.urlretrieve(source)
            if retrieved_file != None:
                retrieved_file = pathlib.Path(retrieved_file)
                if destination == None:
                    destination = config.Configuration.instance().setup_cache_path(source)
                shutil.move(str(retrieved_file),str(destination))
                retrieved_file = destination
        except Exception as e:
            print(f"[exception] retrieve_file: {e}")
        return retrieved_file


    def load_url(self,source,refresh=False):
        if source != None:
            self.data_description = self.db.select(source=source,unique=True)
            cache_path = config.Configuration.instance().get_cache_path(source)
            should_retrieve = (cache_path.is_file() == False) or (refresh == True) or (self.data_description == None)
            if should_retrieve:
                print("retrieve data : {0}".format(cache_path))
                cache_path = self.retrieve_file(source)
                timestamp = datetime.datetime.now()
                if self.data_description == None:
                    id = self.db.insert(source,None,cache_path,timestamp)
                    self.data_description = self.db.select(id=id,unique=True)
                    pass
                else:
                    self.db.update(self.data_description.id,source,None,cache_path,timestamp)
                    self.data_description = self.db.select(id=self.data_description.id,unique=True)
                pass
            self.extract(source,refresh)
            self.create_dataset()
        return self

    def create_dataset(self):
        self.dataset = dataset()
        if self.data_description: 
            self.dataset.data_descriptions[self.data_description.id] = self.data_description
            pass
        if self.children_descriptions and len(self.children_descriptions):
            for description in self.children_descriptions:
                self.dataset.data_descriptions[description.id] = description
                pass
        return self

    def extract(self,source,refresh=False):
        if self.data_description != None and self.data_description.data_path.is_file():
            self.children_descriptions = self.db.select(parent = self.data_description.id)
            if (self.data_description.data_path.suffix.lower()) == ".tgz":
                extract_path = config.Configuration.instance().setup_url_data_generated_path(source)
                with tarfile.open(self.data_description.data_path) as tarball:
                    extracted_files = self.__collect_extracted_files__(tarball,extract_path)
                    should_extract = refresh or (self.__extracted_files_exists__(extracted_files) == False)
                    if should_extract:
                        print("extracting : {0}".format(self.data_description.data_path))
                        tarball.extractall(path=extract_path)
                        timestamp = datetime.datetime.now()
                        mapped_children = self.__map_children__()
                        for extracted_file in extracted_files:
                            print("extracted file : {0}".format(extracted_file))
                            if extracted_file in mapped_children:
                                child_description = mapped_children[extracted_file]
                                self.db.update(child_description.id, "tarball.extract", self.data_description.id, extracted_file, timestamp)
                            else:
                                self.db.insert("tarball.extract",self.data_description.id, extracted_file, timestamp) 
                            pass
                        pass
                    self.children_descriptions = self.db.select(parent = self.data_description.id)
                pass
            pass
        return self

    def __extract__(self,source,refresh=False):
        source_url = urllib.parse.urlparse(source)
        self.data_description = self.db.select(source=source,unique=True)
        if self.data_description != None:
            if self.data_description.data_path.is_file():
                self.generated_data_descriptions = self.db.select_generated(source_id = self.data_description.id)
                if (self.data_description.data_path.suffix.lower()) == ".tgz":
                    extract_path = config.Configuration.instance().setup_data_generated_path(source_url)
                    with tarfile.open(self.data_description.data_path) as tarball:
                        extracted_files = self.__collect_extracted_files__(tarball,extract_path)
                        should_extract = refresh or (self.__extracted_files_exists__(extracted_files) == False)
                        if should_extract:
                            print("extracting : {0}".format(self.data_description.data_path))
                            tarball.extractall(path=extract_path)
                            timestamp = datetime.datetime.now()
                            mapped_files = self.__map_generated_file__()
                            for extracted_file in extracted_files:
                                print("extracted file : {0}".format(extracted_file))
                                if extracted_file in mapped_files:
                                    generated_description = mapped_files[extracted_file]
                                    self.db.update_generated(generated_description.id, self.data_description.id, extracted_file, "tarball.extract", timestamp)
                                else:
                                    self.db.insert_generated(self.data_description.id, extracted_file, "tarball.extract", timestamp)
                                pass
                            pass
                        pass
                    pass
                pass
            pass
        return self

    def __collect_extracted_files__(self,tarball,extract_path):
        exectracted_files = []
        for member in tarball.getmembers():
            if (member.isfile()):
                member_path = extract_path / pathlib.Path(member.name)
                exectracted_files.append(member_path)
            pass
        return exectracted_files

    def __extracted_files_exists__(self,exectracted_files):
        done = False
        iterator = iter(exectracted_files)
        exists = True
        while not done:
            file_path = next(iterator,None)
            if file_path:
                if (file_path.exists() == False) or (file_path.is_file() == False):
                    exists = False
                    pass
            else:
                done = True
            pass
        return exists

    def __map_children__(self):
        result = {}
        if self.children_descriptions and len(self.children_descriptions):
            for description in self.children_descriptions:
                result[description.data_path] = description
                pass
        return result

