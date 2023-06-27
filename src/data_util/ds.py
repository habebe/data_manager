import pathlib
import pandas as pd
import tarfile
import urllib.request
import urllib.parse 
import datetime
import numpy as np
from zlib import crc32
import shutil 
import logging
import hashlib
import h5py

from . import db
from . import config

class Dataset:
    def __init__(self):
        self.__meta_data__ = None
        self.__data__ = {}
        self.__hdf_path__ = None
        pass

    def meta_data(self): 
        return self.__meta_data__

    def data(self):
        return self.__data__
    
    def hdf_path(self):
        return self.__hdf_path__
    
    def set_meta_data(self,data):
        self.__meta_data__ = data
        pass

    def set_hdf_path(self,path):
        self.__hdf_path__ = path
        pass

    def set_data(self,name,value):
        self.__data__[name] = value
        pass

    def to_df(self,name):
        result = None
        if name in self.__data__:
            if self.__data__[name] == None:
                self.__data__[name] = pd.read_hdf(self.hdf_path(),name)
                pass
            result = self.__data__[name]
            pass
        return result
    pass

class HashFile:
    def __call__(self,filename):
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

class RetrieveFile:
    def __call__(self,source,destination = None):
        retrieved_file = None
        try:
            logging.debug(f"RetrieveFile source:{source}")
            (retrieved_file,response) = urllib.request.urlretrieve(source)
            if retrieved_file != None:
                if destination != None:
                    shutil.move(str(retrieved_file),str(destination))
                    retrieved_file = destination
                    pass
                retrieved_file = pathlib.Path(retrieved_file)
                logging.debug(f"RetrieveFile destination:{destination}")
        except Exception as e:
            print(f"[exception] RetrieveFile: {e} {response}")
            raise e
        return retrieved_file


class OpenTarFile:
    def __call__(self,source):
        result = source
        if source.is_file():
            try:
                tarball = tarfile.open(source) 
                result = tarball
            except Exception as e:
                pass
            pass
        return result
    pass

class CollectTarFiles:
    def __call__(self,source,extract_path):
        destination = []
        if isinstance(source,tarfile.TarFile):
            for member in source.getmembers():
                if (member.isfile()):
                    member_path = extract_path / pathlib.Path(member.name)
                    destination.append(member_path)
                    logging.debug(f"CollectTarFiles file:{member_path}")
                    pass
            pass
        else:
            destination.append(source)
            pass
        return destination
    pass

class ExtractFiles:
    def __call__(self,source):
        destination = []
        if isinstance(source,tarfile.TarFile):
            extract_path = dir(source)
            extract_path = pathlib.Path(source.name).parent
            logging.debug(f"ExtractFiles file:{source} -> {extract_path}")
            source.extractall(path=extract_path)
            collector = CollectTarFiles()
            destination = collector(source,extract_path)
        else:
            destination.append(source)
            pass
        return destination
    pass


class CSVToHDF:
    def __call__(self,dataset,file_paths):
        if file_paths != None:
            store = pd.HDFStore(dataset.hdf_path())
            for file_path in file_paths:
                if file_path.suffix.lower() == ".csv":
                    name,suffix = file_path.name,file_path.suffix
                    variable_name = name[:-len(suffix)].replace("-","_")
                    data_frame = pd.read_csv(file_path)
                    store.put(variable_name,data_frame,format="table")
                    dataset.set_data(variable_name,None)
                    pass   
                pass
            pass
        pass
    pass

class HDFKeys:
    def __call__(self,dataset):
        keys = None
        if dataset != None and dataset.hdf_path() and dataset.hdf_path().is_file():
            hfd_file = h5py.File(dataset.hdf_path(), 'r')
            keys = [key for key in hfd_file.keys()]
            hfd_file.close()
            pass
        else:
            keys = []
        return keys
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

    def load(self,source,refresh=False):
        logging.debug(f"load source:{source} refresh:{refresh}")
        database = db.Database.instance()
        configuration = config.Configuration.instance()
        ds = None
        hasher = HashFile()
        retriever = RetrieveFile()
        tarFileOpener = OpenTarFile()
        extractor = ExtractFiles()
        csv_to_hdf = CSVToHDF()
        hdf_keys = HDFKeys()
        if source != None:
            cache_path = configuration.get_cache_path(source)
            ds = Dataset()
            ds.set_meta_data(database.select(source=source,unique=True))
            file_hash = hasher(cache_path)
            should_retrieve = (cache_path.is_file() == False) or (refresh == True) or (ds.meta_data() == None) or (file_hash == None) or (file_hash != ds.meta_data().hash())
            if should_retrieve:
                timestamp = datetime.datetime.now()
                cache_path = config.Configuration.instance().setup_cache_path(source)
                file_path = retriever(source,cache_path)
                if ds.meta_data() == None:
                    id = database.insert(source,None,file_path,file_hash,timestamp)
                    ds.set_meta_data(database.select(id=id,unique=True))
                else:
                    database.update(ds.meta_data().id(),source,None,file_path,file_hash,timestamp)
                    ds.set_meta_data(database.select(id=ds.meta_data().id(),unique=True))
                    pass
                ds.set_hdf_path(configuration.get_hdf_path(ds.meta_data().id()))
                result = tarFileOpener(file_path)
                result = extractor(result)
                csv_to_hdf(ds,result)
                pass
            else:
                logging.debug("Loading from cache.")
                ds.set_hdf_path(configuration.get_hdf_path(ds.meta_data().id()))
                keys = hdf_keys(ds)
                for key in keys:
                    ds.set_data(key,None)
                    pass
                pass
            pass
        return ds
