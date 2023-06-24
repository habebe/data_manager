import configparser
import pathlib
import os
import urllib.parse 

class Configuration:
    __instance__ = None
    __HOME_ENV__ = "DATA_UTIL_CONFIG_FILE"
    __QUANTA_DIR__ = ".data_util"
    __DATA_DIR__ = "data"
    __CONFIG_FILE_NAME__ = "data_util.ini"

    __DB_NAME__ = "data_util.db"
    __CACHE_NAME__ = "cache"

    @classmethod
    def instance(self):
        if self.__instance__ == None:
            self.__instance__ = Configuration()
            pass
        return self.__instance__
    
    def __init__(self):
        self.__home__ = None
        self.__init_home__()
        pass

    def get_db_path(self):
        return self.__data_path__ / pathlib.Path(self.__DB_NAME__)
    
    def get_home(self):
        return self.__home__
    
    def get_cache_path(self,source=None):
        if source == None:
            return self.__cache_path__
        elif type(source) == urllib.parse.ParseResult:
            data_path = self.__cache_path__ / pathlib.Path(source.netloc + os.sep + source.path)
            return data_path
        elif type(source) == str:
            return self.get_cache_path(urllib.parse.urlparse(source))
        return None

    def setup_cache_path(self,source):
        data_path = self.get_cache_path(source)
        if data_path != None:
            data_directory = pathlib.Path(data_path.parent)
            data_directory.mkdir(parents=True,exist_ok=True)
            pass
        return data_path

    def get_url_data_generated_path(self,source):
        if type(source) == urllib.parse.ParseResult:
            data_path = self.get_cache_path(source)
            generated_directory = pathlib.Path(data_path.parent) / pathlib.Path("_generated_")
            return generated_directory
        elif type(source) == str:
            return self.get_url_data_generated_path(urllib.parse.urlparse(source))
        return None

    def setup_url_data_generated_path(self,source):
        data_path = self.get_url_data_generated_path(source)
        if data_path != None:
            data_path.mkdir(parents=True,exist_ok=True)
            return data_path
        return None

    def reload(self):
        self.config = configparser.ConfigParser()
        self.config.read(self.__config_file_path__)
        pass
   
    def __setup_paths__(self):
        self.__home__.mkdir(parents=True,exist_ok=True)
        self.__data_path__.mkdir(parents=True,exist_ok=True)
        self.__cache_path__.mkdir(parents=True,exist_ok=True)
        pass

    def __init_home__(self):
        if self.__HOME_ENV__ in os.environ:
            self.__home__ = self.__HOME_ENV__
        else:
            self.__home__ = pathlib.Path.home() / self.__QUANTA_DIR__
            pass
        self.__config_file_path__ = self.__home__ / self.__CONFIG_FILE_NAME__
        self.__data_path__ = self.__home__ / self.__DATA_DIR__
        self.__cache_path__ = self.__data_path__ / self.__CACHE_NAME__
        self.__setup_paths__()
        if self.__config_file_path__.exists() == False:
            self.__create_config_file__()
            pass
        self.reload()
        pass

    def __create_config_file__(self):
        config = configparser.ConfigParser()
        config['torch'] = {
            'device': 'cuda'
        }
        config['data.store'] = {
            'type': 'sqlite'
        }
        with open(self.__config_file_path__, 'w') as configfile:
            config.write(configfile)
            pass
        pass


