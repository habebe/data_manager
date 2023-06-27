import configparser
import pathlib
import os
import urllib.parse 
import enum
import logging 

class LoggingLevel(enum.IntEnum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    pass

class Configuration:
    __instance__ = None

    __HOME_ENV__ = "DATA_UTIL_CONFIG_FILE"

    __HOME_DIR__ = ".data_util"
    __DATA_DIR__ = "data"
    __CACHE_DIR__ = "cache"
    __HDF_DIR__ = "hdf"
    
    __CONFIG_FILE_NAME__ = "data_util.ini"
    __DB_NAME__ = "data_util.db"


    @classmethod
    def instance(self,reload=False):
        if self.__instance__ == None or reload == True:
            self.__instance__ = Configuration()
            pass
        return self.__instance__
    
    @classmethod
    def get_home_env(self):
        return self.__HOME_ENV__
    
    @classmethod
    def get_home_dir(self):
        return self.__HOME_DIR__

    @classmethod
    def get_data_dir(self):
        return self.__DATA_DIR__
    
    @classmethod
    def get_cache_dir(self):
        return self.__CACHE_DIR__

    @classmethod
    def get_hdf_dir(self):
        return self.__HDF_DIR__

    @classmethod
    def get_config_file_name(self):
        return self.__CONFIG_FILE_NAME__
    
    @classmethod
    def get_db_name(self):
        return self.__DB_NAME__

    def __init__(self):
        self.__home__ = None
        self.__data_path__ = None
        self.__db_path__ = None
        self.__config_file_path__ = None
        self.__cache_path__ = None
        self.__hdf_path__ = None

        self.__is_setup__ = False
        self.__config_parser__ = None
        self.__init_home__()
        pass
    
    def get_home_path(self):
        return self.__home__
    
    def get_data_path(self):
        return self.__data_path__

    def get_db_path(self):
        return self.__db_path__
    
    def get_config_file_path(self):
        return self.__config_file_path__
    
    def get_hdf_path(self,source = None):
        if source == None:
            return self.__hdf_path__
        elif type(source) == int:
            return self.__hdf_path__ / pathlib.Path(str(source) + ".hdf")
        else:
            return self.__hdf_path__ / pathlib.Path(str(source))

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

    def is_setup(self):
        return self.__is_setup__

    def get_config_parser(self):
        return self.__config_parser__

    def setup(self):
        if not self.is_setup():
            self.__setup_paths__()
            if self.get_config_file_path().exists() == False:
                self.__create_config_file__()
                pass
            self.reload()
            pass
        pass

    def reload(self):
        self.__config_parser__ = configparser.ConfigParser()
        self.__config_parser__.read(self.get_config_file_path())
        pass
   
    def __setup_paths__(self):
        self.__home__.mkdir(parents=True,exist_ok=True)
        self.__data_path__.mkdir(parents=True,exist_ok=True)
        self.__hdf_path__.mkdir(parents=True,exist_ok=True)
        self.__cache_path__.mkdir(parents=True,exist_ok=True)
        self.__is_setup__ = True
        pass

    def __init_home__(self):
        if self.get_home_env() in os.environ:
            self.__home__ = pathlib.Path(os.environ[self.get_home_env()])
        else:
            self.__home__ = pathlib.Path.home() / self.get_home_dir()
            pass
        self.__config_file_path__ = self.__home__ / self.get_config_file_name()
        self.__data_path__ = self.__home__ / self.get_data_dir()
        self.__hdf_path__ = self.__data_path__ / self.get_hdf_dir()
        self.__cache_path__ = self.__data_path__ / self.get_cache_dir()
        self.__db_path__ = self.__data_path__ / self.get_db_name()
        pass

    def __create_paths__(self):
        self.__setup_paths__()
        
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


