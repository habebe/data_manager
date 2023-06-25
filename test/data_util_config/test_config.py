from .. import common_util
import unittest
import data_util
import data_util.config
import os
import pathlib

class TestConfig(unittest.TestCase):

    def test_default_path(self):
        config = data_util.get_config(reload=True)
        home = config.get_home_path()
        data_path = config.get_data_path()
        config_file_path = config.get_config_file_path()
        db_path = config.get_db_path()
        cache_path = config.get_cache_path()
        
        self.assertEqual(data_path, home / config.get_data_dir())
        self.assertEqual(config_file_path, home / config.get_config_file_name())
        self.assertEqual(db_path, data_path / config.get_db_name())
        self.assertEqual(cache_path, data_path / config.get_cache_dir())

        self.assertFalse(config.is_setup())
        config.setup()
        self.assertTrue(config.is_setup())
        pass

    def get_home_env(self):
        return None if not data_util.config.Configuration.get_home_env() in os.environ else os.environ[data_util.config.Configuration.get_home_env()]

    def reset_home_env(self,value):
        if value == None:
           if data_util.config.Configuration.get_home_env() in os.environ:
               del os.environ[data_util.config.Configuration.get_home_env()]
               pass
           pass
        else:
            os.environ[data_util.config.Configuration.get_home_env()] = value
            pass
        pass
               

    def get_home_path(self):
        return pathlib.Path(os.getcwd()) / "home_path"

    def test_custom_path(self):
        current_env_value = self.get_home_env()
        test_home_path = self.get_home_path()

        os.environ[data_util.config.Configuration.get_home_env()] = str(test_home_path)
        config = data_util.get_config(reload=True)
        home = config.get_home_path()
        data_path = config.get_data_path()
        config_file_path = config.get_config_file_path()
        db_path = config.get_db_path()
        cache_path = config.get_cache_path()
        
        self.assertEqual(home, test_home_path)
        self.assertEqual(data_path, home / config.get_data_dir())
        self.assertEqual(config_file_path, home / config.get_config_file_name())
        self.assertEqual(db_path, data_path / config.get_db_name())
        self.assertEqual(cache_path, data_path / config.get_cache_dir())

        self.assertFalse(config.is_setup())
        config.setup()
        self.assertTrue(config.is_setup())

        self.assertTrue(home.exists())
        self.assertTrue(data_path.exists())
        self.assertTrue(config_file_path.exists())
        self.assertTrue(cache_path.exists())

        self.reset_home_env(current_env_value)
        pass

if __name__ == '__main__':
    unittest.main()