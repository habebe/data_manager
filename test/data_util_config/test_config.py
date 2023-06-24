from .. import common_util
import unittest
import data_util
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
        pass

if __name__ == '__main__':
    unittest.main()