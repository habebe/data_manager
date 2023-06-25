from .. import common_util
import unittest
import data_util
import data_util.config
import data_util.db
import os
import pathlib

class TestDatabase(unittest.TestCase):
    def test_custom_path(self):
        current_env_value = common_util.get_home_env()
        test_home_path = common_util.get_test_home_path()

        os.environ[data_util.config.Configuration.get_home_env()] = str(test_home_path)
        config = data_util.get_config(reload=True)
        config.setup()
        
        db_path = config.get_db_path()
        self.assertEqual(db_path, config.get_data_path() / config.get_db_name())
        
        db = data_util.get_db(reload=True)
        
        self.assertTrue(db_path.exists())

        common_util.reset_home_env(current_env_value)
        pass

if __name__ == '__main__':
    unittest.main()