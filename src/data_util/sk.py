import enum 
from inspect import Signature, signature, Parameter

import numpy as np

import pandas as pd


from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline, Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.model_selection import train_test_split



def patch():
    """patch some classes which did not handle get_feature_names_out()
       correctly in Scikit-Learn 1.0.*."""


    default_get_feature_names_out = StandardScaler.get_feature_names_out

    if not hasattr(SimpleImputer, "get_feature_names_out"):
      SimpleImputer.get_feature_names_out = default_get_feature_names_out

    if not hasattr(FunctionTransformer, "get_feature_names_out"):
        orig_init = FunctionTransformer.__init__
        orig_sig = signature(orig_init)

        def __init__(*args, feature_names_out=None, **kwargs):
            orig_sig.bind(*args, **kwargs)
            orig_init(*args, **kwargs)
            args[0].feature_names_out = feature_names_out

        __init__.__signature__ = Signature(
            list(signature(orig_init).parameters.values()) + [
                Parameter("feature_names_out", Parameter.KEYWORD_ONLY)])

        def get_feature_names_out(self, names=None):
            if callable(self.feature_names_out):
                return self.feature_names_out(self, names)
            assert self.feature_names_out == "one-to-one"
            return default_get_feature_names_out(self, names)

        FunctionTransformer.__init__ = __init__
        FunctionTransformer.get_feature_names_out = get_feature_names_out
    pass


class DataKey(enum.IntFlag):
    All = 1
    Train = 2
    Test = 4
    Label = 8
    Numeric = 16
    Category = 32
    pass

class ModelState(enum.IntEnum):
    Initial = 0
    DataSplit = 1
    pass

class Model:
    PARAM_TEST_SIZE = "test_size"
    PARAM_TRAIN_SIZE = "train_size"
    PARAM_RANDOM_STATE = "random_state"
    PARAM_STRATIFY = "stratify"
    PARAM_STRATIFY_BIN = "stratify_bin"
    PARAM_LABEL = "label"

    STRATIFY_VARIABLE_NAME = "_stratify_variable_"
    
    def __init__(self,data,**kwargs):
        self.__state__ = ModelState.Initial

        self.__parameters__ = {
            Model.PARAM_TEST_SIZE : 0.2,
            Model.PARAM_TRAIN_SIZE : 0.8,
            Model.PARAM_RANDOM_STATE : None,
            Model.PARAM_STRATIFY : None,
            Model.PARAM_STRATIFY_BIN : None,
            Model.PARAM_LABEL : None
        }
        self.__data__ = {
            DataKey.All : data
        }
        self.set(**kwargs)
        pass
    
    def __repr__(self):
        return str(self.__parameters__)
    
    def data(self,data_type=DataKey.All):
        if data_type in self.__data__:
            return self.__data__[data_type]
        return None
    
    def data_keys(self):
        return self.__data__.keys()
    
    def state(self):
        return self.__state__
    
    def size(self):
        all_data = self.data(DataKey.All)
        train_data = self.data(DataKey.Train)
        test_data = self.data(DataKey.Test)
        return [
            0 if type(all_data) == type(None) else all_data.size , 
            0 if type(train_data) == type(None) else train_data.size , 
            0 if type(test_data) == type(None) else test_data.size 
        ]
    
    def set(self,**kwargs):
        for key, value in kwargs.items():
            if key in self.__parameters__:
                print(f"{key} {value} , {type(value)}")
                if key == Model.PARAM_TEST_SIZE:
                    v = min(max(0.0,value),1.0)
                    self.__parameters__[key] = v
                    self.__parameters__[Model.PARAM_TRAIN_SIZE] = 1.0 - v
                    pass
                elif key == Model.PARAM_TRAIN_SIZE:
                    v = min(max(0.0,value),1.0)
                    self.__parameters__[key] = v
                    self.__parameters__[Model.PARAM_TEST_SIZE] = 1.0 - v
                    pass
                else:
                    self.__parameters__[key] = value
                pass
            else:
                raise Exception(f'{key} is not a valid model parameter. parameters are {self.__parameters__.keys()}')
            pass
        return self
    
    def get(self,name):
        return self.__parameters__[name]
    
    def has_column(self,column_name):
        return column_name in self.data().columns
    
    def split(self,**kwargs):
        self.set(**kwargs)
        self.__state__ = ModelState.DataSplit
        stratify_variable_name = self.get(Model.PARAM_STRATIFY)
        if not self.has_column(stratify_variable_name):
            raise Exception(f'invalid {Model.PARAM_STRATIFY} specified.',self.data().columns)
            pass
        if stratify_variable_name != None:
            stratify_bin = self.get(Model.PARAM_STRATIFY_BIN)
            data = self.data(DataKey.All)
            if stratify_bin == None:
                stratify_data = data[stratify_variable_name]
                min_value = int(stratify_data.min())
                max_value = int(stratify_data.max())
                steps = int((max_value - min_value) / 10)
                stratify_bin = list(range(min_value,max_value,steps)) + [np.inf]
            data[Model.STRATIFY_VARIABLE_NAME] = pd.cut(data[stratify_variable_name],bins=stratify_bin)
            train_data,test_data = train_test_split(
                data,
                test_size=self.get(Model.PARAM_TEST_SIZE),
                random_state=self.get(Model.PARAM_RANDOM_STATE),
                stratify=data[[Model.STRATIFY_VARIABLE_NAME]]
            )
            data.drop(Model.STRATIFY_VARIABLE_NAME,axis=1,inplace=True)
            train_data.drop(Model.STRATIFY_VARIABLE_NAME,axis=1,inplace=True)
            test_data.drop(Model.STRATIFY_VARIABLE_NAME,axis=1,inplace=True)
            self.__data__[DataKey.Train] = train_data
            self.__data__[DataKey.Test] = test_data
            
            train_data.info()
            test_data.info()
            pass
        else:
            self.__train_data__,self.__test_data__ = train_test_split(
                self.__data__,
                test_size=self.get(Model.PARAM_TEST_SIZE),
                random_state=self.get(Model.PARAM_RANDOM_STATE)
            )
            pass
        pass
    
    def all_columns(self):
        return list(self.__data__[DataKey.Train].columns)
    
    def numeric_columns(self):
        return list(self.__data__[DataKey.Train|DataKey.Numeric].columns)
    
    def category_columns(self):
        return list(set(self.all_columns()) - set(self.numeric_columns()))
    
    def prepare(self,**kwargs):
        self.set(**kwargs)
        label_name = self.get(Model.PARAM_LABEL)
        if label_name == None:
            raise Exception(f'label parameter needs to be initialized.')
        if not self.has_column(label_name):
            raise Exception(f'invalid label parameter specified.',self.data().columns)
        
        if self.state() < ModelState.DataSplit:
            self.split()
            pass
        
        self.__data__[DataKey.Label] = self.__data__[DataKey.Train][label_name].copy()
        self.__data__[DataKey.Train] = self.__data__[DataKey.Train].drop(label_name,axis=1)        
        self.__data__[DataKey.Train|DataKey.Numeric] = self.__data__[DataKey.Train].select_dtypes(include=[np.number])        
        category_columns = self.category_columns()
        if category_columns and len(category_columns):
            self.__data__[DataKey.Train|DataKey.Category] = self.__data__[DataKey.Train][category_columns]
            pass
        pass    
    pass
