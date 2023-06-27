# data_util
Various Data, Datasets and Caches uitilities

From Root Directory

## To build:
 python -m build

## To run tests:
 python -m unittest discover -v 

## To install 
 pip install .

## CLI usage

### Data loading example

##### Getting help
data_util --help

#### Dump meta data from cached data
data_util db_dump 

#### Data loading example
data_util load https://raw.githubusercontent.com/patrickloeber/pytorchTutorial/master/data/wine/wine.csv
