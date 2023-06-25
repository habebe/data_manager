
import argparse
import datetime
import logging
import time

from tabulate import tabulate
from . import config
from . import ds
from . import db

class Command:
    def __init__(self,options):
        self.options = options
        self.__setup_complete__ = False
        pass

    def is_setup_complete(self):
        return self.__setup_complete__

    def set_setup_complete(self,status):
        self.__setup_complete__ = status
        pass

    @staticmethod
    def setup_common_options(parser):
        parser.add_argument("-l", "--logging", action="store", type=str, dest="logging", default="info", choices=[i.name.lower() for i in config.LoggingLevel],help="Logging Level")
        pass

    def setup(self):
        if self.__setup_complete__ == False:
            logging.getLogger().setLevel(config.LoggingLevel[self.options.logging.upper()])
            logging.debug("Command.setup:%s",datetime.datetime.now())
            self.set_setup_complete(True)
        return self.__setup_complete__

    def run(self):
        if self.setup():
            return self.run_command()
        return -1
    pass

class DbDump(Command):
    def __init__(self,options):
        Command.__init__(self,options)
        pass

    @staticmethod
    def setup_subparser(subparsers):
        parser = subparsers.add_parser('db_dump', help='database dump')
        Command.setup_common_options(parser)
        parser.set_defaults(func=DbDump.create_command)
        pass

    @staticmethod
    def create_command(options):
        return DbDump(options)

    def setup(self):
        return Command.setup(self)

    def run_command(self):
        database = db.Database.instance()
        result = database.select()
        table = [db.Database.get_columns()]
        for i in result:
            table.append(i.to_list())
            pass
        print(tabulate(table, headers='firstrow', tablefmt='fancy_grid'))
        return 0
    pass


class LoadUrl(Command):
    def __init__(self,options):
        Command.__init__(self,options)
        pass

    @staticmethod
    def setup_subparser(subparsers):
        parser = subparsers.add_parser('load_url', help='loads data from given URL')
        parser.add_argument("url", help="Data url")
        Command.setup_common_options(parser)
        parser.set_defaults(func=LoadUrl.create_command)
        pass

    @staticmethod
    def create_command(options):
        return LoadUrl(options)

    def setup(self):
        return Command.setup(self)

    def run_command(self):
        dataset = ds.Loader.instance().load_url(self.options.url)
        table = []
        if dataset:
            columns = db.Database.get_columns()
            mapped_data = dataset.meta_data.to_map()
            for i in mapped_data:
                if i in columns:
                    table.append([i,mapped_data[i]])
                    pass
                pass
            pass
        print(tabulate(table, tablefmt='fancy_grid'))
        return 0
    pass

class CLI:
    __commands__ = [
        DbDump,
        LoadUrl
    ]

    def __init__(self,args):
        self.__setup_cli_options__(args)
        self.__setup_command__()
        pass

    def __setup_cli_options__(self,args):
        cli_parser = argparse.ArgumentParser("cli options")
        subparsers = cli_parser.add_subparsers(required=True)
        for command in CLI.__commands__:
            command.setup_subparser(subparsers)
            pass
        self.__options__ = cli_parser.parse_args(args)
        pass

    def __setup_command__(self):
        self.command = self.__options__.func(self.__options__)
        pass
    pass