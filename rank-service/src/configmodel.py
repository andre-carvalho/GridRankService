#!/usr/bin/python3
from configparser import ConfigParser
from app_exceptions import ConfigReadError
import os

class ConfigModel:

    #constructor
    def __init__(self, filename='model.cfg', section='deter'):
        self.filename = filename
        self.section = section

    def get(self):
        
        config_file = (os.path.dirname(__file__) or '.') + '/config/' + self.filename

        # Test if model.cfg exists
        if not os.path.exists(config_file):
            # get model params from env vars
            raise ConfigReadError('Model configuration','Model configuration file, {0}, was not found.'.format(self.filename))

        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(config_file)
    
        # get section, default
        cfg = {}
        if parser.has_section(self.section):
            params = parser.items(self.section)
            for param in params:
                cfg[param[0]] = param[1]
        else:
            raise ConfigReadError('Model configuration', 'Section {0} not found in the {1} file'.format(self.section, self.filename))
    
        return cfg