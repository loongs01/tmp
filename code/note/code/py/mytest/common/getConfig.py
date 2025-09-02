# coding: utf-8
import configparser
import os

class GetConfig:
    """定义一个读取配置文件的类"""

    def __init__(self, filepath=None):
        if filepath:
            configpath = filepath
        else:
          #  root_dir = os.path.dirname(os.path.abspath('.'))
            root_dir="/home/etl/scripts/" 
            configpath = os.path.join(root_dir, "config.ini")
        self.cf = configparser.ConfigParser()
        self.cf.read(configpath,encoding='utf-8')
    
    def get_db(self,options, param):
        value = self.cf.get(options, param)
        return value

    def getint_db(self,options,param):
        value = self.cf.getint(options,param)
        return value


