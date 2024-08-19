import os
import shutil
import sqlite3
import time

import pandas as pd

class Json2DB:
    def __init__(self, db_file: str) -> None:
        self.db_file = db_file
        self.__db_dir = os.path.dirname(db_file)
        filename, self.__db_ext = os.path.splitext(db_file)
        self.__db_file = os.path.basename(filename)
        self.__db_load = filename + '_load' +  self.__db_ext
        self.exists = os.path.exists(self.db_file)

    def create_load_copy(self) -> str:
        """Create a copy of the database for loading and return it's location"""
        if self.exists:
            shutil.copyfile(self.db_file, self.__db_load)
        return self.__db_load

    def replace_db(self) -> None:
        """Replace the database with the copy used for loading"""
        shutil.copyfile(self.__db_load, self.db_file)
        os.remove(self.__db_load)

    def create_backup(self) -> None:
        """Create a backup of the database"""
        if self.exists:
            dir_backup = self.__db_dir + '/backup'
            if not os.path.exists(dir_backup):
                os.makedirs(dir_backup)
            file_backup = dir_backup + '/' + self.__db_file + '_' + time.strftime("%Y%m%d_%H%M%S") + '.db'
            shutil.copyfile(self.db_file, file_backup)