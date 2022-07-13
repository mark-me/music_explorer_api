import os
import shutil
import time
import sqlite3
import pandas as pd

class ManageDB:
    def __init__(self, db_file: str) -> None:
        self.__db_path = db_file
        self.__db_dir = os.path.dirname(db_file)
        filename, self.__db_ext = os.path.splitext(db_file)
        self.__db_file = os.path.basename(filename)
        self.__db_load = filename + '_load' +  self.__db_ext
        self.exists = os.path.exists(self.__db_path)

    def create_load_copy(self) -> str:
        """Create a copy of the database for loading and return it's location"""
        if self.exists:
            shutil.copyfile(self.__db_path, self.__db_load)
        return self.__db_load

    def replace_db(self) -> None:
        """Replace the database with the copy used for loading"""
        shutil.copyfile(self.__db_load, self.__db_path)
        os.remove(self.__db_load)

    def create_backup(self) -> None:
        """Create a backup of the database"""
        if self.exists:
            dir_backup = self.__db_dir + '/backup'
            if not os.path.exists(dir_backup):
                os.makedirs(dir_backup)
            file_backup = dir_backup + '/' + self.__db_file + '_' + time.strftime("%Y%m%d_%H%M%S") + '.db'
            shutil.copyfile(self.__db_path, file_backup)


class DBStorage():
    def __init__(self, db_file) -> None:
        self.db_file = db_file

    def create_view(self, name_view:str, sql_definition: str) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql = "CREATE VIEW " + name_view + " AS " + sql_definition +";"
        cursor.execute(sql)
        db_con.commit()
        db_con.close()

    def drop_view(self, name_view: str) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("DROP VIEW IF EXISTS " + name_view)
        db_con.commit()
        db_con.close()

    def execute_sql(self, sql: str) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute(sql)
        cursor.close()

    def write_data(self, df: pd.DataFrame, name_table: str) -> None:
        """Write data to the database"""
        if not df.empty:
            self.create_table(name_table=name_table)
            self.store_append(df=df, name_table=name_table)

    def create_table(self, name_table: str) -> None:
        """Virtual function for creating tables"""
        pass

    def table_exists(self, name_table: str) -> bool:
        """Checks whether a table exists"""
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + name_table + "'")
        does_exist = cursor.fetchone()[0]==1
        db_con.close()
        return does_exist

    def view_exists(self, name_view: str) -> bool:
        """Checks whether a view exists"""
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='view' AND name='" + name_view + "'")
        does_exist = cursor.fetchone()[0]==1
        db_con.close()
        return does_exist

    def drop_existing_table(self, name_table: str) -> None:
        """Dropping a table"""
        if self.table_exists(name_table):
            db_con = sqlite3.connect(self.db_file)
            cursor = db_con.cursor()
            cursor.execute("DROP TABLE " + name_table)
            db_con.commit()
            db_con.close()

    def store_replace(self, df: pd.DataFrame, name_table: str) -> None:
        """Storing data to a table"""
        db_con = sqlite3.connect(self.db_file)
        df.to_sql(name=name_table, con=db_con, if_exists='replace', index=False)
        db_con.close()

    def store_append(self, df: pd.DataFrame, name_table: str) -> None:
        db_con = sqlite3.connect(self.db_file)
        df.to_sql(name=name_table, con=db_con, if_exists='append', index=False)
        db_con.close()

    def read_table(self, name_table: str) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        sql = "SELECT * FROM " + name_table
        df = pd.read_sql_query(sql, con=db_con)
        db_con.close()
        return df

    def read_sql(self, sql: str) -> pd.DataFrame():
        db_con = sqlite3.connect(self.db_file)
        df = pd.read_sql_query(sql, con=db_con)
        db_con.close()
        return df
