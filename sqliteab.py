import logging
import sqlite3
import string


class SQLiteAb:
    """
    A quick & dirty abstraction layer written for SQLite databases.

    Do not use this for actual work
    """

    def __init__(self, database) -> None:
        self._connection = sqlite3.connect(database)
        self._cursor = self._connection.cursor()
        self._table = None

    def __xi(self, text) -> string:
        return f'{text}' if f'{text}'.isdigit() else f"'{text}'"

    def enable_logging(self):
        """
        Enable console logging
        """
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(process)d-%(levelname)s-%(message)s'
        )

    def set_table(self, table) -> None:
        """
        Sets the current table

        Args:
            table (string): _description_
        """
        logging.debug(f'Setting table to {table}')
        self._table = table

    def get_all_data(self) -> tuple:
        """
        Gets all data as a tuple

        Returns:
            tuple: _description_
        """
        query = f'''
            SELECT * FROM {self._table}
        '''

        logging.debug(query)

        res = self._cursor.execute(query)

        return res

    def get_data(self, column_name, column_val) -> tuple:
        """
        Get a tuple of rows matching a given condition

        Args:
            column_name (_type_): _description_
            column_val (_type_): _description_

        Returns:
            _type_: _description_
        """
        query = f'''
            SELECT * FROM {self._table}
            WHERE {column_name} = {self.__xi(column_val)}
        '''

        logging.debug(query)

        res = self._cursor.execute(query)

        return res

    def table_exists(self) -> bool:
        """
        Checks if a table exists

        Returns:
            Boolean: _description_
        """
        query = f'''
            SELECT 1 FROM {self._table}
        '''

        logging.debug(query)

        res = self._cursor.execute(query)

        return True if res else False

    def create_table(self, mp) -> None:
        """
        Create a table

        Args:
            mp (column-name -> column-type): _description_
        """

        query = f'''
            CREATE TABLE {self._table} ({', '.join([
                f"{i} {mp[i]}" for i in mp
            ])})
        '''

        logging.debug(query)

        self._cursor.execute(query)
        self._connection.commit()

    def insert_row(self, mp) -> None:
        """
        Inserts a row into a table

        Args:
            mp (map column-name -> data): _description_
        """
        map_keys = mp.keys()

        query = f'''
            INSERT INTO {self._table}
            ({', '.join(map_keys)})
            VALUES
            ({', '.join(
                [self.__xi(mp[i]) for i in map_keys]
            )})
        '''

        logging.debug(query)

        self._cursor.execute(query)
        self._connection.commit()

    def truncate_table(self) -> None:
        """
        Truncate the table
        """
        query = f'DELETE FROM {self._table}'

        logging.debug(query)

        self._cursor.execute(query)
        self._connection.commit()

    def delete_row(self, column_name, column_val) -> None:
        """
        Delete rows based on condition

        Args:
            column_name (string): _description_
            column_val (string or integer): _description_
        """
        query = f'''
            DELETE FROM {self._table}
            WHERE {column_name} = {self.__xi(column_val)}
        '''

        logging.debug(query)

        self._cursor.execute(query)
        self._connection.commit()

    def modify_row(self, column_name, column_val, mp) -> None:
        """
        Updates a single row

        Args:
            column_name (string): _description_
            column_val (string): _description_
            mp (map column-name -> new data): _description_
        """
        updates = ''
        for k in mp:
            updates += f'{k}={self.__xi(mp[k])},'

        query = f'''
            UPDATE {self._table} 
            SET {updates[:-1]}
            WHERE {column_name} = {self.__xi(column_val)}
        '''

        logging.debug(query)

        self._cursor.execute(query)
        self._connection.commit()

    def drop_table(self) -> None:
        """
        Drop a table
        """
        query = f'DROP TABLE {self._table}'

        logging.debug(query)

        self._cursor.execute(query)
        self._connection.commit()

    def close_connection(self) -> None:
        """
        Close SQLite connection
        """
        logging.debug('Closing database connection')
        self._connection.close()
