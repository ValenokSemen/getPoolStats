import mysql.connector as mariadb
import mysql.connector as errorcode

from configfile import config


class mySqlPython(object):

    mariadb_connection = None

    """docstring for mySqlPython"""

    def __init__(self):
        super(mySqlPython, self).__init__()
        __dbconfig = config["mysql"]
        self._connection_to_db(__dbconfig)
        # self.cursor = self.connection.cursor()

    def _connection_to_db(self, conf):
        try:
            self.mariadb_connection = mariadb.connect(host=conf["host"], port=conf["port"], user=conf[
                "user"], password=conf["password"], database=conf["db"])
        except mariadb.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        pass

    def _get_cursor(self):
        return self.mariadb_connection.cursor()

    def get_rows(self, query):
        """
        Fetchs all rows
        """
        __cursor = self._get_cursor()
        try:
            __cursor.execute(query)
            row = __cursor.fetchall()
            if row:
                return row
            return None
        except (mariadb.Error, mariadb.Warning) as e:
            print(e)
            return None
        finally:
            self.close()

    def insert_row(self, table, *args, **kwargs):
        __cursor = self._get_cursor()
        try:
            values = None
            query = "INSERT INTO %s " % table

            if kwargs:
                keys = kwargs.keys()
                values = tuple(kwargs.values())
                query += "(" + ",".join(["`%s`"] * len(keys)) % tuple(keys) + \
                    ") VALUES (" + ",".join(["'%s'"]
                                            * len(values)) % values + ")"

            __cursor.execute(query)
            self.mariadb_connection.commit()
            return __cursor.lastrowid
        except mariadb.SQLError as sql:
            print("Error in SQL submitted:"), sql
            print("SQL:"), query
            __cursor.execute('ROLLBACK')
        except mariadb.Error as error:
            print("mariadb Error:"), error
            __cursor.execute('ROLLBACK')
        except Exception as e:
            print("Error submitting insert:", e)
            __cursor.execute('ROLLBACK')
        finally:
            __cursor.close()

    def insert_update_row(self, table, **kwargs):
        __cursor = self._get_cursor()
        try:
            values = None
            query = "INSERT INTO %s" % table
            if kwargs:
                keys = kwargs.keys()
                values = tuple(kwargs.values())

                on_duplicates = []
                for field in keys:
                    on_duplicates.append(field + "=VALUES(" + field + ")")

                query += "(" + ",".join(["`%s`"] * len(keys)) % tuple(keys) + \
                    ") VALUES (" + ",".join(["'%s'"]
                                            * len(values)) % values + ")"
                query += " ON DUPLICATE KEY UPDATE"
                query += " %s" % (", ".join(on_duplicates))
                __cursor.execute(query)
                self.mariadb_connection.commit()
                return __cursor.lastrowid
        except mariadb.SQLError as sql:
            print("Error in SQL submitted:"), sql
            print("SQL:"), query
            __cursor.execute('ROLLBACK')
        except mariadb.Error as error:
            print("mariadb Error:"), error
            __cursor.execute('ROLLBACK')
        except Exception as e:
            print("Error submitting insert:", e)
            __cursor.execute('ROLLBACK')
        finally:
            __cursor.close()

    def insert_ignore_row(self, table, *args, **kwargs):
        __cursor = self._get_cursor()
        try:
            values = None
            query = "INSERT IGNORE INTO %s " % table

            if kwargs:
                keys = kwargs.keys()
                values = tuple(kwargs.values())
                query += "(" + ",".join(["`%s`"] * len(keys)) % tuple(keys) + \
                    ") VALUES (" + ",".join(["'%s'"]
                                            * len(values)) % values + ")"

            __cursor.execute(query)
            self.mariadb_connection.commit()
            return __cursor.lastrowid
        except mariadb.SQLError as sql:
            print("Error in SQL submitted:"), sql
            print("SQL:"), query
            __cursor.execute('ROLLBACK')
        except mariadb.Error as error:
            print("mariadb Error:"), error
            __cursor.execute('ROLLBACK')
        except Exception as e:
            print("Error submitting insert:", e)
            __cursor.execute('ROLLBACK')
        finally:
            __cursor.close()

    def select(self, table, where=None, *args, **kwargs):
        __cursor = self._get_cursor()

        # SELECT id from worker where worker_name = 'zdb'
        result = None
        query = 'SELECT '
        keys = args
        values = tuple(kwargs.values())
        l = len(keys) - 1

        for i, key in enumerate(keys):
            query += "`"+key+"`"
            if i < l:
                query += ","
        ## End for keys

        query += ' FROM %s' % table

        if where:
            query += " WHERE %s" % where
        # End if where
        __cursor.execute(query, values)
        number_rows = __cursor.rowcount
        number_columns = len(__cursor.description)

        if number_rows >= 1 and number_columns > 1:
            result = [item for item in __cursor.fetchall()]
        else:
            result = [item[0] for item in __cursor.fetchall()]
        __cursor.close()
        return result
    # End def select


    def close_connection(self):
        self.mariadb_connection.close()
