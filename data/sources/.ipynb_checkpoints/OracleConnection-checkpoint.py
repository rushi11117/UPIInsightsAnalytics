import cx_Oracle as oracle
import configparser

class OracleConnection:
    
    def __init__(self, table_name):
        self.config = configparser.ConfigParser()
        self.config.read(r'D:\Data Analysis\upi-data-analytics\databases\oracle\oracle_config.ini')
        self.table_name = table_name
        self.username = self.config['Oracle']['username']
        self.password = self.config['Oracle']['password']
        self.server = self.config['Oracle']['server']
    
    def connectToOracle(self):
        try:
            connection = oracle.connect(self.username, self.password, self.server)
            return connection
            # if not rows:
            #     return "No data found"
            # else:
            #     return columns, rows
        
        except oracle.Error as error:
            return f"Connection Error: {error}"

    def fetchAllFromTable(self):
        try:
            connection = self.connectToOracle()
            cursor = connection.cursor()
            query = f"SELECT column_name FROM ALL_TAB_COLUMNS WHERE table_name = '{self.table_name}'"
            cursor.execute(query)
            columns = [row[0] for row in cursor.fetchall()]
            query = f"SELECT * FROM {self.table_name}"
            cursor.execute(query)
            return cursor, connection
        except oracle.Error as error:
            return f"Table {table_name}Is Empty"


# cursor, connection = OracleConnection('TRANSACTION_BUFFER').fetchAllFromTable()
# first_tuple = cursor.fetchall()[0]
# print(first_tuple)
# connection.close()