from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode

class Currency_database():

    def __init__(self,db_user,db_password,db_name,db_host='localhost',\
                    port=3306,use_unicode=True,charset="utf8"):

        self.db_host = db_host
        self.db_user = db_user
        self.db_password= db_password
        self.db_name = db_name
        self.port = port
        self.use_unicode = use_unicode
        self.charset = charset
        self.database_config = {
            'host':db_host, 
            'user':db_user, 
            'password':db_password,
            'port':port,
            'use_unicode':use_unicode,
            'charset':charset,}

        self.Mysql_instance = self.connect_to_Mysql_and_return_the_instance()
        print ("Connected to the Database successfully.")
        self.db_cursor = self.Mysql_instance.cursor(buffered=True)
        print ("Cursor Created.","Starting to connect the Database ...",sep="\n")
        self.connect_to_database()  # cursor will be set to the database
        print ("Successful connected to database: %s ; %s @ %s"%(self.db_name,self.db_user,self.db_host))
        print ("Initialisation Complete.")

    def __del__ (self):
        self.db_cursor.close()
        self.Mysql_instance.close()
        print ("Destructor invoked: Mysql_instance and db_cursor closed")

    def connect_to_Mysql_and_return_the_instance(self):
        try:
            self.cnx = mysql.connector.connect(**self.database_config)
            
        except mysql.connector.Error as err:
            #handle connection errors
            print ("Error code:", err.errno)        # error number
            print ("SQLSTATE value:", err.sqlstate) # SQLSTATE value
            print ("Error message:", err.msg)       # error message
            print ("Error:", err)                   # errno, sqlstate, msg values
            raise Exception(str(err))  
        else:
            return self.cnx


    def connect_to_database(self):
        try:
            self.Mysql_instance.database = self.db_name #Try to connect to the specified database
        except mysql.connector.Error as err:   #Exception Handling
            if err.errno == errorcode.ER_BAD_DB_ERROR: #If the database  does not exist
                try:
                    print ("Database does not exist. The system will create a new database ...")
                    self.create_new_database()
                    self.Mysql_instance.database = self.db_name
                    self.create_all_tables()
                except:
                    raise
            else:
                raise
        else:
            self.create_all_tables()
        
            
        

    def create_new_database(self):
        try:
            #Create a database
            self.db_cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.db_name))
        except mysql.connector.Error as err: #Exception Handling
            raise Exception("Failed creating database: {}".format(err))
        else:
            print ("Database '{}' created successfully".format(self.db_name), end='\n')
            try:
                self.Mysql_instance.database = self.db_name
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_BAD_DB_ERROR:
                    self.create_new_database(self)
                    self.Mysql_instance.database = self.db_name
                else:
                    print(err)

    def create_all_tables(self):

        self.db_cursor.execute("SET foreign_key_checks=0")
        self.TABLES = {}
        self.TABLES['All_Currencies'] = (
            "CREATE TABLE `All_Currencies` ("
            "  `id` INT NOT NULL AUTO_INCREMENT,"
            "  `Ranking` SMALLINT NULL,"
            "  `Name` VARCHAR(45) NULL,"
            "  `Symbol` VARCHAR(45) NULL,"
            "  `Market_cap` BIGINT NULL,"
            "  `Price` DECIMAL(12,6) NULL,"
            "  `Circulating_supply` BIGINT NULL,"
            "  `Volume_24h` BIGINT NULL,"
            "  `Percent_1h` DECIMAL(10,2) NULL,"
            "  `Percent_24h` DECIMAL(10,2) NULL,"
            "  `Percent_7d` DECIMAL(10,2) NULL,"
            "  `Updated_time` DATETIME NULL,"
            "  `Collection_time` DATETIME NULL,"
            "  `Test_unique` VARCHAR(200) NULL,"
            "  PRIMARY KEY (`id`)"
            ") ENGINE=InnoDB")

        self.TABLES['24HVR_by_Exchange'] = (
            "CREATE TABLE `24HVR_by_Exchange` ("
            "  `id` INT NOT NULL AUTO_INCREMENT,"
            "  `Market` VARCHAR(45) NULL,"
            "  `Market_ranking` SMALLINT NULL,"
            "  `Ranking` SMALLINT NULL,"
            "  `Currency` VARCHAR(45) NULL,"
            "  `Pair` VARCHAR(20) NULL,"
            "  `Volume_24h` BIGINT NULL,"
            "  `Price` DECIMAL(12,6) NULL,"
            "  `Volume_percent` DECIMAL(5,2) NULL,"
            "  `Updated_time` DATETIME NULL,"
            "  `Collection_time` DATETIME NULL,"
            "  `Test_unique` VARCHAR(200) NULL,"
            "  PRIMARY KEY (`id`)"
            ") ENGINE=InnoDB")

        self.TABLES['24HVR_by_Currency'] = (
            "CREATE TABLE `24HVR_by_Currency` ("
            "  `id` INT NOT NULL AUTO_INCREMENT,"
            "  `Currency` VARCHAR(45) NULL,"
            "  `Currency_ranking` SMALLINT NULL,"
            "  `Ranking` SMALLINT NULL,"
            "  `Source` VARCHAR(45) NULL,"
            "  `Pair` VARCHAR(20) NULL,"
            "  `Volume_24h` BIGINT NULL,"
            "  `Price` DECIMAL(12,6) NULL,"
            "  `Volume_percent` DECIMAL(5,2) NULL,"
            "  `Updated_time` DATETIME NULL,"
            "  `Collection_time` DATETIME NULL,"
            "  `Test_unique` VARCHAR(200) NULL,"
            "  PRIMARY KEY (`id`)"
            ") ENGINE=InnoDB")
        
        self.create_each_table(self.TABLES)


    def create_each_table(self, DICT_tables):
        
        #self.db_cursor.execute("SET GLOBAL innodb_file_per_table=1")
        #self.db_cursor.execute("SET GLOBAL innodb_file_format=Barracuda")
        
        for table_key in DICT_tables:
            try:
                self.db_cursor.execute(self.TABLES[table_key])
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("Table {} already exists.".format(table_key))
                else:
                    print("Creating Table {} Failed: ".format(table_key), err.msg)
            else:
                print("Table {} Created.".format(table_key))

    def insert_data_into_All_Currencies(self, data):
        insertion_format = ("INSERT INTO All_Currencies (Ranking, Name, Symbol, Market_cap, Price, Circulating_supply, Volume_24h, Percent_1h, Percent_24h, Percent_7d, Updated_time, Collection_time, Test_unique)"
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        
        self.db_cursor.execute(insertion_format, data)
        self.Mysql_instance.commit()


    def insert_data_into_24HVR_by_Exchange(self, data):
        insertion_format = ("INSERT INTO 24HVR_by_Exchange (Market, Market_ranking, Ranking, Currency, Pair, Volume_24h, Price, Volume_percent, Updated_time, Collection_time, Test_unique)"
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        
        self.db_cursor.execute(insertion_format, data)
        self.Mysql_instance.commit()


    def insert_data_into_24HVR_by_Currency(self, data):
        insertion_format = ("INSERT INTO 24HVR_by_Currency (Currency, Currency_ranking, Ranking, Source, Pair, Volume_24h, Price, Volume_percent, Updated_time, Collection_time, Test_unique)"
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        self.db_cursor.execute(insertion_format, data)
        self.Mysql_instance.commit()

    def currency_exists_in_All_Currencies(self, data):
        select_format = ("SELECT COUNT(1) FROM All_Currencies WHERE Test_unique = %s")
        self.db_cursor.execute(select_format, (data,))
        return self.db_cursor.fetchone()[0]
    
    def currency_exists_in_24HVR_by_Exchange(self, data):
        select_format = ("SELECT COUNT(1) FROM 24HVR_by_Exchange WHERE Test_unique = %s")
        self.db_cursor.execute(select_format, (data,))
        return self.db_cursor.fetchone()[0]
    
    def currency_exists_in_24HVR_by_Currency(self, data):
        select_format = ("SELECT COUNT(1) FROM 24HVR_by_Currency WHERE Test_unique = %s")
        self.db_cursor.execute(select_format, (data,))
        return self.db_cursor.fetchone()[0]
