"""
    df_mysql_uploader.py

    author: Terry Lee
    created: 28/1/2022

    The generic purpose class to be used for any pandas dataframe to be uploaded
    onto mysql server.
    In fact any data such as csv, excel can be easily converted to pandas df so 
    virtually any data can be uploaded to mysql without hassle. 

    The DfToMySqlUploader class doesn't hold table name and dataframe as
    instance variables.
    This is due to the possible scenarios with handling of multiple tables.
    The class will hold pymysql connector and cursor, so any functions
    which require access to mysql db using this connector are instance 
    functions.

    Usage example - 4 steps required

    (1) create class instance (create db connection and cursor)
        eg. sql_agent = DfToMySqlUploader(HOST, PORT, USER, PASSWD, DATABASE)

    (2) create table in mysql db
        eg. sql_agent.create_tbl(TABLE_NAME, df_sql)

    (3) insert all data in mysql db from dataframe
        eg. sql_agent.insert_all_data_from_df(TABLE_NAME, df_sql)

    (4) close db connection
        eg. sql_agent.close()

"""

import pymysql

DICT_DTYPE_CONVERSION = {
    'object': 'VARCHAR(255)', 
    'float64': 'FLOAT', 
    'int64': 'INT', 
    'datetime64': 'TIMESTAMP', 
    'datetime64[ns]': 'TIMESTAMP', 
    'timedelta64[ns]': 'VARCHAR(255)'
}

class DfToMySqlUploader():

    def __init__(self, host, port, user, passwd, db):
        self.conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset='utf8')
        self.cursor = self.conn.cursor()

    def drop_tbl(self, tbl_name):
        print("Dropping table: %s" %tbl_name)
        res = self.cursor.execute("DROP TABLE IF EXISTS %s;" %tbl_name)
        print(res)

    def create_tbl(self, tbl_name, df):
        self.cleanup_tbl_columns(df)   # will update df columns (inplace)
        col_str = ", ".join("%s %s" %(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(DICT_DTYPE_CONVERSION)))
        print(col_str)

        # print("Dropping table: %s" %tbl_name)
        # res = self.cursor.execute("DROP TABLE IF EXISTS %s;" %tbl_name)
        # print(res)
        self.drop_tbl(tbl_name)
        print("Creating table: %s" %tbl_name)
        res = self.cursor.execute("CREATE TABLE %s (%s);" %(tbl_name, col_str))

        self.conn.commit()

    def get_all_data_from_tbl(self, tbl_name):
        self.cursor.execute("SELECT * FROM %s" %tbl_name)
        res = self.cursor.fetchall()
        for r in res:
            print(r)

    def insert_all_data_from_df(self, tbl_name, df):
        insert_query = self.generate_insert_query(tbl_name, df)
        print("Performing commit..")
        self.cursor.execute(insert_query)
        self.conn.commit()
        print("Data has been successfully uploaded to mysql")

    def close(self):
        self.cursor.close()

    @classmethod
    def cleanup_tbl_columns(cls, df):
        df.reset_index(drop = True, inplace = True)  # required otherwise generating insert query will fail
        cls.change_category_type_column_to_str(df)
        # print(df.columns)
        df.columns = [x.lower().replace(" ", "_").replace(r"(", "_").replace(")", "").replace("/", "").replace(".", "") \
                    .replace(":", "").replace("__", "_").replace("\\", "").replace("$", "") for x in df.columns]
        df.rename(columns={'index':'req_index'}, inplace=True) # in case index is added (eg. reset_index(drop = False))
        # print(df.columns)

    @classmethod
    def change_category_type_column_to_str(cls, df):
        col_names = []
        for col in df.columns:
            try:
                if df[col].dtype == "category":
                    col_names.append(col)
            except:
                pass

        for col in col_names:
            df[col] = df[col].astype("str")

    @classmethod
    def change_all_columns_data_to_str(cls, df):
        for col in df.columns:
            df[col] = df[col].astype("str")

    @classmethod
    def generate_insert_query(cls, tbl_name, df):
        # Pre-condition: The df should be sorted with index ascending
        # reset_index is performed before table creation

        # Upon showing this error: 
        # OperationalError: (2013, 'Lost connection to MySQL server during query ([WinError 10053] 
        # An established connection was aborted by the software in your host machine)')

        # check - 
        # SHOW variables LIKE 'max_allowed_packet';
        # and do - 
        # SET GLOBAL max_allowed_packet=4194304 *4;
        # if still 4M, close and retry mysql workbench
        cls.change_all_columns_data_to_str(df)

        for j in range(df.shape[1]):
            if not df[df.columns.values[j]].dtype in ('float64', 'int64'):
                df[df.columns.values[j]].str.replace("'", "").replace("\n", "").replace("\r", "")

        print("Generating insert query .. ")
        insert_query = "INSERT INTO %s VALUES " %tbl_name
        for i in range(df.shape[0]):
            insert_query += "("
            
            for j in range(df.shape[1]):
                try:
                    cur_str = str(df[df.columns.values[j]][i]).replace("'", "")
                except:  # will fail if the index is not reset
                    print("ERROR on {}:{}".format(i, j))

                # if the value is nan then should not be treated as string
                if cur_str in ('nan', 'NaT') or len(cur_str) == 0 or df[df.columns.values[j]].dtype in ('float64', 'int64'):
                    insert_query += cur_str + ", "
                else:
                    if len(cur_str) > 255:
                        cur_str = cur_str[:255]
                    insert_query += ("'" + cur_str + "', ")

            insert_query = insert_query[:-2] + "), "
        insert_query = insert_query[:-2] + ";"

        # insert_query = insert_query.replace("\'nan\'", "null").replace("nan", "null")  # works
        insert_query = insert_query.replace("nan", "null").replace("NaT", "null")

        print("Generating query completed")
        return insert_query