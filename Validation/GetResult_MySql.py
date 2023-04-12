import pymysql
import Configuration as config
from DB_Config import Mysql


class ConnectMySqlSrc:
    def __init__(self):
        self.srcSchema = config.srcSchema
        self.srcTableNm = config.srcTableNm
        self.srcColumn = config.srcColumn
        self.dateFlag = config.dateFlag

    def SelectSql(self):
        dateSql_Ym = f'''
            select date_format({self.srcColumn}, '%Y-%m'), count(*)
            from {self.srcSchema}.{self.srcTableNm}
            group by date_format({self.srcColumn}, '%Y-%m')
            order by 1
        '''
        dateSql_Y = f'''
            select date_format({self.srcColumn}, '%Y'), count(*)
            from {self.srcSchema}.{self.srcTableNm}
            group by date_format({self.srcColumn}, '%Y')
            order by 1
        '''
        notDateSql = f'''
            select {self.srcColumn}, count(*)
            from {self.srcSchema}.{self.srcTableNm}
            group by {self.srcColumn}
            order by 1
        '''

        if self.dateFlag == "YM":
            return dateSql_Ym
        elif self.dateFlag == "Y":
            return dateSql_Y
        else:
            return notDateSql

    def ConnectSrc(self, schema):
        if schema != Mysql['database']:
            raise ValueError("Could not find database with given name")
        conn = pymysql.connect(host=Mysql['host'],
                               port=Mysql['port'],
                               database=Mysql['database'],
                               user=Mysql['user'],
                               password=Mysql['password'],
                               charset=Mysql['charset'])
        return conn

    def fetchRows(self):
        conn = self.ConnectSrc(self.srcSchema)
        query = self.SelectSql()
        curs = conn.cursor()
        curs.execute(query)
        rows = curs.fetchall()
        curs.close()
        conn.close()
        return rows


class ConnectMySqlTgt:
    def __init__(self):
        self.tgtSchema = config.tgtSchema
        self.tgtTableNm = config.tgtTableNm
        self.tgtColumn = config.tgtColumn
        self.dateFlag = config.dateFlag

    def SelectSql(self):
        dateSql_Ym = f'''
            select date_format({self.tgtColumn}, '%Y-%m'), count(*)
            from {self.tgtSchema}.{self.tgtTableNm}
            group by date_format({self.tgtColumn}, '%Y-%m')
            order by 1
        '''
        dateSql_Y = f'''
            select date_format({self.tgtColumn}, '%Y'), count(*)
            from {self.tgtSchema}.{self.tgtTableNm}
            group by date_format({self.tgtColumn}, '%Y')
            order by 1
        '''
        notDateSql = f'''
            select {self.tgtColumn}, count(*)
            from {self.tgtSchema}.{self.tgtTableNm}
            group by {self.tgtColumn}
            order by 1
        '''

        if self.dateFlag == "YM":
            return dateSql_Ym
        elif self.dateFlag == "Y":
            return dateSql_Y
        else:
            return notDateSql

    def ConnectSrc(self, schema):
        if schema != Mysql['database']:
            raise ValueError('Could not find database with given name')
        conn = pymysql.connect(host=Mysql['host'],
                               port=Mysql['port'],
                               database=Mysql['database'],
                               user=Mysql['user'],
                               password=Mysql['password'],
                               charset=Mysql['charset'])
        return conn

    def fetchRows(self):
        conn = self.ConnectSrc(self.tgtSchema)
        query = self.SelectSql()
        curs = conn.cursor()
        curs.execute(query)
        rows = curs.fetchall()
        curs.close()
        conn.close()
        return rows