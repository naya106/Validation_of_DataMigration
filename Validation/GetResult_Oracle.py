import oracledb
from DB_Config import Oracle
import Configuration as config


class ConnectOracleSrc:
    def __init__(self):
        self.srcSchema = config.srcSchema
        self.srcTableNm = config.srcTableNm
        self.srcColumn = config.srcColumn
        self.dateFlag = config.dateFlag

    def SelectSql(self):
        dateSql_Ym = f'''
            SELECT TO_CHAR({self.srcColumn}, 'YYYY-MM'), COUNT(*)
            FROM {self.srcSchema}.{self.srcTableNm}
            GROUP BY TO_CHAR({self.srcColumn}, 'YYYY-MM')
            ORDER BY 1
        '''
        dateSql_Y = f'''
            SELECT TO_CHAR({self.srcColumn}, 'YYYY'), COUNT(*)
            FROM {self.srcSchema}.{self.srcTableNm}
            GROUP BY({self.srcColumn}, 'YYYY')
            ORDER BY 1
        '''
        notDateSql = f'''
            SELECT {self.srcColumn}, COUNT(*)
            FROM {self.srcSchema}.{self.srcTableNm}
            GROUP BY {self.srcColumn}
            ORDER BY 1
        '''

        if self.dateFlag == "YM":
            return dateSql_Ym
        elif self.dateFlag == "Y":
            return dateSql_Y
        else:
            return notDateSql

    def ConnectSrc(self, schema):
        if schema != Oracle['database']:
            raise ValueError("Could not find database with given name")
        params = oracledb.ConnectParams(host=Oracle['host'],
                                        port=Oracle['port'],
                                        service_name=Oracle['service_name'])
        # Oracle 12.X 이전 버전 시 Thick mode로 실행
        oracledb.init_oracle_client()
        conn = oracledb.connect(user=Oracle['user'],
                                password=Oracle['password'],
                                params=params)
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


class ConnectOracleTgt:
    def __init__(self):
        self.tgtSchema = config.tgtSchema
        self.tgtTableNm = config.tgtTableNm
        self.tgtColumn = config.tgtColumn
        self.dateFlag = config.dateFlag

    def SelectSql(self):
        dateSql_Ym = f'''
            SELECT TO_CHAR({self.tgtColumn}, 'YYYY-MM'), COUNT(*)
            FROM {self.tgtSchema}.{self.tgtTableNm}
            GROUP BY TO_CHAR({self.tgtColumn}, 'YYYY-MM')
            ORDER BY 1
        '''
        dateSql_Y = f'''
            SELECT TO_CHAR({self.tgtColumn}, 'YYYY'), COUNT(*)
            FROM {self.tgtSchema}.{self.tgtTableNm}
            GROUP BY({self.tgtColumn}, 'YYYY')
            ORDER BY 1
        '''
        notDateSql = f'''
            SELECT {self.tgtColumn}, COUNT(*)
            FROM {self.tgtSchema}.{self.tgtTableNm}
            GROUP BY {self.tgtColumn}
            ORDER BY 1
        '''

        if self.dateFlag == "YM":
            return dateSql_Ym
        elif self.dateFlag == "Y":
            return dateSql_Y
        else:
            return notDateSql

    def ConnectTgt(self, schema):
        if schema != Oracle['database']:
            raise ValueError("Could not find database with given name")
        params = oracledb.ConnectParams(host=Oracle['host'],
                                        port=Oracle['port'],
                                        service_name=Oracle['service_name'])
        oracledb.init_oracle_client()
        conn = oracledb.connect(user=Oracle['user'],
                                password=Oracle['password'],
                                params=params)
        return conn

    def fetchRows(self):
        conn = self.ConnectTgt(self.tgtSchema)
        query = self.SelectSql()
        curs = conn.cursor()
        curs.execute(query)
        rows = curs.fetchall()
        curs.close()
        conn.close()
        return rows