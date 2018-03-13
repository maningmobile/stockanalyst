import pymssql
import pandas as pd


class DBUtil(object):

    cnn = 0

    def create_cnn(self):
        self.cnn = pymssql.connect(host="127.0.0.1", user="sa", password="Admin123", database="TS")

    def execute(self, sql):
        cur = self.cnn.cursor()
        cur.execute(sql)
        self.cnn.commit()

    def close_cnn(self):
        self.cnn.close()

    def query(self, sql):
        df = pd.io.sql.read_sql(sql, self.cnn)
        return(df)


if __name__ == "__main__":
    d=DBUtil()
    d.create_cnn()
    print(type(d))
    d.close_cnn()

