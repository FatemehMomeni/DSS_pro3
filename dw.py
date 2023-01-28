import pyodbc
import dimensions
import facts
import pandas as pd
from sklearn.cluster import KMeans


class DataWarehouse:
    def __init__(self):
        self.conn = pyodbc.connect('Driver={SQL Server};'
                                   'Server=DESKTOP-17M5H9H;'
                                   'Database=brazilian_stock_market;'
                                   'Trusted_Connection=yes;')
        self.cursor = self.conn.cursor()

        self.dim = dimensions.DimensionTables()
        self.fac = facts.FactTables()

        self.delete_and_create()
        self.insert_data()
        self.clustering()

    def run_query(self, query):
        for i in range(len(query)):
            self.cursor.execute(query[i])

    def delete_and_create(self):
        # delete tables if exists
        self.run_query(self.dim.delete())
        self.run_query(self.fac.delete())

        # create tables
        self.run_query(self.dim.create())
        self.run_query(self.fac.create())

    def insert_data(self):
        self.run_query(self.dim.insert_coin())
        self.run_query(self.dim.insert_time())
        self.run_query(self.dim.insert_company())

        self.run_query(self.fac.insert_coins())
        self.run_query(self.fac.insert_stocks())

    def clustering(self):
        print("K-Means Clustering:")
        fact_coins_df = pd.read_sql_query(f"select * from {self.fac.fact_tables[0]}", self.conn)
        fact_stocks_df = pd.read_sql_query(f"select * from {self.fac.fact_tables[1]}", self.conn)

        fact_coins_df['label'] = KMeans(n_clusters=3).fit_predict(fact_coins_df)
        for i in range(3):
            print(f"Cluster{i} Length: {len(fact_coins_df[fact_coins_df.label == i])}")
        print('\n')
        fact_stocks_df['label'] = KMeans(n_clusters=4).fit_predict(fact_stocks_df)
        for i in range(4):
            print(f"Cluster{i} Length: {len(fact_stocks_df[fact_stocks_df.label == i])}")
        print('\n')


if __name__ == '__main__':
    dw = DataWarehouse()
