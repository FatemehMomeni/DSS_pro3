import csv


class FactTables:
    def __init__(self):
        self.fact_tables = ['factCoins', 'factStocks']

    def delete(self):
        # delete tables if exists
        delete_fact = list()
        for ft in self.fact_tables:
            delete_fact.append(f"drop table if exists {ft}")

        return delete_fact

    def create(self):
        create_coins = "create table factCoins( keyTime int not null, keyCoin int not null,"\
                      "valueCoin float not null,"\
                      "foreign key (keyTime) references dimTime(keyTime),"\
                      "foreign key (keyCoin) references dimCoin(keyCoin),"\
                      "primary key (keyTime, keyCoin))"
        create_stocks = "create table factStocks( keyTime int not null, keyCompany int not null,"\
                        "openValueStock float not null, closeValueStock float not null,"\
                        "highValueStock float not null, lowValueStock float not null,"\
                        "quantityStock float not null,"\
                        "foreign key (keyTime) references dimTime(keyTime),"\
                        "foreign key (keyCompany) references dimCompany(keyCompany),"\
                        "primary key (keyTime, keyCompany))"

        return [create_coins, create_stocks]

    def insert_coins(self):
        queries = list()
        with open('./data/factCoins.csv', encoding='utf-8-sig') as csv_file:
            excel_list = csv.reader(csv_file, delimiter=',')
            next(excel_list)

            for row in excel_list:
                queries.append(f"insert into factCoins( keyTime, keyCoin, valueCoin) "
                               f"values({row[0]}, {row[1]}, {row[2]})")

        return queries

    def insert_stocks(self):
        queries = list()
        with open('./data/factStocks.csv', encoding='utf-8-sig') as csv_file:
            excel_list = csv.reader(csv_file, delimiter=',')
            next(excel_list)

            for row in excel_list:
                queries.append(f"insert into factStocks( keyTime, keyCompany, openValueStock, closeValueStock,"
                               f"highValueStock, lowValueStock, quantityStock) "
                               f"values({row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]})")

        return queries

