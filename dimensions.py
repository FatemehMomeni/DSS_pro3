import csv


class DimensionTables:
    def __init__(self):
        self.dimension_tables = ['dimCoin', 'dimTime', 'dimCompany']

    def delete(self):
        # delete tables if exists
        delete_dim = list()
        for dt in self.dimension_tables:
            delete_dim.append(f"drop table if exists {dt}")

        return delete_dim

    def create(self):
        create_coin = "create table dimCoin( keyCoin int not null, abbrevCoin varchar(32) not null," \
                      "nameCoin varchar(32) not null, symbolCoin varchar(8) not null," \
                      "primary key (keyCoin))"
        create_time = "create table dimTime( keyTime int not null, datetimeTime varchar(32) not null," \
                      "dayTime smallint not null, dayWeekTime smallint not null," \
                      "dayWeekAbbrevTime varchar(32) not null, dayWeekCompleteTime varchar(32) not null," \
                      "monthTime smallint not null, monthAbbrevTime varchar(32) not null," \
                      "monthCompleteTime varchar(32) not null, bimonthTime smallint not null," \
                      "quarterTime smallint not null, semesterTime smallint not null, yearTime int not null," \
                      "primary key (keyTime))"
        create_company = "create table dimCompany( keyCompany int not null, stockCodeCompany varchar(32) not null,"\
                         "nameCompany varchar(64) not null,sectorCodeCompany varchar(32) not null,"\
                         "sectorCompany varchar(256) not null,segmentCompany varchar(256) not null,"\
                         "primary key (keyCompany))"

        return [create_coin, create_time, create_company]

    def insert_coin(self):
        queries = list()
        with open('./data/dimCoin.csv', encoding='utf-8-sig') as csv_file:
            excel_list = csv.reader(csv_file, delimiter=',')
            next(excel_list)

            for row in excel_list:
                queries.append(f"insert into dimCoin( keyCoin, abbrevCoin, nameCoin, symbolCoin) "
                               f"values({row[0]}, '{row[1]}', '{row[2]}', '{row[3]}')")

        return queries

    def insert_time(self):
        queries = list()
        with open('./data/dimTime.csv', encoding='utf-8-sig') as csv_file:
            excel_list = csv.reader(csv_file, delimiter=',')
            next(excel_list)

            for row in excel_list:
                queries.append(f"insert into dimTime( keyTime, datetimeTime, dayTime, dayWeekTime, dayWeekAbbrevTime,"
                               f"dayWeekCompleteTime, monthTime, monthAbbrevTime, monthCompleteTime, bimonthTime,"
                               f"quarterTime, semesterTime, yearTime) "
                               f"values({row[0]}, '{row[1]}', {row[2]}, {row[3]}, '{row[4]}', '{row[5]}', {row[6]},"
                               f"'{row[7]}', '{row[8]}', {row[9]}, {row[10]}, {row[11]}, {row[12]})")

        return queries

    def insert_company(self):
        queries = list()
        with open('./data/dimCompany.csv', encoding='utf-8-sig') as csv_file:
            excel_list = csv.reader(csv_file, delimiter=',')
            next(excel_list)

            for row in excel_list:
                queries.append(f"insert into dimCompany( keyCompany, stockCodeCompany, nameCompany, sectorCodeCompany,"
                               f"sectorCompany, segmentCompany) "
                               f"values({row[0]}, '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5]}')")

        return queries
