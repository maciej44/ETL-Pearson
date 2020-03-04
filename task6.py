from csvHandler import *
import sqlalchemy
import psycopg2 
import io

# To load data into PostreSQL database it is necessary to create table with same name and columns first
# Script tested, works with my university database (vpn connection)
# https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table/47984180#47984180

class Task6(CsvHandler):
    def __init__(self, file_name):
        super().__init__()
        self.file = file_name
        self.__df = super().toDataFrame(self.file)



    def send_pgsql(self):
        self.engine = sqlalchemy.create_engine(
            'postgresql+psycopg2://USER:PASSW@psql.wmi.amu.edu.pl:5432/DATABASE'
            )
        # cut .csv extension to get table name
        self.sql_table_name = self.file[:-4:]

        #truncates the table
        self.__df.head(0).to_sql(self.sql_table_name, self.engine, if_exists='replace',index=False)

        self.conn = self.engine.raw_connection()
        self.cur = self.conn.cursor()
        self.output = io.StringIO()
        self.__df.to_csv(self.output, sep='\t', header=False, index=False)
        self.output.seek(0)
        self.contents = self.output.getvalue()

        # null values become ''
        self.cur.copy_from(self.output, self.sql_table_name, null="")
        self.conn.commit()


    def replace_tabulators(self, column_name):
        # replace tabulators in col for database insert
        self.__df[column_name] = self.__df[column_name].str.replace('\t', '')




sendTable1 = Task6('test_average_scores.csv')
sendTable1.replace_tabulators('class_name')
sendTable1.send_pgsql()
