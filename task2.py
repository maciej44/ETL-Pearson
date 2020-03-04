from csvHandler import *
import datetime
import numpy as np

class Task2(CsvHandler):
    def __init__(self, csv_file):
        super().__init__()
        self.__csv_file_name = csv_file
        self.__copy_file_name = 'copy_' + self.__csv_file_name
        self.__df = super().toDataFrame(csv_file)
        self.__df.to_csv(r'backup_files/{}'.format(self.__copy_file_name), sep=';', index=False)

        print('Creates backup copy of {} in: backup_files/{}'.format(csv_file, self.__copy_file_name))


    def drop_not_unique_in_col(self, column_name):
        # for pk columns
        self.__df.drop_duplicates(
            subset=[column_name],
            keep=False,
            inplace=True
        )



    def __is_utc_or_null(self, date):
        try:
            # check if date is valid format
            if date == '' or datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f%z'):
                return True
        except ValueError:
            print('Found invalid date format: {}, deleting row...'.format(date))
            return False


    def drop_not_utc_dates(self, column_name):
        # couldn't handle format='%Y-%m-%dT%H:%M:%S.%f%z' data with pandas to_datetime(), as it gives unexpected results,
        # so I did it with str object

        # replace null values with '' and obj to string
        
        self.__df[column_name] = self.__df[column_name].replace(np.nan, '').astype(str)

        self.__df = self.__df[self.__df[column_name].map(self.__is_utc_or_null) == True]# .astype('datetime64[ns]')
        
        # to_datetime()
        # self.__df[column_name] = pd.to_datetime(self.__df[column_name], errors='coerce')
        # print(self.__df[column_name])
        


    def __is_date_or_null(self, date):
        try:
            if date == '' or datetime.datetime.strptime(date, '%d.%m.%y %H:%M'):
                return True
        except ValueError:
            print('Found invalid date format: {}, deleting row...'.format(date))
            return False


    def drop_if_not_date_and_not_null(self, column_name):
        #drops columns with invalid
        self.__df[column_name] = self.__df[column_name].replace(np.nan, '').astype(str)

        self.__df = self.__df[self.__df[column_name].map(self.__is_date_or_null) == True]
        #print(self.__df[column_name])



    def drop_if_not_date(self, column_name):
        # drops rows with invalid dates (nan also)
        # use when every row in column shoud be date
        self.__df[column_name] = self.__df[column_name].replace(np.nan, '')

        self.__df[column_name] = pd.to_datetime(self.__df[column_name], errors='coerce')
        self.__df = self.__df.dropna(subset=[column_name])

    

    def drop_if_falls_outside_closed_interval(self, column_name, min_value, max_value):
        # drop rows with 
        self.index_names = self.__df[(self.__df[column_name] < min_value) | (self.__df[column_name] > max_value)].index
        self.__df.drop(self.index_names, inplace=True)



    def drop_if_not_in(self, column_name, *values_list):
        # drops rows with values other than *values_list
        self.index_names = self.__df[self.__df[column_name].isin(*values_list) == False].index
        self.__df.drop(self.index_names, inplace=True)



    def get_dtypes(self):
        print(self.__df.dtypes)

    
    def save_to_file(self):
        # self.__validated_file_name = 'validated_' + self.__csv_file_name
        super().save_df(self.__df, self.__csv_file_name)



# VALIDATE test.csv
test_table = Task2('test.csv')

test_table.drop_not_unique_in_col('id')

test_table.drop_not_utc_dates('last_event_time')
test_table.drop_if_not_date('created_at')
test_table.drop_if_not_date('updated_at')
test_table.drop_if_not_date_and_not_null('authorized_at')

test_table.drop_if_falls_outside_closed_interval('overall_score', 0, 200)
test_table.drop_if_falls_outside_closed_interval('speaking_score', 0, 200)
test_table.drop_if_falls_outside_closed_interval('writing_score', 0, 200)
test_table.drop_if_falls_outside_closed_interval('reading_score', 0, 200)
test_table.drop_if_falls_outside_closed_interval('listening_score', 0, 200)
test_table.drop_if_falls_outside_closed_interval('confidence_level', 0, 1)

test_table.save_to_file()

# VALIDATE class.csv
class_table = Task2('class.csv')

class_table.drop_not_unique_in_col('id')

class_table.drop_if_not_date('created_at')
class_table.drop_if_not_date('updated_at')
class_table.drop_if_not_date_and_not_null('latest_test_time')

class_table.drop_if_not_in('has_student_with_scored_test', [0, 1])

class_table.save_to_file()