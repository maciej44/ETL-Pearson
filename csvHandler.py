import csv
import pandas as pd
import numpy as np

class CsvHandler:
    def __init__(self):
        pass

    def toDataFrame(self, csv_file):
        with open(csv_file, newline='') as file:
            df = pd.read_csv(file, sep=';')
            #df.to_html('temp.html')

        return df



class Task3(CsvHandler):
    def __init__(self, class_csv, test_csv):
        super().__init__()
        self.__class = super().toDataFrame(test_csv)
        self.__test = super().toDataFrame(class_csv)

        # drop unnecessary columns
        self.__class.drop([
            'institution_id',
            'owner_id',
            'created_at',
            'updated_at',
            'latest_test_time',
            # SPR, czy po tym np nie usuwać rzędow od razu 'has_student_with_scored_test'
        ], axis=1, inplace=True)

        self.__test.drop([
            'student_id',
            'updated_at',
            'last_event_time',
            'overall_score',
            'test_status',
            'institution_id',
            'confidence_level',
            'speaking_score',
            'writing_score',
            'reading_score',
            'listening_score',
            'licence_id'
        ], axis=1, inplace=True)

        print(self.__test.head(4))

        # inner join
        self.__final_df = pd.merge(
            self.__class,
            self.__test,
            how='inner',
            left_on='id',
            right_on='class_id',
        )

        self.__final_df.drop([
            'id_x'
        ], axis=1, inplace=True)

        print(list(self.__final_df.columns.values))

        # change column order
        self.__final_df = self.__final_df[['class_id', 'name', 'teaching_hours', 'id_y', 'test_level_id', 'created_at', 'authorized_at', 'has_student_with_scored_test']]
        
        # drop rows with 'authorized_at' value == Null
        self.__final_df = self.__final_df[self.__final_df.authorized_at.notnull()]


        # count class_test_number
        self.__final_df['class_test_number'] = self.__final_df.groupby((self.__final_df['class_id'] != self.__final_df['class_id'].shift(1)).cumsum()).cumcount() + 1

        print(self.__final_df.head(20))
        print(self.__final_df.has_student_with_scored_test.unique())
        print(self.__final_df.loc[self.__final_df['has_student_with_scored_test'] != 1])



test_utilization = Task3('test.csv', 'class.csv')


'''
class A(object):

    def __init__(self):
        print("Initialiser A was called")

class B(A):

    def __init__(self):
        A.__init__(self)
        # A.__init__(self,<parameters>) if you want to call with parameters
        print("Initialiser B was called")

class C(B):

    def __init__(self):
        #A.__init__(self) # if you want to call most super class...
        B.__init__(self)
        print("Initialiser C was called")


test = C()
'''