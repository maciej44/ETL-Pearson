from csvHandler import *

class Task3(CsvHandler):
    def __init__(self, test_csv, class_csv):
        super().__init__()
        self.__class = super().toDataFrame(class_csv)
        self.__test = super().toDataFrame(test_csv)

        # drop unnecessary columns
        self.__class.drop([
            'institution_id',
            'owner_id',
            'created_at',
            'updated_at',
            'latest_test_time',
            'has_student_with_scored_test'
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

        # inner join
        self.__df = pd.merge(
            self.__class,
            self.__test,
            how='inner',
            left_on='id',
            right_on='class_id'
        )

        self.__df.drop([
            'id_x'
        ], axis=1, inplace=True)

        # print(list(self.__df.columns.values))

        # change column order
        self.__df = self.__df[[
            'class_id',
            'name',
            'teaching_hours',
            'id_y',
            'created_at',
            'authorized_at',
            'test_level_id'
        ]]
        
        # drop rows with 'authorized_at' value == Null
        self.__df = self.__df[
            self.__df.authorized_at.notnull()
        ]


        # count class_test_number
        self.__df['class_test_number'] = self.__df.groupby(
            (self.__df['class_id'] != self.__df['class_id'].shift(1)).cumsum()
            ).cumcount() + 1

        # rename final dataset columns, according to audition task
        self.__df.rename(columns={
            'id_y': 'test_id',
            'created_at': 'test_created_at',
            'authorized_at': 'test_authorized_at',
            'test_level_id': 'test_level'
        }, inplace=True)

        # column objects to datetime objects, change date format according to output table example
        # doing it this way i don't lose datetime64 data_type
        self.__df['test_created_at'] = pd.to_datetime(self.__df['test_created_at'])
        self.__df['test_created_at'] = pd.to_datetime(self.__df['test_created_at'].dt.strftime('%Y-%m-%d'))
        self.__df['test_authorized_at'] = pd.to_datetime(self.__df['test_authorized_at'])
        self.__df['test_authorized_at'] = pd.to_datetime(self.__df['test_authorized_at'].dt.strftime('%Y-%m-%d'))
        

        super().save_df(self.__df, 'test_utilization.csv')



test_utilization = Task3('test.csv', 'class.csv')
