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
        self.__final_df = pd.merge(
            self.__class,
            self.__test,
            how='inner',
            left_on='id',
            right_on='class_id'
        )

        self.__final_df.drop([
            'id_x'
        ], axis=1, inplace=True)

        # print(list(self.__final_df.columns.values))

        # change column order
        self.__final_df = self.__final_df[[
            'class_id',
            'name',
            'teaching_hours',
            'id_y',
            'created_at',
            'authorized_at',
            'test_level_id',
            # 'test_status',
            # 'overall_score',
            # 'writing_score',
            # 'reading_score'
        ]]
        
        # drop rows with 'authorized_at' value == Null
        self.__final_df = self.__final_df[
            self.__final_df.authorized_at.notnull()
        ]


        # count class_test_number
        self.__final_df['class_test_number'] = self.__final_df.groupby(
            (self.__final_df['class_id'] != self.__final_df['class_id'].shift(1)).cumsum()
            ).cumcount() + 1

        # rename final dataset columns, according to audition task
        self.__final_df.rename(columns={
            'id_y': 'test_id',
            'created_at': 'test_created_at',
            'authorized_at': 'test_authorized_at',
            'test_level_id': 'test_level'
        }, inplace=True)

        print(self.__final_df.head(20))
        #print(self.__final_df.has_student_with_scored_test.unique())
        #print(self.__final_df.loc[self.__final_df['has_student_with_scored_test'] != 1])



test_utilization = Task3('test.csv', 'class.csv')
