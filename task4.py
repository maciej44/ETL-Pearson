from csvHandler import *

class Task4(CsvHandler):
    def __init__(self, class_csv, test_csv):
        super().__init__()
        self.__class = super().toDataFrame(class_csv)
        self.__test = super().toDataFrame(test_csv)
        

        # select columns in dataframes
        self.__class = self.__class[[
            'id',
            'name',
            'teaching_hours'
        ]]

        test_rows = [
            'class_id',
            'created_at',
            'authorized_at',
            'test_status',
            'overall_score'
        ]

        self.__test = self.__test[test_rows]

        # drop rows other than with 'test_status' == 'SCORING_SCORED'
        self.__test = self.__test[
            self.__test.test_status == 'SCORING_SCORED'
        ]
        # drop rows with authorized_at value == null
        self.__test = self.__test[
            self.__test.authorized_at.notnull()
        ]
        # drop rows with overall_score == null
        self.__test = self.__test[
            self.__test.overall_score.notnull()
        ]
        
        # inner join
        self.__df = pd.merge(
            self.__class,
            self.__test,
            how='inner',
            left_on='id',
            right_on='class_id'
        )

        # ar = self.__df.groupby(['id'], as_index=False)['overall_score'].mean()

        # using agg, as groupby().mean causes "automatic exclusion of nuisance columns" (in docs)
        f = {
            'name': 'first',
            'teaching_hours': 'first',
            'created_at': 'first',
            'authorized_at': 'first',
            'overall_score': 'mean'
        }
        self.__df = self.__df.groupby(['id'], as_index=False).agg(f)


        self.__df.rename(columns={
            'id': 'class_id',
            'name': 'class_name',
            'created_at': 'test_created_at',
            'authorized_at': 'test_authorized_at',
            'overall_score': 'avg_class_test_overall_score'
        }, inplace=True)

        print(self.__df)



        # print(self.__test.loc[self.__test['authorized_at'].isnull()])
        # print(self.__test.test_status.unique())



task4 = Task4('class.csv', 'test.csv')