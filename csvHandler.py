import pandas as pd

class CsvHandler:
    def __init__(self):
        pass

    def toDataFrame(self, csv_file_name):
        with open(csv_file_name, newline='') as file:
            self.__df = pd.read_csv(file, sep=';')

        return self.__df


    def save_df(self, df_obj, outp_file_name):
        df_obj.to_csv(r'{}'.format(outp_file_name), sep=';', encoding='utf-8-sig', index=False)
        print('Saving filtered DataFrame in file: {}'.format(outp_file_name))
