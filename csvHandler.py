import pandas as pd

class CsvHandler:
    def __init__(self):
        pass

    def toDataFrame(self, csv_file):
        with open(csv_file, newline='') as file:
            df = pd.read_csv(file, sep=';')
            #df.to_html('temp.html')

        return df

