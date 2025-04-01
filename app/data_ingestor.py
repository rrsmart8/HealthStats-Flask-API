import pandas as pd

class DataIngestor:
    """
    Class to handle the data processing 
    """
    def __init__(self, csv_path: str = None, dataframe: pd.DataFrame = None):
        """
        Constructor to initialize the DataIngestor with a CSV file path or directly a DataFrame.
        """
        if csv_path is not None:
            self.data = pd.read_csv(csv_path)
        elif dataframe is not None:
            self.data = dataframe.copy()
        else:
            raise ValueError("Either csv_path or dataframe must be provided.")

        self.data["Question"] = self.data["Question"].fillna("").astype(str)
        self.data["LocationDesc"] = self.data["LocationDesc"].fillna("").astype(str)

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]
    