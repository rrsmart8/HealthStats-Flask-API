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
    def get_states_mean(self, question):
        """
        Get the mean of the data values for a specific question grouped by state
        """
        # Check if the question is in the data
        if question not in self.data["Question"].unique():
            return "Invalid question or state"
        filtered = self.data[self.data["Question"] == question]
        grouped = filtered.groupby("LocationDesc")["Data_Value"].mean()
        return grouped.sort_values().to_dict()

    def get_state_mean(self, question, state):
        """
        Get the mean of the data values for a specificic question for a specific state
        """
        # Check if the state is in the data
        if state not in self.data["LocationDesc"].unique():
            return "Invalid question or state"
        # Check if the question is in the data
        if question not in self.data["Question"].unique():
            return "Invalid question or state"

        filtered = self.data[(self.data["Question"] == question)]
        filtered = filtered[filtered["LocationDesc"] == state]
        grouped = filtered.groupby("LocationDesc")["Data_Value"].mean()
        return grouped.to_dict()

    def get_best5(self, question):
        """
        Get the best 5 states for a specific question
        """

        # Check if the question is in the list of questions where the best value is the minimum
        question_in_min = question in self.questions_best_is_min

        filtered = self.data[self.data["Question"] == question]
        grouped = filtered.groupby("LocationDesc")["Data_Value"].mean()

        return grouped.sort_values(ascending=question_in_min).head(5).to_dict()

    def get_worst5(self, question):
        """
        Get the best 5 states for a specific question
        """
        filtered = self.data[self.data["Question"] == question]
        grouped = filtered.groupby("LocationDesc")["Data_Value"].mean()
        question_in_max = question in self.questions_best_is_max
        return grouped.sort_values(ascending=question_in_max).head(5).to_dict()

    def global_mean(self, question):
        """
        Get the global mean for a specific question
        """
        filtered = self.data[self.data["Question"] == question]
        mean_value = filtered["Data_Value"].mean()
        return {"global_mean": mean_value}

    def diff_from_mean(self, question):
        """
        Get the difference from the global mean for a specific question
        """
        filtered = self.data[self.data["Question"] == question]
        global_mean = filtered["Data_Value"].mean()
        state_mean = filtered.groupby("LocationDesc")["Data_Value"].mean()
        diff = global_mean - state_mean

        return diff.sort_values(ascending=False).to_dict()

    def state_diff_from_mean(self, question, state):
        """
        Get the difference from the global mean for a specific question for a specific state
        """
        filtered = self.data[self.data["Question"] == question]
        global_mean = filtered["Data_Value"].mean()
        state_mean = filtered[filtered["LocationDesc"] == state]["Data_Value"].mean()
        diff = global_mean - state_mean

        return {state: diff}

    def mean_by_category(self, question):
        """
        For each state (LocationDesc), for each category (StratificationCategory1),
        for each segment (Stratification1), compute the average Data_Value for the requested question.
        Return a flat dictionary with stringified tuple keys.
        """
        # Filter to the chosen question
        filtered = self.data[self.data["Question"] == question]

        # Group by (state, category, segment) and compute mean(Data_Value)
        grouped = filtered.groupby(["LocationDesc", "StratificationCategory1", "Stratification1"])["Data_Value"].mean()

        # Convert index to stringified tuple keys
        results = {str(key): value for key, value in grouped.items()}

        return results

    def state_mean_by_category(self, question, state):
        """
        Compute mean Data_Value for each (StratificationCategory1, Stratification1) for a given question and state.
        Return a dict with the state as key, and inner dict of results as value.
        """
        filtered = self.data[
            (self.data["Question"] == question) &
            (self.data["LocationDesc"] == state)
        ]

        grouped = filtered.groupby(["StratificationCategory1", "Stratification1"])["Data_Value"].mean()

        # Convert index to stringified tuple keys
        results = {str(key): value for key, value in grouped.items()}

        return {state: results}
    
    def get_num_jobs(self):
        """
        Get the number of jobs that are not yet completed
        """
        count = sum(1 for job in self.jobs_info.values() if job.get("status") != "done")
        return {"pending_jobs": count}
    
    def get_job_result(self, job_id):
        """
        Get the result of a specific job
        """
        job = self.jobs_info.get(job_id)
        if job and job.get("status") == "done":
            return {"status": "done", "data": job.get("result")}
        else:
            return {"status": "not done"}
        
    def get_all_jobs(self):
        """
        Get all jobs in the expected JSON format
        """
        jobs_list = [{job_id: job_info["status"]} for job_id, job_info in self.jobs_info.items()]
        return {
            "status": "done",
            "data": jobs_list
        }

    def get_shutdown_status(self):
        """
        Return shutdown status for endpoint.
        """
        if any(job.get("status") != "done" for job in self.jobs_info.values()):
            return {"status": "running"}
        return {"status": "done"}
    
    def graceful_shutdown(self):
        """
        Trigger graceful shutdown.
        """
        self.shutting_down = True

    def is_shutting_down(self):
        """
        Check if shutdown was triggered.
        """
        return self.shutting_down

    def get_shutdown_status(self):
        """
        Returns 'running' if any job is still running, otherwise 'done'
        """
        if any(job.get("status") == "running" for job in self.jobs_info.values()):
            return {"status": "running"}
        return {"status": "done"}

    def show_data(self):
        """
        Show the first few rows of the data
        """
        return self.data.head()
    
    def show_data_value(self, column):
        """
        Show the unique values of the Data_Value column
        """
        return self.data[column].unique()
