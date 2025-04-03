import unittest
import pandas as pd
from app.data_ingestor import DataIngestor

class TestWebserver(unittest.TestCase):
    def setUp(self):
        self.df = pd.read_csv("unittests/testing.csv")
        self.ingestor = DataIngestor(dataframe=self.df)

    def test_states_mean(self):
        question = "Percent of adults who engage in no leisure-time physical activity"
        result = self.ingestor.get_states_mean(question)
        expected = {
            "Maryland": 25.0,
            "Texas": 40.0
        }
        self.assertEqual(result, expected)

    def test_state_mean(self):
        question = "Percent of adults who engage in no leisure-time physical activity"
        state = "Maryland"
        result = self.ingestor.get_state_mean(question, state)
        expected = {
            "Maryland": 25.0
        }
        self.assertEqual(result, expected)

    def test_best5(self):
        question = "Percent of adults who engage in no leisure-time physical activity"
        result = self.ingestor.get_best5(question)
        expected = {
            "Maryland": 25.0,
            "Texas": 40.0  # only two states in dataset, both included
        }
        self.assertEqual(result, expected)

    def test_worst5(self):
        question = "Percent of adults who engage in no leisure-time physical activity"
        result = self.ingestor.get_worst5(question)
        expected = {
            "Texas": 40.0,
            "Maryland": 25.0  # only two states in dataset
        }
        self.assertEqual(result, expected)

    def test_global_mean(self):
        result = self.ingestor.global_mean("Percent of adults who engage in no leisure-time physical activity")
        expected = {'global_mean': 30.0} # (20 + 30 + 40) / 3
        self.assertEqual(result, expected)
    
    def test_diff_from_mean(self):
        result = self.ingestor.diff_from_mean("Percent of adults who engage in no leisure-time physical activity")
        expected = {
            'Maryland': 5.0,
            'Texas': -10.0       
        }
        self.assertEqual(result, expected)

    def test_state_diff_from_mean(self):
        result = self.ingestor.state_diff_from_mean("Percent of adults who engage in no leisure-time physical activity", "Maryland")
        expected = {'Maryland': 5.0}
        self.assertEqual(result, expected)


    def test_mean_by_category(self):
        result = self.ingestor.mean_by_category("Percent of adults who engage in no leisure-time physical activity")
        expected = {
            "('Maryland', 'Gender', 'Female')": 20.0,
            "('Maryland', 'Gender', 'Male')": 30.0,
            "('Texas', 'Gender', 'Male')": 40.0
        }
        self.assertEqual(result, expected)

    def test_state_mean_by_category(self):
        result = self.ingestor.state_mean_by_category(
            "Percent of adults who engage in no leisure-time physical activity", "Maryland"
        )
        expected = {
            'Maryland': {
                "('Gender', 'Female')": 20.0,
                "('Gender', 'Male')": 30.0
            }
        }
        self.assertEqual(result, expected)

    def test_num_jobs(self):
        self.ingestor.jobs_info = {
            'job1': {'status': 'done'},
            'job2': {'status': 'running'},
            'job3': {'status': 'running'},
        }

        expected = {"pending_jobs": 2}
        result = self.ingestor.get_num_jobs()
        self.assertEqual(result, expected)

    def test_get_results_valid_done_job(self):
        self.ingestor.jobs_info = {
            'job123': {'status': 'done', 'result': {"Maryland": 26.0}}
        }

        result = self.ingestor.get_job_result('job123')
        expected = {
            "status": "done",
            "data": {"Maryland": 26.0}
        }

        self.assertEqual(result, expected)

    def test_jobs_list(self):
        # Simulate job_info dict
        self.ingestor.jobs_info = {
            'job1': {'status': 'done'},
            'job2': {'status': 'running'},
            'job3': {'status': 'done'},
        }

        expected = {
            "status": "done",
            "data": [
                {"job1": "done"},
                {"job2": "running"},
                {"job3": "done"}
            ]
        }

        result = self.ingestor.get_all_jobs()
        self.assertEqual(result, expected)

    def test_graceful_shutdown(self):
        
        self.ingestor.jobs_info = {
            "job_1": {"status": "done"},
            "job_2": {"status": "running"},
            "job_3": {"status": "done"}
        }

        # Initiate graceful shutdown
        self.ingestor.graceful_shutdown()

        # Check if shutdown flag is set
        self.assertTrue(self.ingestor.is_shutting_down())

        # Check that get_shutdown_status returns "running" because job_2 is still in progress
        result = self.ingestor.get_shutdown_status()
        expected = {"status": "running"}
        self.assertEqual(result, expected)

        # Mark job_2 as done and check status again
        self.ingestor.jobs_info["job_2"]["status"] = "done"
        result = self.ingestor.get_shutdown_status()
        expected = {"status": "done"}
        self.assertEqual(result, expected)

    def test_invalid_state_question(self):
        question = "Invalid Question"
        # Compare with the expected error message
        result = self.ingestor.get_states_mean(question)
        expected = "Invalid question or state"
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
