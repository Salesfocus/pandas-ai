import unittest

import pandas as pd

from pandasai.agent import Agent
from pandasai.llm import OpenAI


class TestLoanPayments(unittest.TestCase):
    def setUp(self) -> None:
        # export OPENAI_API_KEY='sk-...'
        llm = OpenAI(temperature=0, api_token="fake_key")

        csv_file_path = "examples/data/Loan payments data.csv"

        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)

        self.df = Agent([df], config={"llm": llm})

    def test_number_response(self):
        response = self.df.chat(
            "How many loans are from men and have been paid off?", "number"
        )
        self.assertEqual(response, 247)

    def test_plot_response(self):
        response = self.df.chat("Plot of age against loan_status ")
        self.assertTrue(
            response.lower().find("pandas-ai/exports/charts/temp_chart.png") != -1
        )

    def test_string_response(self):
        response = self.df.chat(
            "Will women with education high school of below will pay off loan on time?"
        )
        self.assertTrue(response.lower().find("true") != -1)

    def test_dataframe_response(self):
        response = self.df.chat(
            "Load ID and principal of paidoff loan ordered by Loan ID", "dataframe"
        )
        self.assertTrue(response.head_csv.find("Loan_ID,Principal") != -1)
        self.assertTrue(response.head_csv.find("xqd20160003,1000") != -1)
        self.assertTrue(response.head_csv.find("xqd12160159,1000") != -1)
        self.assertTrue(response.head_csv.find("xqd20160004,1000") != -1)
