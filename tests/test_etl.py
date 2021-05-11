import unittest
import pandas as pd
from etl import extract_data
from etl import transform_data


class CovidEtlTestCase(unittest.TestCase):
    def test_extract_ny(self):
        nyt_data = extract_data.extract_data("../data/ny_test_data.csv", "NYT")
        self.assertEqual(nyt_data.shape[0], 10)
        self.assertEqual(nyt_data.shape[1], 3)

        jh_data = extract_data.extract_data("../data/jh_test_data.csv", "JH")
        self.assertEqual(jh_data.shape[0], 14)
        self.assertEqual(jh_data.shape[1], 6)

        df_transformed = transform_data.transform_data(nyt_data, jh_data)
        self.assertIsNotNone(df_transformed)
        self.assertEqual(df_transformed.shape[0], 10)
        self.assertEqual(df_transformed.shape[1], 4)

    def test_validate_incorrect_col_names(self):
        try:
            nyt_data = extract_data.extract_data("../data/ny_test_data.csv", "NYT")
            transform_data.validate_data(nyt_data, ["a", "b", "c"])
            self.assertEqual(True, False)
        except Exception as e:
            print(str(e))
            self.assertEqual(str(e), "Incorrect column names: NYT.")

    def test_validate_no_data(self):
        try:
            df = pd.DataFrame()
            df.name = "EMPTY"
            transform_data.validate_data(df, ["a", "b", "c"])
            self.assertEqual(True, False)
        except Exception as e:
            print(str(e))
            self.assertEqual(str(e), "Empty data frame: EMPTY.")


if __name__ == '__main__':
    unittest.main()
