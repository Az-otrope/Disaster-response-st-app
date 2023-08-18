import os

import pandas as pd
from sqlalchemy import create_engine

from src.settings import RAW


def fetch_data():
    """
    To read in the raw data files and create a sqlite database.
    INPUT: raw data files with tweets and SMS
    OUTPUT: a dataframe queried from the sqlite database
    """

    db_file = os.path.join(RAW, "sqlite")
    engine = create_engine("sqlite:///" + db_file)
    # df = pd.read_sql_table('sqlite', engine)
    df = pd.read_sql_query("SELECT * FROM sqlite", engine)

    return df
