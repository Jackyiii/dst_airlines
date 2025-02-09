import pandas as pd


def get_airport(engine):
    """
    get data from airport table database，et return  DataFrame
    """
    query = "SELECT * FROM airport"
    df = pd.read_sql(query, engine)
    return pd.DataFrame(df)