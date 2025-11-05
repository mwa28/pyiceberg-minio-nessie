import pandas as pd


def calculate_delta(df: pd.DataFrame) -> pd.DataFrame:
    # Calculate the difference from previous timestamp
    df["diff_ts"] = df.groupby(["controller_id", "parameter"])["timestamp"].diff()
    df["diff_ts"] = df["diff_ts"].fillna(df["timestamp"].astype("double"))
    return df
