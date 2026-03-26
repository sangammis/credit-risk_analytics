import pandas as pd
import numpy as np
import logging
import os

# logging setup
os.makedirs("logs", exist_ok = True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level = logging.INFO, 
    format = '%(asctime)s : %(levelname)s : %(message)s')

# Data Loading
def load_data(path):
    try:
        df = pd.read_csv(path)
        logging.info("Data loaded successfully")
        return df
    except Exception as e:
        logging.error(f"Error while loading the data : {e}")
        raise

# Data Validation
def validate_data(df):
    required_cols = ['credit_score', 'income', 'loan_amount']

    # 1. check required columns exist
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    # 2. check no nulls in critical columns
    for col in required_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            raise ValueError(f"Column '{col}' has {null_count} null values")

    logging.info("Data validation passed")
    return df

# Data Cleaning 
def clean_data(df):
    # Created a copy so we don't modify the oroginal dataset unexpectedly
    working_df = df.copy()

    working_df['credit_score'] = working_df['credit_score'].fillna(
        working_df['credit_score'].median()
    )
    # FIXED BUG
    working_df["is_high_risk"] = np.where(
        working_df['credit_score'] < 600, 1, 0
    )
    return working_df

# Feature Engineering Layer
def feature_engineering(df):
    df['income_to_loan_ratio'] = df['income'] / df['loan_amount']
    return df

# main pipeline
data_path = "data"
os.makedirs(data_path, exist_ok= True)
raw_file_path = os.path.join(data_path, 'loan_customers.csv')
processed_file_path = os.path.join(data_path, "processed_loan_customers.csv")

def main():
    logging.info("Pipeline started")
    df = load_data(raw_file_path)
    df = validate_data(df)
    df = clean_data(df)

    df.to_csv(processed_file_path, index = False)

    logging.info(f"Pipeline Complete. Cleaned data saved to: {processed_file_path}")
    print(f"Success! Find your file here: {os.path.abspath(processed_file_path)}")


# entry point
if __name__ == "__main__":
    main()





