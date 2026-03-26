import pandas as pd
import numpy as np
import logging
import os

# ─────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────
os.makedirs("logs", exist_ok = True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level = logging.INFO, 
    format = '%(asctime)s : %(levelname)s : %(message)s')


# ─────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────
def load_data(path):
    try:
        df = pd.read_csv(path)
        logging.info("Data loaded successfully")
        return df
    except Exception as e:
        logging.error(f"Error while loading the data : {e}")
        raise

# ─────────────────────────────────────
# DATA VALIDATION
# ─────────────────────────────────────
def validate_data(df):
    required_cols = ['credit_score', 'income', 'loan_amount']

    # 1. check required columns exist
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    logging.info("Data validation passed")
    return df

# ─────────────────────────────────────
# Schema VALIDATION
# ─────────────────────────────────────
def enforce_schema(df):
    schema = {
        "customer_id": "int64",
        "age": "int64",
        "income": "float64",
        "loan_amount": "float64",
        "credit_score": "float64",
        "default_flag": "int64"
    }
    for col, dtype in schema.items():
        df[col] = df[col].astype(dtype)

    logging.info("Schema enforcement completed")
    return df

# ─────────────────────────────────────
# DATA Cleaning
# ─────────────────────────────────────
def clean_data(df):
    # Created a copy so we don't modify the oroginal dataset unexpectedly
    working_df = df.copy()
    required_cols = ['credit_score', 'income', 'loan_amount']

    for col in required_cols:
        null_count = working_df[col].isnull().sum()
        null_pct = (null_count/len(working_df)) * 100

        if null_pct == 0:
            logging.info(f" {col}: no nulls found")
        elif null_pct <= 20:
            median_val = working_df[col].median()
            working_df[col] = working_df[col].fillna(median_val)
            logging.warning(f"{col} : {null_pct: .1f}% nulls "
                         f"filled with median ({median_val: .2f})")
        else:
            raise ValueError(f"{col} has {null_pct: .1f}%. Investigate souce data")

    working_df["is_high_risk"] = np.where(
        working_df['credit_score'] < 600, 1, 0)
    
    return working_df

# ─────────────────────────────────────
# FEATURE ENGINEERING
# ─────────────────────────────────────
def feature_engineering(df):
    df['income_to_loan_ratio'] = df['income'] / df['loan_amount']
    logging.info("Feature engineerng completed.")
    return df

# ─────────────────────────────────────
# FILE PATHS
# ─────────────────────────────────────
data_path = "data"
os.makedirs(data_path, exist_ok= True)
raw_file_path = os.path.join(data_path, 'loan_customers.csv')
processed_file_path = os.path.join(data_path, "processed_loan_customers.csv")

# ─────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────
def main():
    logging.info("Pipeline started")
    df = load_data(raw_file_path)
    df = validate_data(df)
    df = enforce_schema(df)
    df = clean_data(df)
    df = feature_engineering(df)

    df.to_csv(processed_file_path, index = False)

    logging.info(f"Pipeline Complete. Cleaned data saved to: {processed_file_path}")
    print(f"Success! Find your file here: {os.path.abspath(processed_file_path)}")


# ─────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────
if __name__ == "__main__":
    main()





