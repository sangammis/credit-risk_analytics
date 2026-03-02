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

# Data Cleaning 
def clean_data(df):
    # Created a copy so we don't modify the oroginal dataset unexpectedly
    working_df = df.copy()

    try:
        # Filling missing credit score with the median
        if 'credit_score' in working_df.columns:
            working_df['credit_score'] = working_df['credit_score'].fillna(working_df['credit_score'].median())
            logging.info("Imputed missing credit scores with median")

         # Vectorized operation to create high risk flag
        working_df["is_high_risk"] = np.where(working_df['credit_scre'] < 600,1,0)
        return working_df
    
    except KeyError as e:
        logging.error(f"Column missing in dataset : {e}")
    return working_df

# main pipeline
data_path = "data"
os.makedirs(data_path, exist_ok= True)
raw_file_path = os.path.join(data_path, 'loan_customers.csv')
processed_file_path = os.path.join(data_path, "processed_loan_customers.csv")

def main():
    logging.info("Pipeline started")
    df = load_data(raw_file_path)
    df = clean_data(df)

    df.to_csv(processed_file_path, index = False)

    logging.info(f"Pipeline Complete. Cleaned data saved to: {processed_file_path}")
    print(f"Success! Find your file here: {os.path.abspath(processed_file_path)}")


# entry point
if __name__ == "__main__":
    main()





