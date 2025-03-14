import pyodbc
import os
import sys
import yaml
import requests
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt
import logging
from sklearn.preprocessing import StandardScaler, LabelEncoder

# Configure logging
logging.basicConfig(filename='./logs/prepare_file.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def impute_missing_values(df):
    """
    Imputes missing values in a DataFrame:
    - Numerical columns: Replaced with mean
    - Categorical columns: Replaced with mode (most frequent value)
    """
    for column in df.columns:
        if df[column].dtype == "object":  # Check if column is categorical
            df[column].fillna(df[column].mode()[0], inplace=True)  # Fill with mode
        else:
            df[column].fillna(df[column].mean(), inplace=True)  # Fill with mean
    return df


# Function for processing
def data_processing(source, destination, c, VISUALIZATIONS_PATH):
    logging.info (f"PROCESSING THE FILE {source}")
    df = pd.read_csv(source)

    logging.info (f"Shape of the dataset {df.shape}")
    logging.info (f"Data types of the columns {df.dtypes}")
    logging.info (f"Information of the dataset {df.info()}")

    # Convert 'TotalCharges' to numeric (errors='coerce' converts non-numeric to NaN)
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    logging.info (f"Datatype of Total Charges :{df['TotalCharges'].dtype}")

    df = impute_missing_values(df)
    df = df.drop_duplicates()

    # To see the correlation of the numeric fields
    df.corr(numeric_only=True)

    # Dropping 'customerID' as it's not needed
    df.drop(columns=['customerID'], inplace=True)

    # Creating Features
    # Convert tenure into categories
    def tenure_category(tenure):
        if tenure <= 12:
            return "Short-term"
        elif tenure <= 48:
            return "Medium-term"
        else:
            return "Long-term"
    df['TenureCategory'] = df['tenure'].apply(tenure_category)

    # Create interaction feature: Total Spend
    df['TotalSpend'] = df['tenure'] * df['MonthlyCharges']

    # Create binary feature: Has Internet
    df['HasInternet'] = df['InternetService'].apply(lambda x: 0 if x == "No" else 1)

    # Count of services opted
    df['NumServices'] = df[['OnlineSecurity', 'OnlineBackup', 'DeviceProtection',
                            'TechSupport', 'StreamingTV', 'StreamingMovies']].apply(lambda x: sum(x == 'Yes'), axis=1)


    # Standardize numerical columns
    num_cols = ['tenure', 'MonthlyCharges', 'TotalCharges', 'TotalSpend']
    scaler = StandardScaler()
    df[num_cols] = scaler.fit_transform(df[num_cols])

    #Encoding categorical variables
    cat_cols = ['gender', 'Partner', 'Dependents', 'PhoneService', 'MultipleLines',
                'InternetService', 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection',
                'TechSupport', 'StreamingTV', 'StreamingMovies', 'Contract',
                'PaperlessBilling', 'PaymentMethod', 'Churn', 'TenureCategory']

    label_encoders = {}
    for col in cat_cols:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])
        label_encoders[col] = le

    #Perform EDA
    # Plot Churn distribution
    plt.figure(figsize=(6, 4))
    sns.countplot(x=df["Churn"], palette="viridis")
    plt.title("Churn Distribution")
    plt.xlabel("Churn (0 = No, 1 = Yes)")
    plt.ylabel("Count")
    name=VISUALIZATIONS_PATH+"churn_distribution_"+c+".jpg"
    plt.savefig(name, format="jpg", dpi=300) 
    plt.close()

    # Plot numerical feature distributions
    df[num_cols].hist(figsize=(12, 5), bins=30, layout=(1, 4), color='purple', edgecolor='black')
    plt.suptitle("Distribution of Standardized Numerical Features")
    name=VISUALIZATIONS_PATH+"distribution_num_features_"+c+".jpg"
    plt.savefig(name, format="jpg", dpi=300)
    plt.close()

    # Boxplot to detect outliers
    plt.figure(figsize=(12, 5))
    sns.boxplot(data=df[num_cols], palette="coolwarm")
    plt.title("Boxplot of Standardized Numerical Features (Outliers Detection)")
    plt.xticks(rotation=45)
    name=VISUALIZATIONS_PATH+"boxplot_"+c+".jpg"
    plt.savefig(name, format="jpg", dpi=300)
    plt.close()

    os.makedirs(os.path.dirname(destination), exist_ok=True)

    # Save cleaned data
    df.to_csv(destination, index=False)

    logging.info("Data Preprocessing, EDA and feature engineering Completed and Saved.")

def main():
    # File paths
    SYNTHETIC_CSV_PATH = sys.argv[1]
    KAGGLE_CSV_PATH = sys.argv[2]
    params = yaml.safe_load(open("params.yaml"))["prepare"]
    DATA_PATH = params["data_path"]
    VISUALIZATIONS_PATH = params["visualizations_path"]
    os.makedirs(os.path.dirname(VISUALIZATIONS_PATH), exist_ok=True)

    KAGGLE_CSV_PATH_PROCESSED = os.path.join(DATA_PATH, "static/static_data.csv")
    SYNTHETIC_CSV_PATH_PROCESSED = os.path.join(DATA_PATH, "api/api_data.csv")
    c = "kaggle"
    data_processing(KAGGLE_CSV_PATH, KAGGLE_CSV_PATH_PROCESSED, c, VISUALIZATIONS_PATH )
    c = "synthetic"
    data_processing(SYNTHETIC_CSV_PATH, SYNTHETIC_CSV_PATH_PROCESSED, c, VISUALIZATIONS_PATH)

if __name__ == "__main__":
    main()
