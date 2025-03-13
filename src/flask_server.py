from flask import Flask, jsonify
import pandas as pd
import numpy as np

app = Flask(__name__)

# Function to generate synthetic data
def generate_synthetic_data(num_rows=1000):
    synthetic_data = pd.DataFrame({
        "customerID": [f"ID{i}" for i in range(num_rows)],
        "gender": np.random.choice(["Male", "Female"], num_rows),
        "SeniorCitizen": np.random.choice([0, 1], num_rows),
        "Partner": np.random.choice(["Yes", "No"], num_rows),
        "Dependents": np.random.choice(["Yes", "No"], num_rows),
        "tenure": np.random.randint(1, 72, num_rows),
        "PhoneService": np.random.choice(["Yes", "No"], num_rows),
        "MultipleLines": np.random.choice(["Yes", "No", "No phone service"], num_rows),
        "InternetService": np.random.choice(["DSL", "Fiber optic", "No"], num_rows),
        "OnlineSecurity": np.random.choice(["Yes", "No", "No internet service"], num_rows),
        "OnlineBackup": np.random.choice(["Yes", "No", "No internet service"], num_rows),
        "DeviceProtection": np.random.choice(["Yes", "No", "No internet service"], num_rows),
        "TechSupport": np.random.choice(["Yes", "No", "No internet service"], num_rows),
        "StreamingTV": np.random.choice(["Yes", "No", "No internet service"], num_rows),
        "StreamingMovies": np.random.choice(["Yes", "No", "No internet service"], num_rows),
        "Contract": np.random.choice(["Month-to-month", "One year", "Two year"], num_rows),
        "PaperlessBilling": np.random.choice(["Yes", "No"], num_rows),
        "PaymentMethod": np.random.choice(["Electronic check", "Mailed check", "Bank transfer", "Credit card"], num_rows),
        "MonthlyCharges": np.round(np.random.uniform(18.25, 118.75, num_rows), 2),
        "TotalCharges": np.round(np.random.uniform(20.0, 8600.0, num_rows), 2),
        "Churn": np.random.choice(["Yes", "No"], num_rows)
    })
    return synthetic_data


@app.route('/generate_synthetic_data', methods=['GET'])
def get_synthetic_data():
    synthetic_data = generate_synthetic_data()
    return jsonify(synthetic_data.to_dict(orient='records'))


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
