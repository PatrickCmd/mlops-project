import os

from flask import Flask, jsonify, request
from dotenv import load_dotenv
from ride_duration.predict import load_model, make_prediction

load_dotenv()


# Load model with run ID and experiment ID defined in the env.
RUN_ID = os.getenv("RUN_ID")
EXPERIMENT_ID = os.getenv("EXPERIMENT_ID")
model = load_model(run_id=RUN_ID, experiment_id=EXPERIMENT_ID)

app = Flask("duration-prediction")


@app.route("/predict", methods=["POST"])
def predict_endpoint():
    """Predict duration of a single ride using CITIBIKESDurationModel."""

    ride = request.get_json()
    preds = make_prediction(model, [ride])

    return jsonify(
        {
            "duration": float(preds[0]),
            "model_version": RUN_ID,
        }
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=9696)
