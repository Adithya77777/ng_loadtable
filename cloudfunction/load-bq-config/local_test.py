from flask import Flask, request, jsonify
from main import read_streams_config

app = Flask(__name__)

@app.route("/", methods=["POST"])
def test_entrypoint():
    # Forward the request to your cloud function handler
    return read_streams_config(request)

if __name__ == "__main__":
    app.run(debug=True, port=8080)