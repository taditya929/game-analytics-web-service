from flask import Flask, request, jsonify, render_template, Response
import pandas as pd
from pymongo import MongoClient
import io

app = Flask(__name__)

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
db = client['game_analytics']  # Replace 'game_analytics' with your database name
collection = db['csv_data']  # Collection to store CSV data


# Endpoint to render the front-end page
@app.route('/')
def index():
    return render_template('home.html')


# Endpoint to upload CSV data to MongoDB
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    try:
        # Check if the POST request has the file part
        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request"}), 400

        file = request.files['file']

        # If user does not select file, browser also submit an empty part without filename
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400

        if file:
            # Read CSV file into DataFrame
            df = pd.read_csv(file)

            # Convert DataFrame to JSON for MongoDB storage
            data_json = df.to_dict(orient='records')

            # Insert data into MongoDB collection
            result = collection.insert_many(data_json)

            return jsonify({"message": f"CSV uploaded and {len(result.inserted_ids)} records stored in MongoDB."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Endpoint to fetch CSV headers
@app.route('/get_csv_headers')
def get_csv_headers():
    try:
        # Fetch headers from MongoDB collection
        headers = collection.find_one()
        if headers:
            headers = list(headers.keys())  # Assuming headers are keys of the first document
            return jsonify(headers), 200
        else:
            return jsonify({"error": "No headers found in CSV data"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Endpoint to query data from MongoDB based on selected columns
@app.route('/query_data', methods=['POST'])
def query_data():
    try:
        # Read selected headers from request payload
        selected_columns = request.json.get('selected_columns')

        if not selected_columns:
            return jsonify({"error": "No columns selected for query"}), 400

        # Fetch data from MongoDB collection based on selected columns
        data = list(collection.find({}, {col: 1 for col in selected_columns}))

        if not data:
            return jsonify({"error": "No data found"}), 404

        # Convert data to CSV format
        csv_data = pd.DataFrame(data).to_csv(index=False)

        # Return CSV response
        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename=query_results.csv"}
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
