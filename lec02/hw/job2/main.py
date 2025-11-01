from flask import Flask, request, jsonify
import os
import json
from fastavro import writer, parse_schema

app = Flask(__name__)

schema = {
    "type": "record",
    "name": "SalesRecord",
    "fields": [
        {"name": "client", "type": "string"},
        {"name": "purchase_date", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "double"}
    ]
}


parsed_schema = parse_schema(schema)


@app.route("/", methods=["POST"])
def save_sales_to_avro():
    data = request.get_json()

    raw_dir = data.get("raw_dir")
    stg_dir = data.get("stg_dir")

    if not raw_dir or not stg_dir:
        return jsonify({"message": "Missing raw_dir or stg_dir parameter"}), 400

    if not os.path.exists(raw_dir):
        return jsonify({"message": f"Raw directory {raw_dir} does not exist"}), 400

    os.makedirs(stg_dir, exist_ok=True)

    records = []
    json_files = [f for f in os.listdir(raw_dir) if f.endswith(".json")]

    if not json_files:
        return jsonify({"message": "No JSON files found in raw_dir"}), 400

    for filename in json_files:
        file_path = os.path.join(raw_dir, filename)
        try:
            with open(file_path, "r") as f:
                content = json.load(f)
                records.append(content)
        except Exception as e:
            return jsonify({"message": f"Error reading {filename}: {str(e)}"}), 500

    avro_file_path = os.path.join(stg_dir, "sales.avro")
    try:
        with open(avro_file_path, "wb") as out:
            writer(out, parsed_schema, records)
    except Exception as e:
        return jsonify({"message": f"Error writing Avro file: {str(e)}"}), 500

    return jsonify({
        "message": f"Successfully converted {len(records)} JSON files to Avro",
        "output_path": avro_file_path
    }), 201

if __name__ == "__main__":
    # Run the Flask app
    app.run(debug=True, host="localhost", port=8082)