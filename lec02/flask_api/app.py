from flask import Flask, jsonify
from service import get_fake_ride_data, get_all_users, get_riders, get_drivers

app = Flask(__name__)


@app.route('/')
def index():
    docs = """
    <html>
      <head>
        <title>Flask API Documentation</title>
      </head>
      <body>
        <h1>Flask API Documentation</h1>
        <p>Welcome to the Flask API. Below is a list of available endpoints:</p>
        <ul>
          <li><strong>GET /</strong>: API Documentation (this page)</li>
          <li><strong>GET /ride</strong>: Returns a fake ride along with its driver and rider data.</li>
          <li><strong>GET /users</strong>: Returns a combined list of riders and drivers (as user records).</li>
          <li><strong>GET /riders</strong>: Returns the list of riders.</li>
          <li><strong>GET /drivers</strong>: Returns the list of drivers.</li>
        </ul>
        <p>Use these endpoints to interact with the API.</p>
      </body>
    </html>
    """
    return docs

@app.route('/ride', methods=['GET'])
def ride():
    """Returns a fake ride along with its driver and rider data."""
    data = get_fake_ride_data()
    return jsonify(data)

@app.route('/users', methods=['GET'])
def users():
    """Returns the combined list of riders and drivers (as user records)."""
    return jsonify(get_all_users())

@app.route('/riders', methods=['GET'])
def riders():
    """Returns the list of riders."""
    return jsonify(get_riders())

@app.route('/drivers', methods=['GET'])
def drivers():
    """Returns the list of drivers."""
    return jsonify(get_drivers())

if __name__ == '__main__':
    # Listen on all interfaces so that the container is reachable.
    app.run(host='0.0.0.0', port=8081)
