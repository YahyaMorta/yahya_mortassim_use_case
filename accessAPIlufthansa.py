from flask import Flask, jsonify, request
import requests
#from utils.secret import *

app = Flask(__name__)

def get_access_token():
    url_token = 'https://api.lufthansa.com/v1/oauth/token'
    payload = {
        "grant_type": "client_credentials",
        "client_id": '9s9cp7yb4q9325r23wqybb65y',
        "client_secret": 'QT6ExtMuNy'
    }
    response = requests.post(url_token, data=payload)

    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        return jsonify({"error": "Impossible d'obtenir le token d'accès"}), 500

# Récupérer toutes les informations sur les pays

@app.route('/<info>', methods=['GET'])
def get_filght_data(info):
    access_token = get_access_token()
    url = f"https://api.lufthansa.com/v1/mds-references/{info}"
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return jsonify(data)
    else:
        return jsonify({"error": "Failed to fetch data"}), 500
if __name__ == '__main__':
    app.run(debug=True)
