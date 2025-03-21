from jobs import bq_service_account_auth, update, load
from meta_marketing import MetaClient
from flask import Flask, request
from google.cloud import bigquery
import os

app = Flask(__name__)

@app.route('/')
def home():
    return {'message': 'Service is running'}, 200

# Local update enpoint
@app.route('/update/local', methods=['POST'])
def local_update():
    data = request.get_json()
    ad_account_ids = data.get('ad_account_ids')
    meta_token = data.get('meta_token')
    bq_project_id = data.get('bq_project_id')
    bq_dataset = data.get('bq_dataset')
    credentials = data.get('credentials')
    # Credentials
    meta_client = MetaClient(token=meta_token)
    bq_client = bq_service_account_auth(credentials=credentials)
    # Update
    update(
        ad_account_ids=ad_account_ids,
        meta_client=meta_client,
        bq_client=bq_client,
        bq_project_id=bq_project_id,
        bq_dataset=bq_dataset
    )
    return {'message': 'Job executed successfully'}, 200

# Update endpoint
@app.route('/update', methods=['POST'])
def cloud_update():
    data = request.get_json()
    ad_account_ids = data.get('ad_account_ids')
    meta_token = data.get('meta_token')
    bq_project_id = data.get('bq_project_id')
    bq_dataset = data.get('bq_dataset')
    # Credentials
    meta_client = MetaClient(token=meta_token)
    bq_client = bigquery.Client()
    # Update
    update(
        ad_account_ids=ad_account_ids,
        meta_client=meta_client,
        bq_client=bq_client,
        bq_project_id=bq_project_id,
        bq_dataset=bq_dataset
    )
    return {'message': 'Job executed successfully'}, 200

# Local loading data endpoint
@app.route('/load/local', methods=['POST'])
def local_load():
    data = request.get_json()
    ad_account_ids = data.get('ad_account_ids')
    meta_token = data.get('meta_token')
    bq_project_id = data.get('bq_project_id')
    bq_dataset = data.get('bq_dataset')
    start = data.get('start')
    credentials = data.get('credentials')
    # Credentials
    meta_client = MetaClient(token=meta_token)
    bq_client = bq_service_account_auth(credentials=credentials)
    # Update
    load(
        ad_account_ids=ad_account_ids,
        meta_client=meta_client,
        bq_client=bq_client,
        bq_project_id=bq_project_id,
        bq_dataset=bq_dataset,
        start=start
    )
    return {'message': 'Job executed successfully'}, 200

# Entry point
if __name__=='__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
