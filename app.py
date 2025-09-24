from typing import List, Dict, Union, Optional
from enum import Enum

from fastapi import FastAPI
from pydantic import BaseModel, Field
from google.cloud import bigquery
from google.oauth2 import service_account

from jobs import update, load
from meta_marketing import MetaClient

app = FastAPI()

@app.get('/')
def home():
    return {'message': 'Service is running'}

class UpdateRequest(BaseModel):
    ad_account_ids: Union[List[str], str]
    meta_token: str
    bq_project_id: str
    bq_dataset: str
    service_account_creds: Dict

@app.post('/update')
def update(req: UpdateRequest):
    meta_client = MetaClient(token=req.meta_token)
    bq_creds = service_account.Credentials.from_service_account_info(req.service_account_creds)
    bq_client = bigquery.Client(credentials=bq_creds)
    update(
        ad_account_ids=req.ad_account_ids,
        meta_client=meta_client,
        bq_client=bq_client,
        bq_project_id=req.bq_project_id,
        bq_dataset=req.bq_dataset
    )
    return {'message': f'Data updated in {req.bq_dataset} dataset successfully.'}

class WriteMode(str, Enum):
    append = 'append'
    truncate = 'truncate'

class LoadRequest(BaseModel):
    ad_account_ids: List[str] | str
    meta_token: str
    bq_project_id: str
    bq_dataset: str
    service_account_creds: Dict
    write_mode: Optional[WriteMode] = Field(
        'truncate',
        description="Write mode in BigQuery. If not specified, data will be truncated.",
        example='truncate'
    )
    start: str = Field(
        description="Start date in format YYYY-MM-DD", 
        example="2025-01-01"
    )

@app.post('/load')
def load(req: LoadRequest):
    meta_client = MetaClient(token=req.meta_token)
    bq_creds = service_account.Credentials.from_service_account_info(req.service_account_creds)
    bq_client = bigquery.Client(credentials=bq_creds)
    load(
        ad_account_ids=req.ad_account_ids,
        meta_client=meta_client,
        bq_client=bq_client,
        bq_project_id=req.bq_project_id,
        bq_dataset=req.bq_dataset,
        start=req.start,
        write_mode = req.write_mode
    )
    return {'message': 'Job executed successfully'}
