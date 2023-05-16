from python.api_lib import download_data
from prefect import flow, task

@task
def download(source):
    download_data(source, source)

@flow
def api_get(source):
    download(source)

api_get("simplesat")