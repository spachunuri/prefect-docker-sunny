from prefect import flow, get_run_logger
import json

@flow(name="Json read")
def json_read():
    logger = open('json/simplesat_config.json', 'r')
    data = json.load(logger)
    print(data)
if __name__ == "__main__":
    json_read()
