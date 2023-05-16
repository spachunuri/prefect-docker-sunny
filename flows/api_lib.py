from datetime import datetime
from jinja2 import Template
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer
import requests
from requests.auth import HTTPBasicAuth
from prefect import variables
from sql_lib import select_value, select_listing, write_to_table
from boto_lib import get_secrets
import json
from time import sleep

ENVIRONMENT_NAME = variables.get("ENVIRONMENT_NAME")
secrets = json.loads(get_secrets())


def download_data(source, source_system):
    config_file = open("include/json/rest_json_to_s3/{source_system}_{source}_config.json".format(source_system=source_system, source=source), "r")
    config_template = Template(config_file.read())
    config_file.close()
    api_key = secrets.get("{source_system}_APIKEY_VALUE".format(source_system=source_system).upper())

    start_date = select_value("watermark")
    print("Incremental start date used: {start_date}".format(start_date=start_date))
    config_data = config_template.render(api_key = api_key, start_date = start_date)
    print("Template rendered: {config_data}".format(config_data=config_data))
    config_json = json.loads(config_data)
    print("JSON parsed succesfully")
    base_url = config_json["url"]
    headers = config_json["headers"]
    auth_basic_username = config_json["auth_basic_username"]
    auth_basic_password = config_json["auth_basic_password"]
    params = config_json["params"]
    pagination_next = config_json["pagination_next"]
    data_lookup_key = config_json["data_lookup_key"]
    watermark_sql_template = config_json["watermark_sql_template"]
    sleep_interval = config_json["sleep_interval"]
    sleep_duration = config_json["sleep_duration"]

    if pagination_next.isnumeric():
        page_counter = int(pagination_next)
        url = base_url.format(page_counter=str(page_counter))
    else:
        page_counter = 0
        if pagination_next == "sql":
            listing = select_listing(source)
            listing_rows = len(listing)
            if listing_rows == 0:
                url = None
                source_max_date = None
            else:
                df_listing = pd.DataFrame(listing)
                source_max_date = df_listing[0].max()
                print("{listing_rows} rows will be processed.".format(listing_rows=listing_rows))
                url = base_url.format(id=listing[0][1])
        else:
            url = base_url

    while url != None:


        # Get response data
        try:
            if auth_basic_username == "":
                print("Using oauth in header")
                response = requests.get(url, headers=headers, params=params)
            else:
                response = requests.get(url, headers=headers, auth=HTTPBasicAuth(auth_basic_username,auth_basic_password), params=params)
        except requests.exceptions.RequestException as e: 
            raise SystemExit(e)
        print('Got response: {0}'.format(url))
        # Convert data to JSON
        print(response)
        if response.status_code != 200: 
            raise Exception("API responded with error code: {status}\nError reason: {reason}".format(status=response.status_code,reason=response.reason))
        else: 
            json_response = response.json()

            # Concatenate current dataframed response to existing responses from previous pages
            if data_lookup_key != "":
                df_response = pd.DataFrame(json_response[data_lookup_key]) 
                print(df_response)
                column_list = df_response.columns
                print(column_list)
            else:
                df_response = pd.DataFrame(json_response)

            
        page_counter = page_counter + 1
        # Pagination - assign next url from json response to next run url or increment page counter
        if page_counter % sleep_interval == 0:
            sleep(sleep_duration)
            #print("sleeping {sleep_duration} seconds after {page_counter} pages.".format(sleep_duration=sleep_duration,page_counter=page_counter))

        if pagination_next.split("|")[0] in json_response:  
            next_url = json_response
            for pagination_child in pagination_next.split("|"):
                next_url = next_url[pagination_child]
            if next_url == url:
                url = None
            else:
                url = next_url
        elif pagination_next.isnumeric() and json_response != [] and df_response.empty == False:
            url = base_url.format(page_counter=str(page_counter))
        elif pagination_next == "sql" and page_counter < listing_rows:
            url = base_url.format(id=listing[page_counter][1])
        else:
            url = None
        #if url != None and page_counter % pages_per_upload == 0:
            #df.reset_index(inplace=True)    
            #print("Uploading set {set_number} with {pages_per_upload} pages of data.".format(set_number=str(page_counter/pages_per_upload), pages_per_upload=pages_per_upload))    
            #target_file_name = s3_upload(source, source_system, df)
        #write_to_table(df_response, "{lake}.SIMPLESAT.SIMPLESAT_FLATTENED")
        print('{0} is next'.format(url))


 