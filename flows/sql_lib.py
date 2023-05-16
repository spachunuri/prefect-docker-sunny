from boto_lib import get_secrets, s3_dump_json
from prefect import variables
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
from datetime import datetime
import requests
import json

environment = variables.get("environment_id")
environment_crosswalk_file = open("include/json/general_config/environment_crosswalk.json", "r")
environment_crosswalk_json = json.loads(environment_crosswalk_file.read())
lake = environment_crosswalk_json[environment]["LAKE"] 
environment_suffix = environment_crosswalk_json[environment]["ENVIRONMENT_SUFFIX"] 

def log_snowflake_oauth_error(tries, exception):
    runexception = "Snowflake oauth token failed on try number {tries} with error {exception}".format(tries=tries, exception=exception)
    audit_file_content = {'environment':environment,
                        'runexception':runexception}
    curr_run_date=str(datetime.now()).replace(" ", "T")
    print('Upload started: {0}'.format(audit_file_content))
    target_file_path = 'snowflake_oauth_error_log/error_log_{0}.json'.format(curr_run_date)
    s3_dump_json(target_file_path=target_file_path, file_content=audit_file_content)


def get_snowflake_conn():
    tries=0
    secrets = json.loads(get_secrets())
    client_id = secrets.get("SNOWFLAKE_CLIENT_ID")
    client_secret = secrets.get("SNOWFLAKE_CLIENT_SECRET")
    token_url = secrets.get("SNOWFLAKE_TOKEN_URL")
    user = secrets.get("SNOWFLAKE_USER")
    password = secrets.get("SNOWFLAKE_PASSWORD")
    while True:
        tries+=1
        try:
            #define the request body
            data = {'grant_type': 'password','username': user, 'password': password}

            #post the request
            access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
            #print(access_token_response)

            #convert the response to json
            tokens = json.loads(access_token_response.text)

            #define the snowflake account and pass the access token
            account="endpoint"
            token=tokens['access_token']

            #create the snowflake connection using the connector and defined credentials
            conn = snowflake.connector.connect(
                            user=user,
                            account=account,
                            authenticator="oauth",
                            token=token,
                            client_session_keep_alive=True,
                            max_connection_pool=20
                            )
        except Exception as exception:
            log_snowflake_oauth_error(tries, exception)
            if tries<20:
                print("Attempting try number {tries}. Error was: {exception}".format(tries=tries, exception=exception))
                continue
            else:
                break
        else:
            break
    return conn

def run_sql_file(filename):
    filepath = "include/sql/{filename}.sql".format(filename=filename)
    file = open(filepath)
    snowflake_dml = file.read()
    file.close()

    snowflake_dml_commands = snowflake_dml.split(";")

    for command in snowflake_dml_commands:
        print(command)
        run_sql_command(command)

def run_sql_command(command):
    conn = get_snowflake_conn()
    command = command.format(environment=environment, environment_suffix=environment_suffix, lake=lake)
    #log_command = "INSERT INTO {lake}.AUDITLOG.SQL_LOG (SQL_STATEMENT, RUN_DATETIME, ROWS_AFFECTED) VALUES ('{command}', '{run_datetime}', {rows_affected})"
    try:
        cur = conn.cursor()
        cur.execute(command)
        rows_affected = cur.rowcount
        if rows_affected == None:
            rows_affected = 0
        print("Rows affected: {0}".format(rows_affected))
        run_datetime = datetime.now()
        #log_command = log_command.format(lake=lake, command=command.replace("'", "''"), run_datetime=run_datetime, rows_affected=rows_affected)
        #print("Logging: {}".format(log_command))
        #cur.execute(log_command)
    finally:
        cur.close()
    cur.close()

def select_value(filename):
    filepath = "include/sql/{filename}.sql".format(filename=filename)
    file = open(filepath)
    command = file.read()
    command = command.format(environment=environment, environment_suffix=environment_suffix, lake=lake)
    file.close()
    print(command)

    conn = get_snowflake_conn()

    try:
        cur = conn.cursor()
        sql_response = cur.execute(command).fetchone()[0]
    finally:
        cur.close()
    cur.close()
    return sql_response    

def select_listing(source):
    filepath = "include/sql/{source}_list.sql".format(source=source)
    file = open(filepath)
    command = file.read()
    command = command.format(environment=environment, environment_suffix=environment_suffix, lake=lake)
    file.close()
    print(command)

    conn = get_snowflake_conn()

    try:
        cur = conn.cursor()
        sql_response = cur.execute(command).fetchall()
    finally:
        cur.close()
    cur.close()
    return sql_response


def select_listing_command(command):
    conn = get_snowflake_conn()

    try:
        cur = conn.cursor()
        sql_response = cur.execute(command).fetchall()
    finally:
        cur.close()
    cur.close()
    return sql_response

def write_to_table(source_df, target_table):
    conn = get_snowflake_conn()

    try:
        result = source_df.to_sql(name=target_table.lower(), con=conn, if_exists="append", method=pd_writer)
    
    except Exception as exception:
        print("Error: {exception}".format(exception=exception))
    return result


