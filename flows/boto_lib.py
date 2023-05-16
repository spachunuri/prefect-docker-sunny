from prefect.blocks.system import Secret
from prefect import variables
import boto3
import botocore
import json

environment = variables.get("environment_id")
environment_crosswalk_file = open("include/json/general_config/environment_crosswalk.json", "r")
environment_crosswalk_json = json.loads(environment_crosswalk_file.read())
aws_access_key_id = environment_crosswalk_json[environment]["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = Secret.load("aws-secret-access-key").get()
s3_bucket = environment_crosswalk_json[environment]["S3_BUCKET"]
SecretId=environment_crosswalk_json[environment]["SECRET_ARN"]

def get_secrets():
    secretsmanager = boto3.client("secretsmanager",
                                    aws_access_key_id=aws_access_key_id, 
                                    aws_secret_access_key=aws_secret_access_key,
                                    region_name = "us-west-2")
    
    secrets_response = secretsmanager.get_secret_value(SecretId=SecretId)
    secrets = secrets_response["SecretString"]
    return secrets

def s3_dump_json(target_file_path, file_content):
    s3 = boto3.resource("s3",
                        aws_access_key_id=aws_access_key_id, 
                        aws_secret_access_key=aws_secret_access_key,
                        region_name = "us-west-2")
    try:
        s3object = s3.Object(s3_bucket, target_file_path)
        s3object.put(
            Body=(bytes(json.dumps(file_content).encode('UTF-8')))
        )
    except botocore.exceptions.ClientError as e:
        raise SystemExit(e)
