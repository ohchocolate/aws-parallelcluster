# uncomment the following line in the lambda console to run tests

# import sys
# import subprocess
#
# subprocess.call('pip install opensearch-py -t /tmp/ --no-cache-dir'.split(),
#                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
# sys.path.insert(1, '/tmp/')
# from opensearchpy import OpenSearch, RequestsHttpConnection

import base64
import datetime
import json
import logging
import math
import os
import re
import zlib

import requests
from botocore.auth import SigV4Auth
import botocore.session
from botocore.awsrequest import AWSRequest

logger = logging.getLogger()
logger.setLevel(logging.INFO)
session = botocore.session.Session()
sigv4 = SigV4Auth(session.get_credentials(), "es", "us-east-2")

# ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"]

# original end point
ENDPOINT = 'https://search-mylogs-kidfhbnbletp4ybierlou2llq4.us-east-2.es.amazonaws.com'

LOG_FAILED_RESPONSES = False


def lambda_handler(event, context):
    """
    A function that processes CloudWatch log events and forwards them to an OpenSearch domain
    :param event: The CloudWatch log event.
    :param context: The AWS Lambda context object.
    :return: A response object with 'statusCode' and 'body' fields.
    """
    try:
        compressed_payload = base64.b64decode(event['awslogs']['data'])
        uncompressed_payload = zlib.decompress(compressed_payload, 16 + zlib.MAX_WBITS)
        payload = json.loads(uncompressed_payload)
        bulk_data = transform(payload)

        if not bulk_data:
            logger.info('Received a control message')
            return {'statusCode': 200, 'body': 'Control message handled successfully'}

        response = post(bulk_data)
        logging.info(f"OpenSearch response status: {response.status_code}, content: {response.json()}")

        if response.status_code == 200:
            logger.info('Success:', response.json())
            return {'statusCode': 200, 'body': 'Success'}
        else:
            if LOG_FAILED_RESPONSES:
                logger.error('Error:', response.json())
            return {'statusCode': 500, 'body': 'Failed to post to OpenSearch'}
    except Exception as e:
        logger.error(f"Error processing lambda: {e}")
        return {'statusCode': 500, 'body': str(e)}


def build_source(message, extracted_fields):
    """
    Construct the source object for OpenSearch based on the log message
    and any extracted fields.
    :param message: The log message.
    :param extracted_fields: Fields extracted from the log message.
    :return: The constructed source object.
    """

    if extracted_fields:
        source = {}
        for key in extracted_fields:
            if extracted_fields.hasOwnProperty(key) and extracted_fields[key]:
                value = extracted_fields[key]

                if math.isnan(value):
                    source[key] = 1 * value
                    continue

            json_substring = extract_json(value)
            if json_substring:
                source["$" + key] = json.loads(json_substring)

            source[key] = value
        return source
    if is_valid_json(message):
        json_substring = extract_json(message)
        if json_substring:
            return json.loads(json_substring)
    return {}


def extract_json(message):
    """
    Extract a JSON substring from a given message.
    :param message: The message string containing potential JSON data.
    :return: The extracted JSON substring or None if not found.
    """
    json_start = message.index("{")
    if json_start < 0:
        return None
    json_substring = message[json_start:]
    if is_valid_json(json_substring):
        return json_substring
    return None


def is_valid_json(message):
    """
    Check if the given message is a valid JSON string.
    :param message: The message string to check.
    :return: True if valid JSON, False otherwise.
    """
    try:
        json.loads(message)
        return True
    except json.JSONDecodeError:
        return False


def transform(payload):
    """
    Transform the CloudWatch log payload into a format suitable for
    bulk indexing in OpenSearch.
    :param payload: The CloudWatch log payload.
    :return: The transformed data in bulk request format.
    """
    if payload["messageType"] == "CONTROL_MESSAGE":
        return None
    bulk_request_body = ""
    for log_event in payload["logEvents"]:
        # Build a ISO 8610 timestamp
        timestamp = datetime.datetime.fromtimestamp(log_event["timestamp"] / 1000.0).isoformat() + 'Z'
        # logger timestamp to debug
        logger.info(f"Timestamp for log event {log_event['id']}: {timestamp}")
        index_name = timestamp[:10].split("-")
        index_name = "cwl-" + ".".join(index_name)

        source = build_source(log_event["message"], log_event.get("extractedFields", None))
        source["id"] = log_event["id"]
        source["timestamp"] = timestamp
        source["message"] = log_event["message"]
        source["owner"] = payload["owner"]
        source["log_group"] = payload["logGroup"]
        source["log_stream"] = payload["logStream"]

        action = {"index": {}}
        action["index"]["_index"] = index_name
        action["index"]["_id"] = log_event["id"]

        bulk_request_body += json.dumps(action) + "\n" + json.dumps(source) + "\n"

    return bulk_request_body


def make_request(method, endpoint, data=None):
    """
    Make a signed request to the OpenSearch domain.
    :param method: The HTTP method.
    :param endpoint: The OpenSearch endpoint URL.
    :param data: The request payload.
    :return: The response object from the OpenSearch request.
    """

    logger.info(f"Making {method} request to {endpoint} with data: {data}")
    headers = {"Content-Type": "application/json"}

    request = AWSRequest(method=method, url=endpoint, data=data, headers=headers)
    request.context["payload_signing_enabled"] = True

    logger.info("Adding SIGV4 auth to the request...")
    sigv4.add_auth(request)

    prepped = request.prepare()

    logger.info(f"Sending request with URL: {prepped.url}, Headers: {prepped.headers}, Body: {prepped.body}")
    response = requests.request(method, prepped.url, data=prepped.body, headers=prepped.headers, timeout=200)

    logger.info(f"Received response with status code: {response.status_code}, content: {response.text}")
    return response


def post(body):
    """
    Post the transformed log data to the OpenSearch domain.
    :param body: The bulk data to post to OpenSearch.
    :return: The response object from the OpenSearch post request.
    """
    logger.info(f"Posting data: {body}")
    endpoint_for_post = ENDPOINT + "/_bulk"
    response = make_request("POST", endpoint_for_post, body)
    return response


