# import sys
# import subprocess
#
# subprocess.call('pip install opensearch-py -t /tmp/ --no-cache-dir'.split(),
#                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
# sys.path.insert(1, '/tmp/')
# from opensearchpy import OpenSearch, RequestsHttpConnection

import json
import logging
import botocore.session
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest


# Build a logger for debug
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ENDPOINT = 'https://search-mylogs-kidfhbnbletp4ybierlou2llq4.us-east-2.es.amazonaws.com'


def lambda_handler(event, context):
    print("Lambda function triggered!")

    event_type = event.get("event_type", "scontrol-show-job-information")

    result = search_data(ENDPOINT, event_type)

    return result


def build_query(event_type):
    return {
        "size": 5,
        "query": {
            "match": {
                "event-type": event_type
            }
        },
        "sort": [
            {
                "timestamp": {
                    "order": "desc"
                }
            }
        ]
    }


def search_data(endpoint, event_type):
    session = botocore.session.Session()
    sigv4 = SigV4Auth(session.get_credentials(), "es", "us-east-2")
    # searching in OpenSearch for indices matching the pattern "cwl-*"
    path = "/cwl-*/_search"
    url = endpoint + path
    query = build_query(event_type)
    headers = {"Content-Type": "application/json"}
    request = AWSRequest(method="GET", url=url, data=json.dumps(query), headers=headers)
    sigv4.add_auth(request)
    prepped_request = request.prepare()

    # Send the request and handle response
    try:
        response = requests.get(url, headers=prepped_request.headers, data=prepped_request.body)
        response.raise_for_status()
        logger.info("Query succeeded")
        results = response.json()
        logger.info("See the response below")
        print(results)
        return {
            'statusCode': 200,
            'body': json.dumps(response.json())
        }
    except requests.HTTPError as e:
        logger.error("Query failed")
        return {
            'statusCode': e.response.status_code,
            'body': f"Query failed: {e.response.text}"
        }
