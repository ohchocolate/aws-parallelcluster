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
    # fetch job info
    # a dict: {'statusCode': int, 'body': str}
    jobs_log = search_data(ENDPOINT, "scontrol-show-job-information")
    jobs_detail = extract_data_from_response(jobs_log['body'])
    logger.info("The body/detail of jobs log")
    logger.info(jobs_detail)
    # fetch node mapping
    nodes_log = search_data(ENDPOINT, "node-instance-mapping-event")
    nodes_detail = extract_data_from_response(nodes_log['body'])
    logger.info("The body/detail of nodes log")
    logger.info(nodes_detail)
    jobs_cost = calculate_cost(nodes_detail, jobs_detail)
    # jobs cost is a dictionary {job_id : cost}
    logger.info(jobs_cost)

    # push costs to OpenSearch
    # TODO: Get and store the document id for a job
    # add_estimated_cost(ENDPOINT, document_id, calculated_cost)
    # Mark the documents as processed after processing
    mark_documents_as_processed(ENDPOINT, "scontrol-show-job-information")
    mark_documents_as_processed(ENDPOINT, "node-instance-mapping-event")
    return jobs_cost


def extract_data_from_response(response_body):
    parsed_response = json.loads(response_body)
    hits = parsed_response.get('hits', {}).get('hits', [])
    logger.info("What are the hits?")
    logger.info(hits)
    extracted_data = [hit["_source"]["detail"] for hit in hits if "detail" in hit["_source"]]
    logger.info("See the extracted data below")
    logger.info("Extracted data: %s", extracted_data)
    print(extracted_data)
    return extracted_data


def get_instance_cost(instance_type):
    # Dummy data for testing
    # In the real scenario, we may need to call an API
    costs = {
        "t2.micro": 0.02,  # cost per hour in USD
        "t3.large": 0.05
    }
    return costs.get(instance_type, 0)


def calculate_runtime_in_minutes(run_time):
    # Given a time format as HH:MM:SS, compute total minutes.
    hours, minutes, seconds = map(int, run_time.split(':'))
    return hours * 60 + minutes + seconds / 60


def calculate_cost(nodes_data, jobs_data):
    total_costs = {}

    for job in jobs_data:
        if job.get("job_state") != "running":
            continue

        job_id = job["job_id"]
        total_cost_for_job = 0

        # Calculate runtime in minutes for the current job
        job_runtime_minutes = calculate_runtime_in_minutes(job["run_time"])

        for node_name, cpus in zip(job["nodes"], job["cpus"]):
            node_detail = next((node for node in nodes_data if node["node_name"] == node_name), None)

            if not node_detail:
                # This is an error scenario where we couldn't find the node detail.
                continue

            # Calculate total vCPUs for the node
            total_vcpus = node_detail["threads_per_core"] * node_detail["core_count"]
            print(f"This node {node_name} has the total of {total_vcpus} vcpus")

            # Determine the CPU usage ratio for the job on that node
            cpu_usage_ratio = int(cpus) / total_vcpus
            print(f"This job {job_id} has {cpu_usage_ratio} cpu usage ratio of {node_name}")

            # Fetch the cost per hour for that instance_type
            cost_per_hour = get_instance_cost(node_detail["instance_type"])

            # Calculate the cost per minute and then for the job's runtime
            cost_per_minute = cost_per_hour / 60
            cost_for_node = cost_per_minute * job_runtime_minutes * cpu_usage_ratio

            total_cost_for_job += cost_for_node

        total_costs[job_id] = total_cost_for_job

    return total_costs


def add_estimated_cost(endpoint, document_id, calculated_cost):
    """
    Push calculated cost for a specific job to OpenSearch
    :param endpoint:
    :param calculated_cost:
    :return:
    """
    session = botocore.session.Session()
    sigv4 = SigV4Auth(session.get_credentials(), "es", "us-east-2")
    path = f"/your-index-name/_update/{document_id}"
    url = endpoint + path
    # TODO: query to update a field in OpenSearch
    query = {

    }
    headers = {
        "Content-Type": "application/json"
    }
    request = AWSRequest(method="POST", url=url, data=json.dumps(query), headers=headers)
    sigv4.add_auth(request)
    prepped_request = request.prepare()

    # Send the request
    try:
        response = requests.post(url, headers=prepped_request.headers, data=prepped_request.body)
        response.raise_for_status()
        logger.info("Successfully added the estimated cost")
        return {
            'statusCode': 200,
            'body': json.dumps(response.json())
        }
    except requests.HTTPError as e:
        logger.error("Failed to add the estimated cost")
        return {
            'statusCode': e.response.status_code,
            'body': f"Failed to add the estimated cost: {e.response.text}"
        }


def mark_documents_as_processed(endpoint, event_type):
    """
    Marks the documents with the specified event-type as processed in OpenSearch.
    :param endpoint:
    :param event_type:
    :return:
    """
    session = botocore.session.Session()
    sigv4 = SigV4Auth(session.get_credentials(), "es", "us-east-2")
    # Update by query to set processed=true for documents matching the event-type
    path = "/cwl-*/_update_by_query"
    url = endpoint + path
    query = {
        "query": {
            "match": {
                "event-type": event_type
            }
        },
        "script": {
            # ref: https://opensearch.org/docs/1.3/api-reference/document-apis/update-by-query/
            "source": "ctx._source.processed = true"
        }
    }
    headers = {"Content-Type": "application/json"}
    request = AWSRequest(method="POST", url=url, data=json.dumps(query), headers=headers)
    sigv4.add_auth(request)
    prepped_request = request.prepare()

    # Send the request
    try:
        response = requests.post(url, headers=prepped_request.headers, data=prepped_request.body)
        response.raise_for_status()
        logger.info("Documents marked as processed")
        return {
            'statusCode': 200,
            'body': json.dumps(response.json())
        }
    except requests.HTTPError as e:
        logger.error("Failed to mark documents as processed")
        return {
            'statusCode': e.response.status_code,
            'body': f"Failed to mark documents as processed: {e.response.text}"
        }


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
        # convert response to json
        results = response.json()
        logger.info("See the response below")
        print(results)
        return {
            'statusCode': 200,
            'body': json.dumps(results)
        }
    except requests.HTTPError as e:
        logger.error("Query failed")
        return {
            'statusCode': e.response.status_code,
            'body': f"Query failed: {e.response.text}"
        }
