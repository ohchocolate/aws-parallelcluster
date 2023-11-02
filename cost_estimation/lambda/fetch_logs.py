# import sys
# import subprocess
#
# subprocess.call('pip install opensearch-py -t /tmp/ --no-cache-dir'.split(),
#                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
# sys.path.insert(1, '/tmp/')
# from opensearchpy import OpenSearch, RequestsHttpConnection

import json
import logging
from datetime import datetime

import botocore.session
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# Build a logger for debug
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ENDPOINT = 'https://search-mylogs-kidfhbnbletp4ybierlou2llq4.us-east-2.es.amazonaws.com'


def lambda_handler(event, context):
    try:
        # TODO: CHECK IF THE DATA IS PROCESSED
        # dict: {'statusCode': int, 'body': str}
        jobs_log = search_data(ENDPOINT, "scontrol-show-job-information")
        jobs_detail = extract_data_from_response(jobs_log['body'])
        logger.info(f"The extracted_data_list of jobs log {jobs_detail}")
        nodes_log = search_data(ENDPOINT, "node-instance-mapping-event")
        nodes_detail = extract_data_from_response(nodes_log['body'])
        logger.info(f"The extracted_data_list of nodes log {nodes_detail}")
        # Calculate costs if there are jobs and nodes details
        if jobs_detail and nodes_detail:
            jobs_cost = calculate_cost(nodes_detail, jobs_detail)
            logger.info(f"The job cost: {jobs_cost}")

            job_detail_with_cost = []
            for job in jobs_detail:
                combined_id = job["combined_id"]
                job["estimated_cost"] = jobs_cost.get(combined_id, 0)
                job_detail_with_cost.append(job)

            # Push costs to OpenSearch
            response = post_estimated_cost_to_opensearch(ENDPOINT, job_detail_with_cost)
            return {
                'statusCode': 200,
                'body': 'Successfully processed jobs and updated estimated costs in OpenSearch.'
            }
        else:
            # Log message and return early if no data to process
            logger.info("No jobs or nodes detail to process.")
            return {
                'statusCode': 200,
                'body': 'No jobs or nodes detail to process.'
            }

    except Exception as e:
        logger.error(f"Error processing the Lambda: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error processing the Lambda: {str(e)}"
        }


def calculate_runtime_in_minutes(run_time):
    # Given a time format as HH:MM:SS, compute total minutes.
    hours, minutes, seconds = map(int, run_time.split(':'))
    return hours * 60 + minutes + seconds / 60


def extract_data_from_response(response_body):
    logger.info("start extracting...")
    parsed_response = json.loads(response_body)
    hits = parsed_response.get('hits', {}).get('hits', [])
    extracted_data_list = []
    instance_ids = set()
    job_status_dict = {}

    for hit in hits:
        if "processed" in hit["_source"]:
            # Skip the log if it is already processed
            continue
        if "detail" in hit["_source"]:
            detail = hit["_source"]["detail"]
            cluster = hit["_source"]["cluster-name"]

            if "job_id" in detail:
                combined_id = f"{cluster}_{detail['job_id']}"
                # Get the current job state and runtime
                cur_job_state = detail.get('job_state')
                raw_cur_runtime = detail.get('runtime')
                cur_runtime = calculate_runtime_in_minutes(raw_cur_runtime) if raw_cur_runtime else None

                # Update the dictionary of job state and run time
                if combined_id in job_status_dict:
                    prev_job_state = job_status_dict[combined_id].get("state")
                    prev_runtime = job_status_dict[combined_id].get("runtime")
                    if prev_job_state == "RUNNING" and cur_job_state == "RUNNING":
                        if cur_runtime is not None and (prev_runtime is None or cur_runtime <= prev_runtime):
                            continue
                    elif prev_job_state == "COMPLETED":
                        continue

                job_status_dict[combined_id] = {
                    "state": cur_job_state,
                    "runtime": cur_runtime
                }
                logger.info(f"The current job_status_dict is {job_status_dict}")
                extracted_data = {
                    "cluster_name": cluster,
                    "combined_id": combined_id,
                    "detail": detail
                }
                extracted_data_list.append(extracted_data)
            else:
                # Based on the simplified assumption, the node mapping will not change once form
                node_list = detail.get("node_list", [])
                new_node_list = []
                for node in node_list:
                    instance_id = node.get("instance_id")
                    if instance_id and instance_id not in instance_ids:
                        instance_ids.add(instance_id)
                        new_node_list.append(node)
                if new_node_list:
                    extracted_data = {
                        "cluster_name": cluster,
                        "detail": {
                            "node_list": new_node_list
                        }
                    }
                    extracted_data_list.append(extracted_data)

    logger.info("Extracted data: %s", extracted_data_list)
    return extracted_data_list


def get_instance_cost(instance_type):
    # Reference: https://aws.amazon.com/ec2/instance-types/t2/
    # In the real scenario, we may need to call an API
    costs = {
        # "t2.nano": 0.01,
        # "t2.micro": 0.01,
        # "t2.small": 0.02,
        # "t2.medium": 0.05,
        # "t2.large": 0.09,
        # "t2.xlarge": 0.19,
        # "t2.2xlarge": 0.37
        "t2.nano": 1,
        "t2.micro": 1,
        "t2.small": 2,
        "t2.medium": 5,
        "t2.large": 9,
        "t2.xlarge": 19,
        "t2.2xlarge": 37
    }
    return costs.get(instance_type, 0)


def get_total_cores(cpu_ids):
    total_cores = 0
    for cpu_id in cpu_ids:
        if "-" in cpu_id:
            start, end = cpu_id.split("-")
            if start.isdigit() and end.isdigit():
                total_cores += int(end) - int(start) + 1
        elif cpu_id.isdigit():
            total_cores += 1
    return total_cores


def calculate_cost(nodes_detail, jobs_detail):
    logger.info("Enter into the cost calculation")
    total_costs = {}
    logger.info(f"Current nodes detail: {nodes_detail}")
    for job_data in jobs_detail:
        job = job_data["detail"]
        combined_id = job_data["combined_id"]
        logger.info(f"Processing job: {job}")
        # Consider job_state: completed and running
        if job.get("job_state") != "RUNNING" and job.get("job_state") != "COMPLETED":
            continue

        total_cost_for_job = 0

        job_runtime_minutes = calculate_runtime_in_minutes(job["run_time"])

        current_cluster = job_data["cluster_name"]

        cluster_nodes_detail = None
        for node_cluster in nodes_detail:
            if node_cluster["cluster_name"] == current_cluster:
                cluster_nodes_detail = node_cluster["detail"]["node_list"]
                break

        if not cluster_nodes_detail:
            logger.error(f"Could not find node details for cluster: {current_cluster}")
            continue
        logger.info(f"This is current cluster node detail: {cluster_nodes_detail}")
        for node_name, cpu_ids in zip(job["nodes"], job["cpu_ids"]):
            logger.info(f"Processing node name {node_name}")

            node_detail = next((node for node in cluster_nodes_detail if node["node_name"] == node_name), None)
            logger.info(f"This is current node detail {node_detail}")
            if not node_detail:
                logger.error(f"Could not find node details for node name: {node_name}")
                continue

            # Calculate total vCPUs for the node
            total_vcpus = node_detail["threads_per_core"] * node_detail["core_count"]
            if total_vcpus == 0:
                continue

            # Calculate the number of used CPUs for the job on that node
            used_cores = get_total_cores(cpu_ids)
            logger.info(f"This is the used CPUs {used_cores} for node {node_name}")

            # Determine the CPU usage ratio for the job on that node
            cpu_usage_ratio = used_cores / total_vcpus
            logger.info(f"This is the cpu usage ratio {cpu_usage_ratio}")

            # Fetch the cost per hour for that instance_type
            cost_per_hour = get_instance_cost(node_detail["instance_type"])

            # Calculate the cost per minute and then for the job's runtime
            cost_per_minute = cost_per_hour / 60
            cost_for_node = cost_per_minute * job_runtime_minutes * cpu_usage_ratio
            logger.info(f"This is the cost {cost_for_node} for node {node_name}")

            total_cost_for_job += cost_for_node
            # Round cost
            rounded_cost_for_job = round(total_cost_for_job, 4)
            logger.info(f"This is the total cost for job {combined_id}: {total_cost_for_job}")
            logger.info(f"This is the rounded total cost for job {combined_id}: {total_cost_for_job}")
        # cluster name + job id as the key
        total_costs[combined_id] = rounded_cost_for_job
        logger.info(f"Current total costs is {total_costs}")

    return total_costs


def post_estimated_cost_to_opensearch(endpoint, job_detail_with_cost):
    """
    Push or update calculated cost and related information for a specific job to OpenSearch
    :param endpoint: OpenSearch endpoint
    :param job_detail_with_cost: A list containing job details and its estimated cost
    :return:
    """
    session = botocore.session.Session()
    sigv4 = SigV4Auth(session.get_credentials(), "es", "us-east-2")
    # Remember to use bulk API
    path = "/estimated_cost/_bulk/"
    url = endpoint + path

    headers = {
        "Content-Type": "application/x-ndjson"
    }

    # Preparing bulk data
    bulk_data = ''
    for job in job_detail_with_cost:
        # Add timestamp to the index
        job["timestamp"] = datetime.utcnow().isoformat() + "Z"
        # Use combined_id as document ID and specify update operation
        action_metadata = {"update": {"_id": job["combined_id"]}}
        update_data = {"doc": job, "doc_as_upsert": True}
        bulk_data += json.dumps(action_metadata) + '\n' + json.dumps(update_data) + '\n'
    request = AWSRequest(method="POST", url=url, data=bulk_data, headers=headers)
    sigv4.add_auth(request)
    prepped_request = request.prepare()

    # Send the request
    try:
        response = requests.post(url, headers=prepped_request.headers, data=prepped_request.body)
        logger.info(f"Received response: {response.status_code} {response.text}")
        response.raise_for_status()

        # Mark the documents as processed only if successfully added to OpenSearch
        mark_documents_as_processed(ENDPOINT, "scontrol-show-job-information")
        mark_documents_as_processed(ENDPOINT, "node-instance-mapping-event")
        logger.info("Mark documents as processed")

        logger.info("Successfully added the estimated cost")
        return {
            'statusCode': 200,
            'body': json.dumps(response.json())
        }
    except requests.HTTPError as e:
        logger.error("Failed to add the estimated cost")
        return {
            'statusCode': e.response.status_code,
            'body': f"Failed to add the estimated cost: {e.response.status_code} {e.response.text}"
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
        # logger.info("See the response below")
        # print(results)
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
