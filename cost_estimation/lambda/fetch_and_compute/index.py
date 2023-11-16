import os
import json
import logging
from datetime import datetime

import boto3
import requests
from requests.auth import HTTPBasicAuth

# Build a logger for debug
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"]
# Original Endpoint
# ENDPOINT = 'https://search-mylogs-kidfhbnbletp4ybierlou2llq4.us-east-2.es.amazonaws.com'


def lambda_handler(event, context):
    """
    Handles the incoming event by fetching job and node details from CloudWatch logs,
    calculating costs, and posting estimated costs to OpenSearch.

    :param event: The event triggering the lambda.
    :param context: Lambda execution context.
    :return: A dictionary with statusCode and body indicating the outcome of the operation.
    """
    try:
        # dict: {'statusCode': int, 'body': str}
        jobs_log = search_data(ENDPOINT, "scontrol-show-job-information")
        jobs_detail = extract_data_from_response(jobs_log['body'])
        logger.info(f"The extracted_data_list of jobs log {jobs_detail}")

        nodes_log = search_data(ENDPOINT, "node-instance-mapping-event")
        nodes_detail = extract_data_from_response(nodes_log['body'])
        logger.info(f"The extracted_data_list of nodes log {nodes_detail}")
        # Calculate costs if there are jobs and nodes details
        if jobs_detail and nodes_detail:
            jobs_cost_dict = calculate_cost(nodes_detail, jobs_detail)
            logger.info(f"The job cost: {jobs_cost_dict}")

            job_detail_with_cost = merge_job_details_with_cost(jobs_detail, jobs_cost_dict)

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


def merge_job_details_with_cost(jobs_detail, jobs_cost_dict):
    """
    Merges job details with their estimated costs.

    :param jobs_detail: A list of dictionaries, each containing details of a job.
    :param jobs_cost_dict: A dictionary mapping job combined ids to their estimated costs.
    :return: A list of job details merged with their estimated costs.
    """
    job_detail_with_cost = []
    for job in jobs_detail:
        combined_id = job["combined_id"]
        job["estimated_cost"] = jobs_cost_dict.get(combined_id, 0)
        job_detail_with_cost.append(job)
    return job_detail_with_cost


def parse_runtime_to_minutes(run_time):
    """
    Parses a runtime string in HH:MM:SS format and converts it to total minutes.

    :param run_time: String representation of runtime in HH:MM:SS format.
    :return: Total runtime in minutes as an integer.
    :raises ValueError: If run_time format is invalid.
    """
    try:
        # Given a time format as HH:MM:SS, compute total minutes.
        hours, minutes, seconds = map(int, run_time.split(':'))
        return hours * 60 + minutes + seconds / 60
    except ValueError as e:
        logger.error(f"Error parsing runtime '{run_time}': {e}")
        raise ValueError(f"Invalid run_time format: {run_time}")


def is_log_processed(hit):
    """
    Checks if a log entry has already been processed.

    :param hit: A dictionary representing a log entry.
    :return: Boolean indicating whether the log entry is marked as processed.
    """
    return "processed" in hit["_source"]


def skip_job_update(prev_job_state, cur_job_state, prev_runtime, cur_runtime):
    """
    Determines whether to skip updating a job based on its current and previous states and runtimes.

    :param prev_job_state: The previous state of the job.
    :param cur_job_state: The current state of the job.
    :param prev_runtime: The previous runtime of the job.
    :param cur_runtime: The current runtime of the job.
    :return: Boolean indicating whether to skip the job update.
    """
    # Only update the dictionary of job status if the job state is running and the job has larger runtime
    if prev_job_state == "RUNNING" and cur_job_state == "RUNNING":
        return cur_runtime is not None and (prev_runtime is None or cur_runtime <= prev_runtime)
    elif prev_job_state == "COMPLETED":
        return True
    return False


def format_partition_name(cluster_name, partition):
    """
    Formats a partition name by combining it with the cluster name.

    :param cluster_name: Name of the cluster.
    :param partition: Name of the partition.
    :return: Formatted partition name as a string.
    """
    return f"{cluster_name}_{partition}"


def extract_job_info(detail, combined_id, job_status_dict):
    """
    Extracts and returns job information based on job details and existing job statuses.

    :param detail: Dictionary containing details of the job.
    :param combined_id: Combined id for the job.
    :param job_status_dict: Dictionary of current job statuses.
    :return: Dictionary with updated job state and runtime, or None if no update is needed.
    """
    # Get the current job state and runtime
    cur_job_state = detail.get('job_state')
    raw_cur_runtime = detail.get('runtime')
    cur_runtime = parse_runtime_to_minutes(raw_cur_runtime) if raw_cur_runtime else None

    # Update the dictionary of job state and run time
    if combined_id in job_status_dict:
        prev_job_state = job_status_dict[combined_id].get("state")
        prev_runtime = job_status_dict[combined_id].get("runtime")
        if skip_job_update(prev_job_state, cur_job_state, prev_runtime, cur_runtime):
            return None
    return {
        "state": cur_job_state,
        "runtime": cur_runtime
    }


def update_node_list(detail, instance_ids):
    """
    Updates the list of nodes based on the provided details and existing instance IDs.

    :param detail: Dictionary containing job or node details.
    :param instance_ids: Set of existing instance IDs.
    :return: A list of new nodes based on the provided details.
    """
    node_list = detail.get("node_list", [])
    new_node_list = []
    for node in node_list:
        instance_id = node.get("instance_id")
        if instance_id and instance_id not in instance_ids:
            instance_ids.add(instance_id)
            new_node_list.append(node)
    return new_node_list


def extract_data_from_response(response_body):
    """
    Extracts and returns job and node data from a response body.

    :param response_body: The response body as a JSON string from which data is to be extracted.
    :return: A list of extracted data from the response.
    """
    logger.info("start extracting...")
    parsed_response = json.loads(response_body)
    hits = parsed_response.get('hits', {}).get('hits', [])
    extracted_data_list = []
    instance_ids = set()
    job_status_dict = {}

    for hit in hits:
        if is_log_processed(hit):
            # Skip the log if it is already processed
            continue
        if "detail" in hit["_source"]:
            detail = hit["_source"]["detail"]
            cluster = hit["_source"]["cluster-name"]

            if "partition" in detail:
                formatted_partition_name = format_partition_name(cluster, detail["partition"])
                detail["partition"] = formatted_partition_name

            if "job_id" in detail:
                # Create a combined id as the unique identifier for the cluster job combination
                combined_id = f"{cluster}_{detail['job_id']}"
                job_info = extract_job_info(detail, combined_id, job_status_dict)

                if job_info:
                    job_status_dict[combined_id] = job_info
                    logger.info(f"The current job_status_dict is {job_status_dict}")
                    extracted_data = {
                        "cluster_name": cluster,
                        "combined_id": combined_id,
                        "detail": detail
                    }
                    extracted_data_list.append(extracted_data)
            else:
                # Based on the simplified assumption, the node mapping will not change once form
                new_node_list = update_node_list(detail, instance_ids)
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
    """
    Retrieves the cost associated with a specific instance type.

    :param instance_type: The type of the instance.
    :return: Cost of the instance as an integer or float.
    """
    # Reference: https://aws.amazon.com/ec2/instance-types/t2/
    # In the real scenario, we may need to call an API
    costs = {
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
    """
    Calculates the total number of cores used based on CPU ID information.

    :param cpu_ids: A list of CPU IDs.
    :return: Total number of cores used as an integer.
    """
    total_cores = 0
    for cpu_id in cpu_ids:
        if "-" in cpu_id:
            start, end = cpu_id.split("-")
            if start.isdigit() and end.isdigit():
                total_cores += int(end) - int(start) + 1
        elif cpu_id.isdigit():
            total_cores += 1
    return total_cores


def is_job_relevant(job):
    """
    Determines if a job is relevant based on its state.

    :param job: A dictionary representing a job.
    :return: Boolean indicating whether the job state is either RUNNING or COMPLETED.
    """
    return job.get("job_state") in ["RUNNING", "COMPLETED"]


def get_cluster_nodes_detail(nodes_detail, current_cluster):
    """
    Retrieves node details for a specific cluster.

    :param nodes_detail: A list of dictionaries containing nodes' details.
    :param current_cluster: The name of the cluster for which to get node details.
    :return: A list of node details for the specified cluster, or None if not found.
    """
    for node_cluster in nodes_detail:
        if node_cluster["cluster_name"] == current_cluster:
            return node_cluster["detail"]["node_list"]
    logger.error(f"Could not find node details for cluster: {current_cluster}")
    return None


def find_node_detail(cluster_nodes_detail, node_name):

    node_detail = next((node for node in cluster_nodes_detail if node["node_name"] == node_name), None)
    if not node_detail:
        logger.error(f"Could not find node details for node name: {node_name}")
    return node_detail


def calculate_cpu_usage_ratio(used_cores, node_detail):
    """
    Calculates the CPU usage ratio for a node.

    :param used_cores: Number of used cores on the node.
    :param node_detail: A dictionary containing details of the node.
    :return: The CPU usage ratio as a float, or 0 if total VCPUs is zero.
    """
    total_vcpus = node_detail["threads_per_core"] * node_detail["core_count"]
    return used_cores / total_vcpus if total_vcpus else 0


def calculate_cost_for_node(node_name, cpu_ids, cluster_nodes_detail, job_runtime_minutes):
    """
    Calculates the cost for a node based on CPU usage and runtime.

    :param node_name: The name of the node.
    :param cpu_ids: List of CPU IDs used for the job.
    :param cluster_nodes_detail: Details of nodes in the cluster.
    :param job_runtime_minutes: The runtime of the job in minutes.
    :return: The calculated cost for the node as a float.
    """
    node_detail = find_node_detail(cluster_nodes_detail, node_name)
    if node_detail:
        used_cores = get_total_cores(cpu_ids)
        cpu_usage_ratio = calculate_cpu_usage_ratio(used_cores, node_detail)
        cost_per_minute = calculate_cost_per_minute(node_detail)
        return cost_per_minute * job_runtime_minutes * cpu_usage_ratio
    return 0


def calculate_cost_per_minute(node_detail):
    """
    Calculates the cost per minute for a node.

    :param node_detail: A dictionary containing details of the node.
    :return: The cost per minute as a float.
    """
    cost_per_hour = get_instance_cost(node_detail["instance_type"])
    return cost_per_hour / 60


def calculate_cost_for_job(job_data, nodes_detail):
    """
    Calculates the total cost for a job based on node usage and runtime.

    :param job_data: A dictionary containing details of the job.
    :param nodes_detail: Details of nodes in the cluster.
    :return: The total cost for the job as a float.
    """
    total_cost_for_job = 0
    job_runtime_minutes = parse_runtime_to_minutes(job_data["detail"]["run_time"])
    cluster_nodes_detail = get_cluster_nodes_detail(nodes_detail, job_data["cluster_name"])

    if cluster_nodes_detail:
        for node_name, cpu_ids in zip(job_data["detail"]["nodes"], job_data["detail"]["cpu_ids"]):
            cost_for_node = calculate_cost_for_node(node_name, cpu_ids, cluster_nodes_detail, job_runtime_minutes)
            total_cost_for_job += cost_for_node

    return round(total_cost_for_job, 4)


def calculate_cost(nodes_detail, jobs_detail):
    """
    Calculates costs for multiple jobs based on node details and job details.

    :param nodes_detail: Details of nodes in the cluster.
    :param jobs_detail: A list of dictionaries, each containing details of a job.
    :return: A dictionary mapping job IDs to their calculated costs.
    """
    logger.info("Enter into the cost calculation")
    total_costs = {}
    logger.info(f"Current nodes detail: {nodes_detail}")
    for job_data in jobs_detail:
        if is_job_relevant(job_data["detail"]):
            total_costs[job_data["combined_id"]] = calculate_cost_for_job(job_data, nodes_detail)
    return total_costs


def prepare_bulk_data(job_detail_with_cost):
    """
    Prepares data for bulk upload to OpenSearch.

    :param job_detail_with_cost: A list of job details including their estimated costs.
    :return: A string formatted for bulk upload to OpenSearch.
    """
    bulk_data = ''
    for job in job_detail_with_cost:
        # Add timestamp to the index
        job["timestamp"] = datetime.utcnow().isoformat() + "Z"
        # Use combined_id as document ID and specify update operation
        action_metadata = {"update": {"_id": job["combined_id"]}}
        update_data = {"doc": job, "doc_as_upsert": True}
        bulk_data += json.dumps(action_metadata) + '\n' + json.dumps(update_data) + '\n'
    return bulk_data


# def prepare_request(method, url, data, headers, sigv4):
#     request = AWSRequest(method=method, url=url, data=data, headers=headers)
#     sigv4.add_auth(request)
#     return request.prepare()


def post_estimated_cost_to_opensearch(endpoint, job_detail_with_cost):
    """
    Post estimated cost and related information for a specific job to OpenSearch
    :param endpoint: OpenSearch endpoint
    :param job_detail_with_cost: A list containing job details and its estimated cost
    :return: A dictionary with statusCode and response body.
    """
    username, password = get_username_and_password()
    # Remember to use bulk API
    path = "/jce-2023.11.13/_bulk/"
    url = endpoint + path

    headers = {
        "Content-Type": "application/x-ndjson"
    }

    # Preparing bulk data
    bulk_data = prepare_bulk_data(job_detail_with_cost)

    # Send the request
    try:
        response = requests.post(url, auth=(username, password), headers=headers, data=bulk_data)
        logger.info(f"Received response: {response.status_code} {response.text}")
        response.raise_for_status()

        # Mark the documents as processed only if successfully added to OpenSearch
        mark_documents_as_processed(ENDPOINT, "scontrol-show-job-information")
        mark_documents_as_processed(ENDPOINT, "node-instance-mapping-event")

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
    :param endpoint: OpenSearch endpoint
    :param event_type: The event type of the documents to be marked as processed.
    :return: A dictionary with statusCode and response body.
    """
    username, password = get_username_and_password()
    # sigv4 = SigV4Auth(session.get_credentials(), "es", "us-east-2")
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

    # Send the request
    try:
        response = requests.post(url, auth=HTTPBasicAuth(username, password), headers=headers, data=json.dumps(query))
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


def build_query_to_search(event_type):
    """
    Constructs a query for searching documents in OpenSearch based on the event type.

    :param event_type: The type of event to match in the query.
    :return: A dictionary representing the search query.
    """
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


def get_username_and_password():
    """
    Retrieves the username and password stored in AWS Secret Manager.

    :return: A tuple containing the username and password.
    """
    secret_name = os.environ["SECRET_NAME"]
    region_name = "us-east-2"
    s = boto3.session.Session()
    client = s.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(get_secret_value_response['SecretString'])

    username = list(secret.keys())[0]
    password = secret[username]

    return username, password


def search_data(endpoint, event_type):
    """
    Searches for data in OpenSearch indices based on the provided event type.

    :param endpoint: The OpenSearch endpoint URL.
    :param event_type: The event type to search for.
    :return: A dictionary with statusCode and the search results or error message.
    """
    username, password = get_username_and_password()

    # searching in OpenSearch for indices matching the pattern "cwl-*"
    path = "/cwl-*/_search"
    url = endpoint + path
    query = build_query_to_search(event_type)
    headers = {"Content-Type": "application/json"}

    # Send the request and handle response
    try:
        response = requests.get(url, auth=(username, password), headers=headers, json=query)
        response.raise_for_status()
        logger.info("Query succeeded")
        results = response.json()
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
