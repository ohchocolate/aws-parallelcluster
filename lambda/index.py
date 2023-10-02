import base64
import datetime
import json
import logging
import math
import re
import zlib

import botocore.session
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# use log to debug
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
session = botocore.session.Session()
sigv4 = SigV4Auth(session.get_credentials(), "es", "us-east-2")
endpoint = 'https://search-mylogs-kidfhbnbletp4ybierlou2llq4.us-east-2.es.amazonaws.com'
logFailedResponses = False


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = zlib.decompress(compressed_payload, 16 + zlib.MAX_WBITS)
    logger.info(f"Uncompressed payload: {uncompressed_payload}")

    payload = json.loads(uncompressed_payload)
    logger.info(f"JSON payload: {json.dumps(payload)}")

    bulk_data = transform(payload)
    logger.info(f"Transformed bulk data: {bulk_data}")

    if not bulk_data:
        print('Received a control message')
        return {'statusCode': 200, 'body': 'Control message handled successfully'}

    response = post(bulk_data)
    logging.info(f"OpenSearch response status: {response.status_code}, content: {response.json()}")

    if response.status_code == 200:
        print('Success:', response.json())
        return {'statusCode': 200, 'body': 'Success'}
    else:
        if logFailedResponses:
            print('Error:', response.json())
        return {'statusCode': 500, 'body': 'Failed to post to OpenSearch'}


def transform(payload):
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
        logger.debug("Build the node mapping")
        if source.get("event-type") == "scontrol-show-job-information":
            source = process_job_info(source)

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


def process_job_info(source):
    query = {
        "size": 1,
        "query": {"match": {"event-type.keyword": "node-instance-mapping-event"}},
        "sort": [{"datetime": {"order": "desc"}}],
    }
    response = json.loads(get(query).text)
    node_name = source["detail"]["node_list"]
    node_name_list = get_node_list(node_name)
    node_map = response["hits"]["hits"][0]["_source"]["detail"]["node_list"]
    nodes = []
    node_list = source["detail"]["nodes"]
    cpu_ids_list = source["detail"]["cpu_ids"]
    gres_list = source["detail"]["gres"]
    node_dict = get_node_dict(node_list, cpu_ids_list, gres_list, node_map)
    for node in node_name_list:
        nodes.append(node_dict[node])
    source["detail"]["nodes"] = nodes
    del source["detail"]["node_list"]
    return source


def get_node_list(node_names):
    """
    Convert node_names to a list of nodes.

    Example input node_names: "queue1-st-c5xlarge-[1,3,4-5],queue1-st-c5large-20"
    Example output [queue1-st-c5xlarge-1, queue1-st-c5xlarge-3, queue1-st-c5xlarge-4, queue1-st-c5xlarge-5,
    queue1-st-c5large-20]
    """
    matches = []
    if type(node_names) is str:
        matches = re.findall(r"((([a-z0-9\-]+)-(st|dy)-([a-z0-9\-]+)-)(\[[\d+,-]+\]|\d+))(,|$)", node_names)
        # [('queue1-st-c5xlarge-[1,3,4-5]', 'queue1-st-c5xlarge-', 'queue1', 'st', 'c5xlarge', '[1,3,4-5]'),
        # ('queue1-st-c5large-20', 'queue1-st-c5large-', 'queue1', 'st', 'c5large', '20')]
    node_list = []
    if not matches:
        print("Invalid Node Name Error")
    for match in matches:
        node_name, prefix, _, _, _, nodes, _ = match
        if "[" not in nodes:
            # Single node name
            node_list.append(node_name)
        else:
            # Multiple node names
            try:
                node_range = convert_range_to_list(nodes.strip("[]"))
            except ValueError:
                print("Invalid Node Name Error")
            node_list += [prefix + str(n) for n in node_range]
    return node_list


def convert_range_to_list(node_range):
    """
    Convert a number range to a list.

    Example input: Input can be like one of the format: "1-3", "1-2,6", "2, 8"
    Example output: [1, 2, 3]
    """
    return sum(
        (
            (list(range(*[int(j) + k for k, j in enumerate(i.split("-"))])) if "-" in i else [int(i)])
            for i in node_range.split(",")
        ),
        [],
    )


def get_node_dict(node_list, cpu_ids_list, gres_list, node_map):
    node_name_dict = {}
    for node in node_map:
        name = node["node_name"]
        node_name_dict[name] = node
    for i in range(len(node_list)):
        node_names = get_node_list(node_list[i])
        cpu_ids = cpu_ids_list[i]
        gres = None
        for node_name in node_names:
            node_info = node_name_dict[node_name]
            node_info["cpu_ids"] = cpu_ids
            if gres_list:
                gres = gres_list[i]
            cpu_usage = 0
            if "," in cpu_ids:
                cpu_ids_continuous_list = cpu_ids.split(",")
                for cpu_ids_continuous in cpu_ids_continuous_list:
                    cpu_usage += get_cpu_num(cpu_ids_continuous)
            else:
                cpu_usage = get_cpu_num(cpu_ids)
            node_info["cpu_usage"] = cpu_usage
            if gres:
                gpu_ids = re.search(r"\((.*?):(\d+)\)", gres).group(2)
                gpu_type_and_usage = gres[4:].split("(")[0]
                node_info["gpu_ids"] = gpu_ids
                node_info["gpu_type"] = gpu_type_and_usage.split(":")[0]
                node_info["gpu_usage"] = gpu_type_and_usage.split(":")[1]
            else:
                node_info["gpu_ids"] = None
                node_info["gpu_type"] = None
                node_info["gpu_usage"] = 0
            node_name_dict[node_name] = node_info

    return node_name_dict


def get_cpu_num(cpu_ids):
    if "-" in cpu_ids:
        split = cpu_ids.split("-")
        return int(split[1]) - int(split[0]) + 1
    return 1


def build_source(message, extracted_fields):
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
    json_start = message.index("{")
    if json_start < 0:
        return None
    json_substring = message[json_start:]
    if is_valid_json(json_substring):
        return json_substring
    return None


def is_valid_json(message):
    try:
        json.loads(message)
        return True
    except json.JSONDecodeError:
        return False


def make_request(method, endpoint, data=None):
    logger.debug(f"Making {method} request to {endpoint} with data: {data}")
    headers = {"Content-Type": "application/json"}

    request = AWSRequest(method=method, url=endpoint, data=data, headers=headers)
    request.context["payload_signing_enabled"] = True

    logger.debug("Adding SIGV4 auth to the request...")
    sigv4.add_auth(request)

    prepped = request.prepare()

    logger.debug(f"Sending request with URL: {prepped.url}, Headers: {prepped.headers}, Body: {prepped.body}")
    response = requests.request(method, prepped.url, data=prepped.body, headers=prepped.headers, timeout=200)

    logger.debug(f"Received response with status code: {response.status_code}, content: {response.text}")
    return response


def post(body):
    logger.debug(f"Posting data: {body}")
    endpoint_for_post = endpoint + "/_bulk"
    response = make_request("POST", endpoint_for_post, body)
    return response


def get(query):
    data = json.dumps(query)
    endpoint_for_get = endpoint + "/cwl-2023.09.*/_search"
    response = make_request("GET", endpoint_for_get, data)
    return response
