from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.exceptions import NotFoundError

ENDPOINT = 'https://search-mylogs-kidfhbnbletp4ybierlou2llq4.us-east-2.es.amazonaws.com'


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


def fetch_log_from_opensearch(event_type):
    # Create a connection to the OpenSearch cluster
    opensearch = OpenSearch(
        hosts=[ENDPOINT],
        connection_class=RequestsHttpConnection,
        use_ssl=True,
        verify_certs=True,
    )

    try:
        # Use the search method to fetch logs
        response = opensearch.search(index="cwl-*", body=build_query(event_type))
        return response['hits']['hits']  # This will return the list of logs
    except NotFoundError:
        print(f"No logs found for event type: {event_type}")
        return []


if __name__ == '__main__':
    # Fetch node mapping information
    logs = fetch_log_from_opensearch("node-instance-mapping-event")
    for log in logs:
        print(log['_source'])  # Print the source of each log entry
