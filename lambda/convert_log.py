import base64
import json
import zlib

# 示例CloudWatch日志事件
sample_log_event = {
    "messageType": "DATA_MESSAGE",
    "owner": "123456789012",
    "logGroup": "testLogGroup",
    "logStream": "testLogStream",
    "subscriptionFilters": ["testFilter"],
    "logEvents": [
        {
            "id": "eventId1",
            "timestamp": 1565080400000,
            "event-type": "scontrol-show-job-information",
            "message": "Job information from scontrol",
        },
        {
            "id": "eventId2",
            "timestamp": 1565080400001,
            "event-type": "scontrol-show-job-information",
            "message": "Job information from scontrol",
        }
    ]
}


def convert_log(log_event):
    compressed_string = zlib.compress(json.dumps(log_event).encode('utf-8'))
    encoded_string = base64.b64encode(compressed_string)
    return encoded_string


if __name__ == '__main__':
    print(convert_log(sample_log_event))
    print("//////////////")

