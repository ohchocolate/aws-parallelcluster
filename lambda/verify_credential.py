import botocore.session
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest


def main():
    session = botocore.session.get_session()
    credentials = session.get_credentials()
    sigv4 = SigV4Auth(credentials, "es", "us-east-2")

    url = 'https://search-mylogs-kidfhbnbletp4ybierlou2llq4.us-east-2.es.amazonaws.com'

    request = AWSRequest(method='GET', url=url, data=None)

    sigv4.add_auth(request)

    response = requests.get(url, headers=dict(request.headers))

    print(response.text)


if __name__ == '__main__':
    main()
