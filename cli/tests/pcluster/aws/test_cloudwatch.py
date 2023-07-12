#  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
#  with the License. A copy of the License is located at http://aws.amazon.com/apache2.0/
#  or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
#  limitations under the License.
from datetime import datetime

import pytest
from assertpy import assert_that

from pcluster import utils as utils
from pcluster.aws.cfn import CfnClient
from pcluster.aws.cloudwatch import CloudWatchClient
from pcluster.aws.common import AWSClientError
from tests.pcluster.test_utils import FAKE_NAME, _generate_stack_event
from tests.utils import MockedBoto3Request


@pytest.fixture()
def boto3_stubber_path():
    return "pcluster.aws.common.boto3"


class TestCloudWatchClient:
    @pytest.mark.parametrize(
        "describe_alarms_response, expected_response",
        [(
                {
                    "MetricAlarms": [
                        {
                            "AlarmName": "Alarm1",
                            "AlarmArn": "arn:aws:cloudwatch:us-east-2:123:alarm:Alarm1",
                            "AlarmConfigurationUpdatedTimestamp": "2023-06-13T14:52:07.908000+00:00",
                            "ActionsEnabled": True,
                            "OKActions": [],
                            "AlarmActions": [],
                            "InsufficientDataActions": [],
                            "StateValue": "INSUFFICIENT_DATA",
                            "StateReason": "Insufficient Data: 1 datapoint was unknown.",
                            "StateReasonData": "{}",
                            "StateUpdatedTimestamp": "2023-06-19T21:05:38.314000+00:00",
                            "MetricName": "ClusterInProtectedMode",
                            "Namespace": "ParallelCluster",
                            "Statistic": "SampleCount",
                            "Dimensions": [
                                {
                                    "Name": "ClusterName",
                                    "Value": "Metric1"
                                }
                            ],
                            "Period": 60,
                            "EvaluationPeriods": 1,
                            "DatapointsToAlarm": 1,
                            "Threshold": 1.0,
                            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
                            "TreatMissingData": "missing",
                            "StateTransitionedTimestamp": "2023-06-19T21:05:38.314000+00:00"
                        },
                    ],
                },
                {
                    "MetricAlarms": [
                        {
                            "AlarmName": "Alarm1",
                            "AlarmArn": "arn:aws:cloudwatch:us-east-2:123:alarm:Alarm1",
                            "AlarmConfigurationUpdatedTimestamp": "2023-06-13T14:52:07.908000+00:00",
                            "ActionsEnabled": True,
                            "OKActions": [],
                            "AlarmActions": [],
                            "InsufficientDataActions": [],
                            "StateValue": "INSUFFICIENT_DATA",
                            "StateReason": "Insufficient Data: 1 datapoint was unknown.",
                            "StateReasonData": "{}",
                            "StateUpdatedTimestamp": "2023-06-19T21:05:38.314000+00:00",
                            "MetricName": "ClusterInProtectedMode",
                            "Namespace": "ParallelCluster",
                            "Statistic": "SampleCount",
                            "Dimensions": [
                                {
                                    "Name": "ClusterName",
                                    "Value": "Metric1"
                                }
                            ],
                            "Period": 60,
                            "EvaluationPeriods": 1,
                            "DatapointsToAlarm": 1,
                            "Threshold": 1.0,
                            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
                            "TreatMissingData": "missing",
                            "StateTransitionedTimestamp": "2023-06-19T21:05:38.314000+00:00"
                        },
                    ],
                },
        )],

    )
    def test_describe_alarms(self, set_env, boto3_stubber, describe_alarms_response, expected_response):
        set_env("AWS_DEFAULT_REGION", "us-east-1")

        generate_error = isinstance(describe_alarms_response, Exception)
        mocked_requests = [
            MockedBoto3Request(
                method="describe_alarms",
                response=describe_alarms_response if not generate_error else "error",
                expected_params={"AlarmNames": ["Alarm1"]},
                generate_error=generate_error,
                error_code="error" if generate_error else None,
            )
        ]
        boto3_stubber("cloudwatch", mocked_requests)

        cw_client = CloudWatchClient()
        response = cw_client.describe_alarms(["Alarm1"])
        assert response == expected_response

    @pytest.mark.parametrize(
        "describe_alarms_response, expected_response",
        [
            (
                    {"MetricAlarms": []},
                    [],
            ),
            (
                    {"MetricAlarms": [{"AlarmName": "Alarm1", "StateValue": "OK"}]},
                    [],
            ),
            (
                    {
                        "MetricAlarms": [
                            {"AlarmName": "Alarm1", "StateValue": "ALARM"},
                            {"AlarmName": "Alarm2", "StateValue": "OK"},
                        ]
                    },
                    [{"alarm_type": "Alarm1", "alarm_state": "ALARM"}],
            ),
            (
                    {
                        "MetricAlarms": [
                            {"AlarmName": "Alarm1", "StateValue": "ALARM"},
                            {"AlarmName": "Alarm2", "StateValue": "ALARM"},
                        ]
                    },
                    [
                        {"alarm_type": "Alarm1", "alarm_state": "ALARM"},
                        {"alarm_type": "Alarm2", "alarm_state": "ALARM"},
                    ],
            ),
        ],
    )
    def test_get_alarms_in_alarm(self, set_env, boto3_stubber, describe_alarms_response, expected_response):
        set_env("AWS_DEFAULT_REGION", "us-east-1")
        mocked_requests = [
            MockedBoto3Request(
                method="describe_alarms",
                response=describe_alarms_response,
                expected_params={"AlarmNames": ["Alarm1", "Alarm2"]},
            )
        ]
        boto3_stubber("cloudwatch", mocked_requests)

        cw_client = CloudWatchClient()
        response = cw_client.get_alarms_in_alarm(["Alarm1", "Alarm2"])
        assert response == expected_response
