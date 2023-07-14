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
        "describe_alarms_response, expected_response, expect_error",
        [
            pytest.param(
                {"MetricAlarms": []},
                [],
                False,
                id="test with no alarms",
            ),
            pytest.param(
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
                            "StateValue": "OK",
                            "StateReason": "",
                            "StateReasonData": "{}",
                            "StateUpdatedTimestamp": "2023-07-07T20:39:26.932000+00:00",
                            "MetricName": "ClusterInProtectedMode",
                            "Namespace": "ParallelCluster",
                            "Statistic": "Average",
                            "Dimensions": [{"Name": "ClusterName", "Value": "test"}],
                            "Period": 60,
                            "EvaluationPeriods": 1,
                            "DatapointsToAlarm": 1,
                            "Threshold": 1.0,
                        }
                    ]
                },
                [],
                False,
                id="test with OK alarms only",
            ),
            pytest.param(
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
                        },
                        {
                            "AlarmName": "Alarm2",
                            "AlarmArn": "arn:aws:cloudwatch:us-east-2:666:alarm:test2_ProtectedModeAlarm_HeadNode",
                            "AlarmConfigurationUpdatedTimestamp": "2023-07-12T15:50:52.691000+00:00",
                            "ActionsEnabled": True,
                            "OKActions": [],
                            "AlarmActions": [],
                            "InsufficientDataActions": [],
                            "StateValue": "INSUFFICIENT_DATA",
                        },
                    ]
                },
                [],
                False,
                id="test with INSUFFICIENT_DATA alarms only",
            ),
            pytest.param(
                {
                    "MetricAlarms": [
                        {"AlarmName": "Alarm1", "StateValue": "ALARM"},
                        {"AlarmName": "Alarm2", "StateValue": "OK"},
                    ]
                },
                [{"alarm_type": "Alarm1", "alarm_state": "ALARM"}],
                False,
                id="test with some alarms in 'ALARM' state",
            ),
            pytest.param(
                {
                    "MetricAlarms": [
                        {
                            "AlarmName": "Alarm1",
                            "AlarmArn": "arn:aws:cloudwatch:us-east-2:666:alarm:test1_ProtectedModeAlarm_HeadNode",
                            "AlarmConfigurationUpdatedTimestamp": "2023-07-06T18:50:52.691000+00:00",
                            "ActionsEnabled": True,
                            "OKActions": [],
                            "AlarmActions": [],
                            "InsufficientDataActions": [],
                            "StateValue": "ALARM",
                        },
                        {
                            "AlarmName": "Alarm2",
                            "AlarmArn": "arn:aws:cloudwatch:us-east-2:666:alarm:test2_ProtectedModeAlarm_HeadNode",
                            "AlarmConfigurationUpdatedTimestamp": "2023-07-12T15:50:52.691000+00:00",
                            "ActionsEnabled": True,
                            "OKActions": [],
                            "AlarmActions": [],
                            "InsufficientDataActions": [],
                            "StateValue": "ALARM",
                        },
                    ]
                },
                [
                    {"alarm_type": "Alarm1", "alarm_state": "ALARM"},
                    {"alarm_type": "Alarm2", "alarm_state": "ALARM"},
                ],
                False,
                id="test with all alarms in 'ALARM' state",
            ),
            pytest.param(Exception("An error occurred"), None, True, id="test with exceptions"),
        ],
    )
    def test_get_alarms_in_alarm(
        self, set_env, boto3_stubber, describe_alarms_response, expected_response, expect_error
    ):
        set_env("AWS_DEFAULT_REGION", "us-east-2")
        generate_error = isinstance(describe_alarms_response, Exception)
        mocked_requests = [
            MockedBoto3Request(
                method="describe_alarms",
                response=describe_alarms_response if not generate_error else "error",
                expected_params={"AlarmNames": ["Alarm1", "Alarm2"]},
                generate_error=generate_error,
                error_code="error" if generate_error else None,
            )
        ]

        boto3_stubber("cloudwatch", mocked_requests)

        cw_client = CloudWatchClient()
        if not expect_error:
            response = cw_client.get_alarms_in_alarm(["Alarm1", "Alarm2"])
            assert response == expected_response
        else:
            with pytest.raises(AWSClientError) as e:
                cw_client.get_alarms_in_alarm(["Alarm1", "Alarm2"])
            assert_that(e.value.error_code).is_equal_to("error")
