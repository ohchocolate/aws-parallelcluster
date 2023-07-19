# coding: utf-8

from __future__ import absolute_import

from datetime import date, datetime  # noqa: F401
from typing import Dict, List  # noqa: F401

from pcluster.api import util
from pcluster.api.models.base_model_ import Model


class Detail(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, alarm_type=None, alarm_state=None):  # noqa: E501
        """Detail - a model defined in OpenAPI

        :param alarm_type: The alarm_type of this Detail.  # noqa: E501
        :type alarm_type: str
        :param alarm_state: The alarm_state of this Detail.  # noqa: E501
        :type alarm_state: str
        """
        self.openapi_types = {"alarm_type": str, "alarm_state": str}

        self.attribute_map = {"alarm_type": "alarmType", "alarm_state": "alarmState"}

        self._alarm_type = alarm_type
        self._alarm_state = alarm_state

    @classmethod
    def from_dict(cls, dikt) -> "Detail":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The Detail of this Detail.  # noqa: E501
        :rtype: Detail
        """
        return util.deserialize_model(dikt, cls)

    @property
    def alarm_type(self):
        """Gets the alarm_type of this Detail.

        The alarm type when the verbose flag is set to true.  # noqa: E501

        :return: The alarm_type of this Detail.
        :rtype: str
        """
        return self._alarm_type

    @alarm_type.setter
    def alarm_type(self, alarm_type):
        """Sets the alarm_type of this Detail.

        The alarm type when the verbose flag is set to true.  # noqa: E501

        :param alarm_type: The alarm_type of this Detail.
        :type alarm_type: str
        """

        self._alarm_type = alarm_type

    @property
    def alarm_state(self):
        """Gets the alarm_state of this Detail.

        The alarm state when the verbose flag is set to true.  # noqa: E501

        :return: The alarm_state of this Detail.
        :rtype: str
        """
        return self._alarm_state

    @alarm_state.setter
    def alarm_state(self, alarm_state):
        """Sets the alarm_state of this Detail.

        The alarm state when the verbose flag is set to true.  # noqa: E501

        :param alarm_state: The alarm_state of this Detail.
        :type alarm_state: str
        """

        self._alarm_state = alarm_state