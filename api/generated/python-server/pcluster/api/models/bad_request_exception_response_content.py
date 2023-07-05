# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from pcluster.api.models.base_model_ import Model
from pcluster.api import util


class BadRequestExceptionResponseContent(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, message=None):  # noqa: E501
        """BadRequestExceptionResponseContent - a model defined in OpenAPI

        :param message: The message of this BadRequestExceptionResponseContent.  # noqa: E501
        :type message: str
        """
        self.openapi_types = {
            'message': str
        }

        self.attribute_map = {
            'message': 'message'
        }

        self._message = message

    @classmethod
    def from_dict(cls, dikt) -> 'BadRequestExceptionResponseContent':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The BadRequestExceptionResponseContent of this BadRequestExceptionResponseContent.  # noqa: E501
        :rtype: BadRequestExceptionResponseContent
        """
        return util.deserialize_model(dikt, cls)

    @property
    def message(self):
        """Gets the message of this BadRequestExceptionResponseContent.


        :return: The message of this BadRequestExceptionResponseContent.
        :rtype: str
        """
        return self._message

    @message.setter
    def message(self, message):
        """Sets the message of this BadRequestExceptionResponseContent.


        :param message: The message of this BadRequestExceptionResponseContent.
        :type message: str
        """

        self._message = message
