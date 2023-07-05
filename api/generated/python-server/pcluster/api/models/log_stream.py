# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from pcluster.api.models.base_model_ import Model
from pcluster.api import util


class LogStream(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, log_stream_name=None, creation_time=None, first_event_timestamp=None, last_event_timestamp=None, last_ingestion_time=None, upload_sequence_token=None, log_stream_arn=None):  # noqa: E501
        """LogStream - a model defined in OpenAPI

        :param log_stream_name: The log_stream_name of this LogStream.  # noqa: E501
        :type log_stream_name: str
        :param creation_time: The creation_time of this LogStream.  # noqa: E501
        :type creation_time: datetime
        :param first_event_timestamp: The first_event_timestamp of this LogStream.  # noqa: E501
        :type first_event_timestamp: datetime
        :param last_event_timestamp: The last_event_timestamp of this LogStream.  # noqa: E501
        :type last_event_timestamp: datetime
        :param last_ingestion_time: The last_ingestion_time of this LogStream.  # noqa: E501
        :type last_ingestion_time: datetime
        :param upload_sequence_token: The upload_sequence_token of this LogStream.  # noqa: E501
        :type upload_sequence_token: str
        :param log_stream_arn: The log_stream_arn of this LogStream.  # noqa: E501
        :type log_stream_arn: str
        """
        self.openapi_types = {
            'log_stream_name': str,
            'creation_time': datetime,
            'first_event_timestamp': datetime,
            'last_event_timestamp': datetime,
            'last_ingestion_time': datetime,
            'upload_sequence_token': str,
            'log_stream_arn': str
        }

        self.attribute_map = {
            'log_stream_name': 'logStreamName',
            'creation_time': 'creationTime',
            'first_event_timestamp': 'firstEventTimestamp',
            'last_event_timestamp': 'lastEventTimestamp',
            'last_ingestion_time': 'lastIngestionTime',
            'upload_sequence_token': 'uploadSequenceToken',
            'log_stream_arn': 'logStreamArn'
        }

        self._log_stream_name = log_stream_name
        self._creation_time = creation_time
        self._first_event_timestamp = first_event_timestamp
        self._last_event_timestamp = last_event_timestamp
        self._last_ingestion_time = last_ingestion_time
        self._upload_sequence_token = upload_sequence_token
        self._log_stream_arn = log_stream_arn

    @classmethod
    def from_dict(cls, dikt) -> 'LogStream':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The LogStream of this LogStream.  # noqa: E501
        :rtype: LogStream
        """
        return util.deserialize_model(dikt, cls)

    @property
    def log_stream_name(self):
        """Gets the log_stream_name of this LogStream.

        Name of the log stream.  # noqa: E501

        :return: The log_stream_name of this LogStream.
        :rtype: str
        """
        return self._log_stream_name

    @log_stream_name.setter
    def log_stream_name(self, log_stream_name):
        """Sets the log_stream_name of this LogStream.

        Name of the log stream.  # noqa: E501

        :param log_stream_name: The log_stream_name of this LogStream.
        :type log_stream_name: str
        """
        if log_stream_name is None:
            raise ValueError("Invalid value for `log_stream_name`, must not be `None`")  # noqa: E501

        self._log_stream_name = log_stream_name

    @property
    def creation_time(self):
        """Gets the creation_time of this LogStream.

        The creation time of the stream.  # noqa: E501

        :return: The creation_time of this LogStream.
        :rtype: datetime
        """
        return self._creation_time

    @creation_time.setter
    def creation_time(self, creation_time):
        """Sets the creation_time of this LogStream.

        The creation time of the stream.  # noqa: E501

        :param creation_time: The creation_time of this LogStream.
        :type creation_time: datetime
        """
        if creation_time is None:
            raise ValueError("Invalid value for `creation_time`, must not be `None`")  # noqa: E501

        self._creation_time = creation_time

    @property
    def first_event_timestamp(self):
        """Gets the first_event_timestamp of this LogStream.

        The time of the first event of the stream.  # noqa: E501

        :return: The first_event_timestamp of this LogStream.
        :rtype: datetime
        """
        return self._first_event_timestamp

    @first_event_timestamp.setter
    def first_event_timestamp(self, first_event_timestamp):
        """Sets the first_event_timestamp of this LogStream.

        The time of the first event of the stream.  # noqa: E501

        :param first_event_timestamp: The first_event_timestamp of this LogStream.
        :type first_event_timestamp: datetime
        """
        if first_event_timestamp is None:
            raise ValueError("Invalid value for `first_event_timestamp`, must not be `None`")  # noqa: E501

        self._first_event_timestamp = first_event_timestamp

    @property
    def last_event_timestamp(self):
        """Gets the last_event_timestamp of this LogStream.

        The time of the last event of the stream. The lastEventTime value updates on an eventual consistency basis. It typically updates in less than an hour from ingestion, but in rare situations might take longer.  # noqa: E501

        :return: The last_event_timestamp of this LogStream.
        :rtype: datetime
        """
        return self._last_event_timestamp

    @last_event_timestamp.setter
    def last_event_timestamp(self, last_event_timestamp):
        """Sets the last_event_timestamp of this LogStream.

        The time of the last event of the stream. The lastEventTime value updates on an eventual consistency basis. It typically updates in less than an hour from ingestion, but in rare situations might take longer.  # noqa: E501

        :param last_event_timestamp: The last_event_timestamp of this LogStream.
        :type last_event_timestamp: datetime
        """
        if last_event_timestamp is None:
            raise ValueError("Invalid value for `last_event_timestamp`, must not be `None`")  # noqa: E501

        self._last_event_timestamp = last_event_timestamp

    @property
    def last_ingestion_time(self):
        """Gets the last_ingestion_time of this LogStream.

        The last ingestion time.  # noqa: E501

        :return: The last_ingestion_time of this LogStream.
        :rtype: datetime
        """
        return self._last_ingestion_time

    @last_ingestion_time.setter
    def last_ingestion_time(self, last_ingestion_time):
        """Sets the last_ingestion_time of this LogStream.

        The last ingestion time.  # noqa: E501

        :param last_ingestion_time: The last_ingestion_time of this LogStream.
        :type last_ingestion_time: datetime
        """
        if last_ingestion_time is None:
            raise ValueError("Invalid value for `last_ingestion_time`, must not be `None`")  # noqa: E501

        self._last_ingestion_time = last_ingestion_time

    @property
    def upload_sequence_token(self):
        """Gets the upload_sequence_token of this LogStream.

        The sequence token.  # noqa: E501

        :return: The upload_sequence_token of this LogStream.
        :rtype: str
        """
        return self._upload_sequence_token

    @upload_sequence_token.setter
    def upload_sequence_token(self, upload_sequence_token):
        """Sets the upload_sequence_token of this LogStream.

        The sequence token.  # noqa: E501

        :param upload_sequence_token: The upload_sequence_token of this LogStream.
        :type upload_sequence_token: str
        """
        if upload_sequence_token is None:
            raise ValueError("Invalid value for `upload_sequence_token`, must not be `None`")  # noqa: E501

        self._upload_sequence_token = upload_sequence_token

    @property
    def log_stream_arn(self):
        """Gets the log_stream_arn of this LogStream.

        The Amazon Resource Name (ARN) of the log stream.  # noqa: E501

        :return: The log_stream_arn of this LogStream.
        :rtype: str
        """
        return self._log_stream_arn

    @log_stream_arn.setter
    def log_stream_arn(self, log_stream_arn):
        """Sets the log_stream_arn of this LogStream.

        The Amazon Resource Name (ARN) of the log stream.  # noqa: E501

        :param log_stream_arn: The log_stream_arn of this LogStream.
        :type log_stream_arn: str
        """
        if log_stream_arn is None:
            raise ValueError("Invalid value for `log_stream_arn`, must not be `None`")  # noqa: E501

        self._log_stream_arn = log_stream_arn
