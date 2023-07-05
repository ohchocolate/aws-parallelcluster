# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from pcluster.api.models.base_model_ import Model
import re
from pcluster.api import util

import re  # noqa: E501

class BuildImageRequestContent(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, image_configuration=None, image_id=None):  # noqa: E501
        """BuildImageRequestContent - a model defined in OpenAPI

        :param image_configuration: The image_configuration of this BuildImageRequestContent.  # noqa: E501
        :type image_configuration: str
        :param image_id: The image_id of this BuildImageRequestContent.  # noqa: E501
        :type image_id: str
        """
        self.openapi_types = {
            'image_configuration': str,
            'image_id': str
        }

        self.attribute_map = {
            'image_configuration': 'imageConfiguration',
            'image_id': 'imageId'
        }

        self._image_configuration = image_configuration
        self._image_id = image_id

    @classmethod
    def from_dict(cls, dikt) -> 'BuildImageRequestContent':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The BuildImageRequestContent of this BuildImageRequestContent.  # noqa: E501
        :rtype: BuildImageRequestContent
        """
        return util.deserialize_model(dikt, cls)

    @property
    def image_configuration(self):
        """Gets the image_configuration of this BuildImageRequestContent.

        Image configuration as a YAML document.  # noqa: E501

        :return: The image_configuration of this BuildImageRequestContent.
        :rtype: str
        """
        return self._image_configuration

    @image_configuration.setter
    def image_configuration(self, image_configuration):
        """Sets the image_configuration of this BuildImageRequestContent.

        Image configuration as a YAML document.  # noqa: E501

        :param image_configuration: The image_configuration of this BuildImageRequestContent.
        :type image_configuration: str
        """
        if image_configuration is None:
            raise ValueError("Invalid value for `image_configuration`, must not be `None`")  # noqa: E501

        self._image_configuration = image_configuration

    @property
    def image_id(self):
        """Gets the image_id of this BuildImageRequestContent.

        Id of the Image that will be built.  # noqa: E501

        :return: The image_id of this BuildImageRequestContent.
        :rtype: str
        """
        return self._image_id

    @image_id.setter
    def image_id(self, image_id):
        """Sets the image_id of this BuildImageRequestContent.

        Id of the Image that will be built.  # noqa: E501

        :param image_id: The image_id of this BuildImageRequestContent.
        :type image_id: str
        """
        if image_id is None:
            raise ValueError("Invalid value for `image_id`, must not be `None`")  # noqa: E501
        if image_id is not None and not re.search(r'^[a-zA-Z][a-zA-Z0-9-]+$', image_id):  # noqa: E501
            raise ValueError("Invalid value for `image_id`, must be a follow pattern or equal to `/^[a-zA-Z][a-zA-Z0-9-]+$/`")  # noqa: E501

        self._image_id = image_id
