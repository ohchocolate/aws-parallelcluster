# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from pcluster.api.models.base_model_ import Model
from pcluster.api.models.image_info_summary import ImageInfoSummary
from pcluster.api import util

from pcluster.api.models.image_info_summary import ImageInfoSummary  # noqa: E501

class DeleteImageResponseContent(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, image=None):  # noqa: E501
        """DeleteImageResponseContent - a model defined in OpenAPI

        :param image: The image of this DeleteImageResponseContent.  # noqa: E501
        :type image: ImageInfoSummary
        """
        self.openapi_types = {
            'image': ImageInfoSummary
        }

        self.attribute_map = {
            'image': 'image'
        }

        self._image = image

    @classmethod
    def from_dict(cls, dikt) -> 'DeleteImageResponseContent':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The DeleteImageResponseContent of this DeleteImageResponseContent.  # noqa: E501
        :rtype: DeleteImageResponseContent
        """
        return util.deserialize_model(dikt, cls)

    @property
    def image(self):
        """Gets the image of this DeleteImageResponseContent.


        :return: The image of this DeleteImageResponseContent.
        :rtype: ImageInfoSummary
        """
        return self._image

    @image.setter
    def image(self, image):
        """Sets the image of this DeleteImageResponseContent.


        :param image: The image of this DeleteImageResponseContent.
        :type image: ImageInfoSummary
        """
        if image is None:
            raise ValueError("Invalid value for `image`, must not be `None`")  # noqa: E501

        self._image = image
