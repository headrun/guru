"""
Example settings for local development

Use this file as a base for your local development settings.
It should not be checked into your code repository.
"""
from settings import *   # pylint: disable=W0614,W0401

DEBUG=True
ALLOWED_HOSTS.extend(["guru.headrun.com", "144.76.48.157"])
DATABASES["default"].update({

  'NAME': 'ars',
  'USER': 'root',
  'PASSWORD': ''
})
