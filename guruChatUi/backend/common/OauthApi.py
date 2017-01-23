import time
import binascii
import uuid
import json
import requests
from tornado.auth import _oauth10a_signature
from urllib import urlencode

requests.packages.urllib3.disable_warnings()

class OauthApi(object):

  def __init__(self, consumerToken, accessToken):

    '''
    consumerToken and accessToken are dicts with 'key' and 'secret'
    as keys
    '''

    self.consumerToken = consumerToken
    self.accessToken   = accessToken

  def getParams(self, url, parameters=None, method="GET"):

    #handled default parameter gotcha in python
    if not parameters:
      parameters = {}

    consumerToken = self.consumerToken
    accessToken   = self.accessToken

    baseArgs = dict(
        oauth_consumer_key=consumerToken["key"],
        oauth_token=accessToken["key"],
        oauth_signature_method="HMAC-SHA1",
        oauth_timestamp=str(int(time.time())),
        oauth_nonce=binascii.b2a_hex(uuid.uuid4().bytes),
        oauth_version="1.0a",
    )
    args = {}
    args.update(baseArgs)
    args.update(parameters)
    signature = _oauth10a_signature(consumerToken, method, url, args,
                                     accessToken)
    baseArgs["oauth_signature"] = signature

    parameters.update(baseArgs)

    return parameters

  def getUrl(self, url, parameters=None, method="GET"):

    parameters = self.getParams(url, parameters, method)
    parameters = urlencode(parameters)

    return "%s?%s" % (url, parameters)

  def request(self, url, parameters=None, method="GET"):

    url = self.getUrl(url, parameters, method)

    if method == "POST":
      return requests.post(url)

    return requests.get(url)

  def requestJson(self, url, parameters=None, method="GET"):

    data = self.request(url, parameters, method).content

    return json.loads(data)
