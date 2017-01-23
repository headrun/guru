import os
import json

from django.views.decorators.csrf import ensure_csrf_cookie
from django.conf import settings

from common.utils import getHttpResponse as HttpResponse
from common.decorators import allowedMethods

from auth.decorators import loginRequired

from .src import Api

@allowedMethods(["GET"])
@ensure_csrf_cookie
def csrf(request):

  return HttpResponse("")

@allowedMethods(["GET", "POST"])
@loginRequired
def messages(request):

  resp = {"messages": []}

  if request.method == "GET":
    after   = request.GET.get("after")
    before  = request.GET.get("before")
    count   = request.GET.get("count", 10)
    msgType = request.GET.get("type")

    user = request.user

    messages = Api.run("getMessages", user,\
                       {"after" : after,
                        "before": before,
                        "count" : count,
                        "type"  : msgType})

    resp["messages"]   = messages
    resp["pagination"] = {}

    if messages:
      url = request.path_info
      prevPage = "%s?before=%s" % (url, messages[0])
      nextPage = "%s?after=%s" % (url, messages[len(messages) - 1])

      resp["pagination"]["prev"] = prevPage
      resp["pagination"]["next"] = nextPage

    return HttpResponse(resp)

  body = request.body
  inputMsg = None

  try:
    body = json.loads(body)
    inputMsg = body.get("input")
  except ValueError:
    pass

  if not inputMsg:
    return HttpResponse("Invalid input", error=1)

  error, messages = Api.run("parse", request.user, inputMsg)

  if error:
    return HttpResponse(error, error=1)

  resp["messages"] = messages

  return HttpResponse(resp)
