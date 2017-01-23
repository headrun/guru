import os
import json
import re
import datetime

from django.http import HttpResponse
from django.utils.timezone import utc

def getHttpResponse(resp, error=0, status=200, content_type="application/json"):

  retVal = None

  if error:
    retVal = {"error": 1,\
              "msg"  : resp}
  else:
    retVal = {"error" : 0,\
              "result": resp}

  resp = json.dumps(retVal)

  return HttpResponse(resp, content_type, status)

def getUniqueStrByTime():

  now = datetime.datetime.now()

  return re.sub(r'\.|\:|\-|T', "", now.isoformat())

def getUnixTimeMillisec(date=datetime.datetime.now()):

  beginingOfTime = datetime.datetime(1970, 1, 1, tzinfo=utc)

  if isinstance(date, datetime.date) and not isinstance(date, datetime.datetime):
    beginingOfTime = beginingOfTime.date()

  seconds = date - beginingOfTime
  seconds = seconds.total_seconds()*1000

  return seconds

def handleFileUpload(reqFileObj, fileName):

  if not os.path.exists(os.path.dirname(fileName)):

    try:
      os.makedirs(os.path.dirname(fileName))
    except:
      if exc.errno != errno.EEXIST:
        raise OSError("Unable to create dir")

  with open(fileName, "w") as destination:

    for chunk in reqFileObj.chunks():
      destination.write(chunk)

  return fileName
