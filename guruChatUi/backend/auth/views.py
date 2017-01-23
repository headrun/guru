import json
from django.views.decorators.csrf import ensure_csrf_cookie
from django.contrib.auth import authenticate, login as authLogin,\
                                logout as authLogout

from decorators import loginRequired
from common.utils import getHttpResponse as HttpResponse
from common.decorators import allowedMethods

def getUserData(user):

  return {"userId"   : user.id,\
          "userName" : user.username,\
          "firstName": user.first_name,\
          "lastName" : user.last_name,\
          "email"    : user.email}

@allowedMethods(["POST"])
def login(request):

  username = request.POST.get("username")
  password = request.POST.get("password")

  if not username and not password:
    body = request.body

    try:
      body = json.loads(body)
      username = body.get("username")
      password = body.get("password")

    except ValueError:
      pass

  user = authenticate(username=username, password=password)

  resp = None

  if user is not None:

    if user.is_active:
      authLogin(request, user)

      resp = HttpResponse(getUserData(user))

    else:
      resp = HttpResponse("User Not Active", error=1, status=401)

  else:
    resp = HttpResponse("Invalid Credentials", error=1, status=401)

  return resp

@allowedMethods(["GET"])
@loginRequired
def logout(request):

  authLogout(request)

  return HttpResponse("logged out")

@ensure_csrf_cookie
def status(request):

  if request.user.is_authenticated():
    return HttpResponse({"user": getUserData(request.user)})

  return HttpResponse("Invalid Login")
