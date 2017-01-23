from common.utils import getHttpResponse as HttpResponse

def loginRequired(view):

  def check(request, *args, **kwargs):

    if not request.user.is_authenticated():

      return HttpResponse("Invalid Login", error=1, status=401)

    return view(request, *args, **kwargs)

  return check
