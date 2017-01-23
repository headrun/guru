from utils import getHttpResponse as HttpResponse

def allowedMethods(methods):

  def wrap(view):

    def check(request, *args, **kwargs):

      if request.method not in methods:
        return HttpResponse("Invalid Method", error=1, status=405)

      return view(request, *args, **kwargs)

    return check

  return wrap
