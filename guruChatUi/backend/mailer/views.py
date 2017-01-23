from django.shortcuts import render
from common.utils import getHttpResponse as HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from mailer import send_mail

# Create your views here.


@csrf_exempt
def contact_us_email(request):

    client_email = request.POST.get("email", "")
    subject = request.POST.get('subject', "")
    client_name = request.POST.get("name", "")
    message = request.POST.get("message", "")
    body = "<html><body><p>Name : <b>" + client_name + "</b></p><p>" + "Email : <b>" + client_email + "</b></p>"
    body += "<p>Message : <b>" + message + "</b></p>"
    body += "<p>From App : <b> Buzzwall </b></p>"
    body += "</body></html>"
    if client_email:
        try:
            send_mail(subject, body, ["pamidi@buzzinga.com"], [], settings.DEFAULT_FROM_EMAIL)
            return HttpResponse('Success', error=0, status=200)
        except Exception as e:
            if settings.DEBUG:
                print e.message
    return HttpResponse('Error', error=1, status=500)

