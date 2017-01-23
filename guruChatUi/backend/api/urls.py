import views
from django.conf.urls import url, include

urlpatterns = [
  url(r'^mailer/', include('mailer.urls')),
  url(r'^auth/', include('auth.urls')),


  url(r'^csrf/?$', views.csrf),
  url(r'^messages/?$', views.messages),
]
