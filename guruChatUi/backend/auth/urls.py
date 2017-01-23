import views
from django.conf.urls import url

urlpatterns = [
  url(r'^login/', views.login),
  url(r'^logout/', views.logout),
  url(r'^status/', views.status),
]
