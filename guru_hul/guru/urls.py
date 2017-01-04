from django.conf.urls import url
from . import views
from django.contrib.auth import views as auth_views

urlpatterns= [
    #url(r'^$', auth_views.login, {'template_name': 'login.html'}, name='login'),
    url(r'^$', auth_views.login, {'template_name': 'login.html', 'extra_context': {'next':'/home'}}, name='login'),
    url(r'^home', views.user_home),
    url(r'^logout', views.user_logout, name='logout'),
    url(r'^search', views.search_query, name='search_query'),
    url(r'^mail_gateway', views.send_mail, ),
    url(r'^queries', views.get_sample_queries, ),
   ]

