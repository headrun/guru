from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.contrib.auth import views as auth_views
from django.contrib.auth import authenticate, login, logout
from django.template import RequestContext
#from .wit_ai import guru_main
from .api_ai.pandas import guru_main

#Create your views here.
def user_home(request):
    print(request.user, type(request.user))
    username = password = ''
    if request.user.is_authenticated:
        print("authenticated")
        return render(request, 'index.html')

    return redirect('/')

def get_sample_queries(request):
    return render(request, 'sample_queries.html', context= {})

def user_logout(request):
    if request.user and  request.user.is_authenticated:
        print('user logout')
        logout(request)
    return redirect('/')

def search_query(request):
    return guru_main.get_answer(request)

def send_mail(request):
    print(request)
    return guru_main.send_mail(request)
