import os
os.environ['DJANGO_SETTINGS_MODULE']= 'project_guru.settings'
import django
django.setup()

from django.contrib.auth.models import User
from guru.models import scheduler

username= input('\nUsername: ')
email= input('\nEmail: ')
password= input('\nPassword: ')
u= scheduler(user= User.objects.create_superuser(username, email, password))
u.save()
print(u)

