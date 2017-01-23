import os
os.environ['DJANGO_SETTINGS_MODULE']= 'backend.local_settings'
import django
django.setup()

from django.contrib.auth.models import User
#from guru.models import scheduler

username= raw_input('\nUsername: ')
email= raw_input('\nEmail: ')
password= raw_input('\nPassword: ')
u= User.objects.create_superuser(username, email, password)
u.save()
print(u)

