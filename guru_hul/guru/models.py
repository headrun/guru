from django.db import models
from django.contrib.auth.models import User

class scheduler(models.Model):
    user= models.OneToOneField(User, on_delete= models.CASCADE, related_name='profile')
    db_query= models.CharField(max_length= 500, null= True)
    nl_query= models.CharField(max_length= 1000, null= True)
    answer_type= models.CharField(max_length= 100, null= True)
    answer_format= models.CharField(max_length= 100)
    day_of_week= models.SmallIntegerField(null= True)
    duration= models.IntegerField(null=True)
    time= models.SmallIntegerField(null= True)
    next_trigger_on= models.DateTimeField(null= True)


