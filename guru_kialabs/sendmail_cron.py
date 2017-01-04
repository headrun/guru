import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'project_guru.settings')
import django
django.setup()

from django.contrib.auth.models import User
from django.utils import timezone
from guru.api_ai.pandas.guru_main import get_query_results, generate_report_and_send_mail
print('started')
while(True):
    users= User.objects.all()
    for user in users:
        if user.profile.next_trigger_on and (user.profile.next_trigger_on - timezone.now()).days < 0:
            answer = get_query_results(db_query=eval(user.profile.db_query), answer_type=user.profile.answer_type, raw_info = {})
            print(answer)
            answer['format'] = eval(user.profile.answer_format)
            res = generate_report_and_send_mail(raw_result=answer, nl_query=user.profile.nl_query, email_address=user.email)
            print(res)
            t1 = timezone.now()
            print("\n\n\nTrigged to: ", user.email, "at", t1)
            user.profile.next_trigger_on = t1 + timezone.timedelta(seconds=int(user.profile.duration))
            user.profile.save()
            print("Next Trigger : After", user.profile.duration, 'seconds')



