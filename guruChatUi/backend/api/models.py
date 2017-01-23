from __future__ import unicode_literals

from django.db import models
from django.contrib.auth.models import User

from common.utils import getUnixTimeMillisec

class Conversation (models.Model):

  user    = models.ForeignKey(User)
  created = models.DateTimeField(auto_now_add=True)
  updated = models.DateTimeField(auto_now=True)

  @property
  def json(self):

    return {
      "id"     : self.id,
      "created": getUnixTimeMillisec(self.created),
      "updated": getUnixTimeMillisec(self.updated)
    }

  def __str__(self):

    return "Conv %s of user %s" % (self.id, self.user)

class Message (models.Model):

  conversation = models.ForeignKey(Conversation)
  data         = models.TextField(null=False, blank=False)

  raw_data = models.TextField(null=False, blank=False)
  types = (("html", "HTML"),
           ("js", "JavaScript"),
           ("text", "Text"))

  type    = models.CharField(max_length=4, choices=types)
  created = models.DateTimeField(auto_now_add=True)
  inAnswerTo = models.IntegerField(null=True, blank=True)
  worthSending = models.BooleanField(default=False)

  @property
  def json(self):

    return {
      "id"          : self.id,
      "conversation": self.conversation.id,
      "type"        : self.type,
      "data"        : self.data,
      "created"     : getUnixTimeMillisec(self.created),
      "inAnswerTo"  : self.inAnswerTo
    }

  def __str__(self):

    return "Message - %s of conv %s created at %s" % (self.type,
                                                      self.conversation,
                                                      self.created)
