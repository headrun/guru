from random import random

# for queries that are supported by Guru.
_assertions = [
        "Here is what I found",
        "Check it out",
        "I found this for you",
        "Check this out",
        "Heres your result",
        "These results might of some usage to you.",
        "Got some results for you.",
        "These results might be of some interest to you",
        "Hang on, I Got these."
        ]

# for queries that are not supported by Guru.
_negatives = [
        "I didn't find anything for you",
        "I couldn't understand what you saying",
        "Didn't get you. Sorry",
        "Unable to get what you asked",
        "Couldn't analyse what you said",
        "I can't help you with that",
        "I don't understand that. Sorry",
        "Didn't understand that. Oops",
        "Sorry, couldn't understand what you just said."
]

# Not sufficient data.
_no_records = [
        "Sorry. Didn't find any records",
        "No data availabe. Sorry",
        "No records found. Sorry",
        "Available data not sufficient. Sorry",
        "Required data not available. Sorry",
        "No sufficient data available.",
        "Insufficient data to act upon. Sorry"
]

# if confidence is less than the defined threshold
_low_confidence = [
            "Im not sure what you said.",
            "Not quite sure what you mean.",
            "I did't get you quite right.",
            "Im not confident about what you said.",
            "I think Im missing something."
]

_send_mail_success = [
            "Mail successfully sent to {email}. It may appear as spam.",
            "Check your Inbox or Spam Folder. Mail successfully delivered to {email}",
            "Delieved to {email}. Please check your Inbox (or spam folder)",
]

def get_resp_positive(mesg=None):
    return mesg if mesg else _assertions[int(random()*len(_assertions))]

def get_resp_negative(mesg=None):
    return mesg if mesg else _negatives[int(random()*len(_negatives))]

def get_resp_no_records(mesg=None):
    return mesg if mesg else _no_records[int(random()*len(_no_records))]

def get_resp_confidence(mesg=None):
    return mesg if mesg else _low_confidence[int(random()*len(_low_confidence))]

def get_resp_mail_success(email, mesg=None):
    return mesg if mesg else _send_mail_success[int(random()*len(_send_mail_success))].format(email=email)

