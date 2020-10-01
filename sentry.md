To configure the SDK, initialize it with the Django integration in your settings.py file:

import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration

sentry_sdk.init(
    dsn="https://8dfb388cedb2480695adafd613e0a7ac@o455936.ingest.sentry.io/5448250",
    integrations=[DjangoIntegration()],
    traces_sample_rate=1.0,

    # If you wish to associate users to errors (assuming you are using
    # django.contrib.auth) you may enable sending PII data.
    send_default_pii=True
)
The above configuration captures both error and performance data. To reduce the volume of performance data captured, change traces_sample_rate to a value between 0 and 1.

You can easily verify your Sentry installation by creating a route that triggers an error:

from django.urls import path

def trigger_error(request):
    division_by_zero = 1 / 0

urlpatterns = [
    path('sentry-debug/', trigger_error),
    # ...
]
Visiting this route will trigger an error that will be captured by Sentry.
