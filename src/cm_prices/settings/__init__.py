import os

if os.getenv('DJANGO_ENV') == 'prod':
    from .prod import *
else:
    from .dev import *
