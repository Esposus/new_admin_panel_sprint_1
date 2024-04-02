import os
from pathlib import Path

from dotenv import load_dotenv
from split_settings.tools import include

load_dotenv()


BASE_DIR = Path(__file__).resolve().parent.parent

include(
    'components/apps.py',
    'components/auth_password_validators',
    'components/database.py',
    'components/middlewares.py',
    'components/templates.py',
)

SECRET_KEY = os.environ.get('SECRET_KEY')

DEBUG = os.environ.get('DEBUG') == 'True'

ALLOWED_HOSTS = ['127.0.0.1']

ROOT_URLCONF = 'config.urls'

WSGI_APPLICATION = 'config.wsgi.application'

LANGUAGE_CODE = 'ru-RU'

LOCALE_PATHS = ['movies/locale']

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True

STATIC_URL = 'static/'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
