"""
Django settings for d2qc project.

Generated by 'django-admin startproject' using Django 2.1.dev20180428173945.

For more information on this file, see
https://docs.djangoproject.com/en/dev/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/dev/ref/settings/
"""

from .base import *

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'REPLACE THIS WITH RANDOM KEY!!!!!'

# ALLOWED_HOSTS must be added here
ALLOWED_HOSTS = []


# Database
# https://docs.djangoproject.com/en/dev/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'd2qc',
        'USER': 'd2qc',
        'PASSWORD': 'd2qc', # ADD CORRECT PASSWORD
        'HOST': 'localhost',
        'PORT': '3306',
        'OPTIONS': {
            'init_command': 'SET storage_engine=INNODB',
        },
    }
}
