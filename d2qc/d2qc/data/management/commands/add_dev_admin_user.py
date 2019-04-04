from django.contrib.auth import get_user_model
from d2qc.data.models import Profile
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

class Command(BaseCommand):
    '''### Only use on development environments ###
    Create a admin user with username admin, password 123. If this user
    already exists, just set the password to 123, so you can access the admin
    account easily.
    '''

    def handle(self, *args, **options):
        '''Restore the database
        '''
        if not hasattr(settings, 'DEV_ADMIN_PROPERTIES'):
            raise CommandError(
                "DEV_ADMIN_PROPERTIES not set in settings. See "
                + "/d2qc/d2qc/setup/sample.development.py for an example."
            )

        User = get_user_model()
        admin,b=User.objects.get_or_create(
            username=settings.DEV_ADMIN_PROPERTIES['user']
        )
        admin.is_superuser = True
        admin.is_staff = True
        admin.set_password(settings.DEV_ADMIN_PROPERTIES['password'])
        admin.email = settings.DEV_ADMIN_PROPERTIES['email']
        admin.save()
        admin.profile = Profile(user=admin)
        admin.profile.save()
