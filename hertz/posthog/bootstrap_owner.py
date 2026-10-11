"""Run via manage.py shell before publishing the gateway; never print credentials."""
import json
import os
from pathlib import Path
import secrets

from django.db import connection, transaction
from django.db.migrations.executor import MigrationExecutor

from posthog.models import Organization, Team, User

os.umask(0o077)
executor = MigrationExecutor(connection)
if executor.migration_plan(executor.loader.graph.leaf_nodes()):
    raise RuntimeError('Complete initial database migrations before bootstrapping owners')
output = Path('/tmp/kasanova-posthog-bootstrap.json')
if output.exists():
    raise RuntimeError('Bootstrap credentials already exist; preserve them')
if User.objects.exists() or Organization.objects.exists():
    raise RuntimeError('Instance already has owners; refuse to overwrite accounts')
password = secrets.token_urlsafe(36)
with transaction.atomic():
    organization, prod, user = User.objects.bootstrap(
        organization_name='Kasanova', email='ren@kasanova.io', password=password,
        first_name='Ren', is_staff=True, team_fields={'name': 'Kasanova PROD', 'autocapture_opt_out': True},
    )
    user.is_email_verified = True
    user.save(update_fields=['is_email_verified'])
    dev = Team.objects.create_with_data(initiating_user=user, organization=organization,
                                      name='Kasanova DEV', autocapture_opt_out=True)
    if prod.api_token == dev.api_token or prod.pk == dev.pk:
        raise RuntimeError('Project isolation failed')
    output.write_text(json.dumps({'url': 'https://posthog.kasanova.io', 'email': user.email,
                                  'password': password, 'organization_id': str(organization.pk),
                                  'projects': {'prod': {'id': prod.pk, 'api_key': prod.api_token},
                                               'dev': {'id': dev.pk, 'api_key': dev.api_token}}}, indent=2) + '\n')
    output.chmod(0o600)
print(json.dumps({'bootstrap_complete': True, 'email': user.email,
                  'projects': {'prod': prod.pk, 'dev': dev.pk}, 'credentials_printed': False}))
