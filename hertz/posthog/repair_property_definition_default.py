#!/usr/bin/env python3
"""Restore the installed upstream migration's missing default; never update rows."""
import argparse
import ast
import fcntl
import hashlib
import json
import subprocess
from pathlib import Path

PROJECT = 'kasanova_posthog'
MIGRATION = '/code/posthog/migrations/1013_eventdefinition_enforcement_mode_db_default.py'
MIGRATION_SHA256 = '8b451fc13b0b1848aa9b283bb002077c6e23052306afdd2ec315ab2e3841f136'
SQL = "ALTER TABLE posthog_eventdefinition ALTER COLUMN enforcement_mode SET DEFAULT 'allow'"


def migration_statement(source):
    if hashlib.sha256(source).hexdigest() != MIGRATION_SHA256:
        raise RuntimeError('Installed upstream migration drift')
    calls = [n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and n.func.attr == 'RunSQL']
    statements = [ast.literal_eval(k.value) for n in calls for k in n.keywords if k.arg == 'sql']
    if statements != [SQL]:
        raise RuntimeError('Unexpected upstream migration operation')
    return statements[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--checkpoint', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    checkpoint = args.checkpoint.resolve()
    if checkpoint.parent != Path('/home/ren/kasanova-archives/posthog'):
        raise RuntimeError('Unexpected checkpoint')
    manifest = json.loads((checkpoint / 'manifest.json').read_text())
    restored = json.loads((checkpoint / 'restore-verification.json').read_text())
    if (manifest['state'] != 'cold_checkpoint_complete'
        or not restored.get('postgresql_owner_and_projects_verified')
        or not restored.get('analytics_journal_all_rows_and_pending_verified')
        or not restored.get('owned_temporary_restore_resources_cleaned')):
        raise RuntimeError('Verified native checkpoint required before schema repair')
    run = lambda argv: subprocess.check_output(argv, stderr=subprocess.PIPE)
    ids = run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + PROJECT]).decode().split()
    owned = json.loads(run(['docker', 'inspect', *ids]))
    services = {c['Config']['Labels']['com.docker.compose.service']: c for c in owned}
    for name in ('db', 'web', 'property-defs-rs'):
        c = services[name]
        if c['Config']['Labels'].get('com.docker.compose.project') != PROJECT or not c['State']['Running']:
            raise RuntimeError('Owned database writer is unavailable')
    source = run(['docker', 'exec', services['web']['Name'], 'cat', MIGRATION])
    statement = migration_statement(source)
    def query(sql):
        return run(['docker', 'exec', services['db']['Name'], 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                    '-U', 'posthog', '-d', 'posthog', '-At', '-c', sql]).decode().strip()
    sql = "SELECT COALESCE(column_default,'<missing>')||'|'||is_nullable||'|'||data_type||'|'||character_maximum_length FROM information_schema.columns WHERE table_schema='public' AND table_name='posthog_eventdefinition' AND column_name='enforcement_mode'"
    before = query(sql)
    if before not in ("<missing>|NO|character varying|10", "'allow'::character varying|NO|character varying|10"):
        raise RuntimeError('Unexpected field definition; preserve it')
    if query("SELECT count(*) FROM django_migrations WHERE app='posthog' AND name='1013_eventdefinition_enforcement_mode_db_default'") != '1':
        raise RuntimeError('Native migration not recorded; use normal migration workflow')
    with Path('/home/ren/Kasanova/tools/posthog/operations/.backup.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.apply and before.startswith('<missing>'):
            # Exact installed migration SQL. DDL changes only the future insert
            # default, preserves NOT NULL, and never rewrites existing values.
            query("BEGIN; SET LOCAL lock_timeout='5s'; LOCK TABLE posthog_eventdefinition IN ACCESS EXCLUSIVE MODE; "
                  "DO $$ BEGIN IF (SELECT column_default IS NOT NULL FROM information_schema.columns "
                  "WHERE table_schema='public' AND table_name='posthog_eventdefinition' AND column_name='enforcement_mode') "
                  "THEN RAISE EXCEPTION 'Default changed since inspection'; END IF; END $$; " + statement + '; COMMIT;')
        after = query(sql)
    if args.apply and after != "'allow'::character varying|NO|character varying|10":
        raise RuntimeError('Default repair did not verify')
    print(json.dumps({'checkpoint': str(checkpoint), 'migration_sha256': MIGRATION_SHA256,
        'before': before, 'after': after, 'applied': args.apply and before != after,
        'existing_rows_modified': False, 'rollback_sql': 'ALTER TABLE posthog_eventdefinition ALTER COLUMN enforcement_mode DROP DEFAULT'}))


if __name__ == '__main__':
    main()
