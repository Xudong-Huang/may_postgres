#!/bin/bash
# Test fixtures for may_postgres' suite, applied by the postgres image's
# docker-entrypoint-initdb.d hook.
#
# The suite exercises each of PostgreSQL's password authentication methods, so
# it needs one role per method and a pg_hba.conf that routes them accordingly.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-'SQL'
    -- Extensions used by the type round-trip tests.
    CREATE EXTENSION IF NOT EXISTS citext;
    CREATE EXTENSION IF NOT EXISTS hstore;

    -- One role per authentication method. `password_encryption` decides how the
    -- verifier is stored, which is what makes md5 and scram distinguishable;
    -- cleartext `password` auth works against either, so pass_user can reuse md5.
    SET password_encryption TO 'md5';
    CREATE ROLE md5_user LOGIN PASSWORD 'password';
    CREATE ROLE pass_user LOGIN PASSWORD 'password';

    SET password_encryption TO 'scram-sha-256';
    CREATE ROLE scram_user LOGIN PASSWORD 'password';
SQL

# Written wholesale rather than appended: pg_hba is first-match-wins, so the
# per-role lines must precede the catch-all that the image generates, and
# appending would leave them unreachable.
cat > "$PGDATA/pg_hba.conf" <<-'HBA'
	# TYPE  DATABASE  USER        ADDRESS       METHOD
	local   all       all                       trust

	host    all       md5_user    all           md5
	host    all       pass_user   all           password
	host    all       scram_user  all           scram-sha-256

	# Everything else, including the postgres superuser the tests connect as.
	host    all       all         all           trust
HBA
