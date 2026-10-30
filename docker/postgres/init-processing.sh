#!/bin/sh
set -eu

# Runs only when PostgreSQL initializes a NEW volume. Values are SQL parameters,
# never interpolated into SQL source or logged as connection URLs.
psql --username "$POSTGRES_USER" --dbname postgres --set ON_ERROR_STOP=1 \
    --set owner="$PROCESSING_DATABASE_USER" \
    --set owner_password="$PROCESSING_DATABASE_PASSWORD" \
    --set app_user="$PROCESSING_APP_USER" \
    --set app_password="$PROCESSING_APP_PASSWORD" \
    --set database="$PROCESSING_DATABASE_NAME" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L', :'owner', :'owner_password') \gexec
SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L', :'app_user', :'app_password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'database', :'owner') \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', :'database') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', :'database', :'app_user') \gexec
SQL

psql --username "$POSTGRES_USER" --dbname "$PROCESSING_DATABASE_NAME" \
    --set ON_ERROR_STOP=1 --set owner="$PROCESSING_DATABASE_USER" \
    --set app_user="$PROCESSING_APP_USER" <<'SQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO :"app_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"app_user";
SQL
