#!/bin/sh
set -eu

fail() {
    echo "symba postgres initialization failed: $1" >&2
    exit 78
}

[ "${POSTGRES_USER:-}" = "postgres" ] || fail "bootstrap_user_invalid"
[ "${POSTGRES_DB:-}" = "postgres" ] || fail "bootstrap_database_invalid"
[ "${POSTGRES_INITDB_ARGS:-}" = "--auth-host=scram-sha-256 --auth-local=peer" ] \
    || fail "initdb_auth_contract_invalid"
[ "${POSTGRES_HOST_AUTH_METHOD:-}" = "scram-sha-256" ] \
    || fail "host_auth_contract_invalid"

: "${PGDATA:?required}"
hba="$PGDATA/pg_hba.conf"
[ -f "$hba" ] || fail "pg_hba_missing"
[ ! -L "$hba" ] || fail "pg_hba_symlink"
[ "$(awk '!/^[[:space:]]*#/ && $1 == "host" && $2 == "all" && $3 == "all" && $4 == "all" && $5 == "scram-sha-256" { count++ } END { print count + 0 }' "$hba")" = "1" ] \
    || fail "generic_scram_rule_invalid"
[ "$(awk '!/^[[:space:]]*#/ && $1 == "local" && $2 == "all" && $3 == "all" && $4 == "peer" { count++ } END { print count + 0 }' "$hba")" = "1" ] \
    || fail "local_peer_rule_invalid"
[ "$(awk '!/^[[:space:]]*#/ && $1 == "local" && $2 == "replication" && $3 == "all" && $4 == "peer" { count++ } END { print count + 0 }' "$hba")" = "1" ] \
    || fail "local_replication_peer_rule_invalid"
[ "$(awk '!/^[[:space:]]*#/ && $1 == "host" && $2 == "all" && $3 == "all" && ($4 == "127.0.0.1/32" || $4 == "::1/128") && $5 == "scram-sha-256" { count++ } END { print count + 0 }' "$hba")" = "2" ] \
    || fail "loopback_scram_rules_invalid"
[ "$(grep -Fxc 'host all postgres all reject' "$hba" || true)" = "0" ] \
    || fail "bootstrap_reject_rule_already_present"
[ "$(grep -Fxc 'host replication postgres all reject' "$hba" || true)" = "0" ] \
    || fail "bootstrap_reject_rule_already_present"

umask 077
hba_tmp=$(mktemp "$PGDATA/pg_hba.conf.symba.XXXXXX") \
    || fail "pg_hba_temporary_file_failed"
cleanup() {
    rm -f "$hba_tmp"
}
trap cleanup EXIT HUP INT TERM

cp -p "$hba" "$hba_tmp" || fail "pg_hba_metadata_copy_failed"
{
    printf '%s\n' \
        'host replication postgres all reject' \
        'host all postgres all reject'
    cat "$hba"
} > "$hba_tmp" || fail "pg_hba_render_failed"

[ "$(grep -Fxc 'host replication postgres all reject' "$hba_tmp" || true)" = "1" ] \
    || fail "pg_hba_render_invalid"
[ "$(grep -Fxc 'host all postgres all reject' "$hba_tmp" || true)" = "1" ] \
    || fail "pg_hba_render_invalid"

mv -f "$hba_tmp" "$hba" || fail "pg_hba_atomic_replace_failed"
trap - EXIT HUP INT TERM
