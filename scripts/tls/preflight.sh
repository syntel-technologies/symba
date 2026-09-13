#!/bin/sh
set -eu

fail() {
    echo "symba runtime preflight failed: $1" >&2
    exit 78
}

require_value() {
    variable_name=$1
    eval "variable_value=\${$variable_name:-}"
    [ -n "$variable_value" ] || fail "required_configuration_missing"
}

/usr/local/bin/symba-verify-release

for variable_name in \
    SYMBA_POSTGRES_ADMIN_PASSWORD \
    SYMBA_POSTGRES_MIGRATION_PASSWORD \
    SYMBA_POSTGRES_RUNTIME_PASSWORD \
    SYMBA_DEV_TOKEN \
    SYMBA_REDIS_PASSWORD \
    SYMBA_GRPC_TLS_EXPECTED_DNS_NAME \
    SYMBA_GRPC_TLS_EXPECTED_IP \
    SYMBA_GRPC_TLS_EXPECTED_CERT_SHA256 \
    SYMBA_GRPC_TLS_EXPECTED_CA_SHA256 \
    SYMBA_GRPC_TLS_MIN_REMAINING_SECONDS \
    SYMBA_TLS_KEY_EXPECTED_OWNER; do
    require_value "$variable_name"
done

if [ "${#SYMBA_REDIS_PASSWORD}" -lt 32 ] \
    || [ "${#SYMBA_DEV_TOKEN}" -lt 32 ] \
    || [ "${#SYMBA_POSTGRES_ADMIN_PASSWORD}" -lt 32 ] \
    || [ "${#SYMBA_POSTGRES_MIGRATION_PASSWORD}" -lt 32 ] \
    || [ "${#SYMBA_POSTGRES_RUNTIME_PASSWORD}" -lt 32 ]; then
    fail "secret_too_short"
fi

for secret in \
    "$SYMBA_REDIS_PASSWORD" \
    "$SYMBA_DEV_TOKEN"; do
    case "$secret" in
        *[!A-Za-z0-9_-]*|'') fail "url_safe_secret_invalid" ;;
    esac
done

[ "$SYMBA_POSTGRES_ADMIN_PASSWORD" != "$SYMBA_POSTGRES_MIGRATION_PASSWORD" ] \
    || fail "secrets_not_independent"
[ "$SYMBA_POSTGRES_ADMIN_PASSWORD" != "$SYMBA_POSTGRES_RUNTIME_PASSWORD" ] \
    || fail "secrets_not_independent"
[ "$SYMBA_POSTGRES_MIGRATION_PASSWORD" != "$SYMBA_POSTGRES_RUNTIME_PASSWORD" ] \
    || fail "secrets_not_independent"
for database_secret in \
    "$SYMBA_POSTGRES_ADMIN_PASSWORD" \
    "$SYMBA_POSTGRES_MIGRATION_PASSWORD" \
    "$SYMBA_POSTGRES_RUNTIME_PASSWORD"; do
    [ "$SYMBA_DEV_TOKEN" != "$database_secret" ] \
        || fail "secrets_not_independent"
    [ "$SYMBA_REDIS_PASSWORD" != "$database_secret" ] \
        || fail "secrets_not_independent"
done
[ "$SYMBA_REDIS_PASSWORD" != "$SYMBA_DEV_TOKEN" ] \
    || fail "secrets_not_independent"

certificate=/run/secrets/symba/server.crt
private_key=/run/secrets/symba/server.key
ca_certificate=/run/secrets/symba/ca.crt

for path in "$certificate" "$private_key" "$ca_certificate"; do
    [ -f "$path" ] || fail "tls_file_missing"
    [ ! -L "$path" ] || fail "tls_file_symlink"
    [ -r "$path" ] || fail "tls_file_unreadable"
    [ -s "$path" ] || fail "tls_file_empty"
    mount_options=$(
        awk -v target="$path" '$5 == target { print $6 }' /proc/self/mountinfo
    )
    case ",$mount_options," in
        *,ro,*) ;;
        *) fail "tls_file_mount_not_read_only" ;;
    esac
done
[ "$(stat -c '%u:%g %a' "$private_key")" = "$SYMBA_TLS_KEY_EXPECTED_OWNER 600" ] \
    || fail "tls_private_key_metadata_invalid"

case "$SYMBA_GRPC_TLS_MIN_REMAINING_SECONDS" in
    ''|*[!0-9]*) fail "tls_min_remaining_seconds_invalid" ;;
esac
[ "$SYMBA_GRPC_TLS_MIN_REMAINING_SECONDS" -ge 3600 ] \
    || fail "tls_min_remaining_seconds_invalid"

normalize_fingerprint() {
    printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -d ':'
}

expected_certificate_fingerprint=$(normalize_fingerprint "$SYMBA_GRPC_TLS_EXPECTED_CERT_SHA256")
expected_ca_fingerprint=$(normalize_fingerprint "$SYMBA_GRPC_TLS_EXPECTED_CA_SHA256")
for fingerprint in "$expected_certificate_fingerprint" "$expected_ca_fingerprint"; do
    [ "${#fingerprint}" -eq 64 ] || fail "tls_fingerprint_invalid"
    case "$fingerprint" in
        *[!0-9a-f]*) fail "tls_fingerprint_invalid" ;;
    esac
done

[ "$(grep -c '^-----BEGIN CERTIFICATE-----$' "$certificate" || true)" = "1" ] \
    || fail "tls_certificate_pem_invalid"
[ "$(grep -c '^-----BEGIN CERTIFICATE-----$' "$ca_certificate" || true)" = "1" ] \
    || fail "tls_ca_pem_invalid"
grep -Eq '^-----BEGIN (RSA |EC |ENCRYPTED )?PRIVATE KEY-----$' "$private_key" \
    || fail "tls_private_key_pem_invalid"

openssl x509 -in "$certificate" -noout >/dev/null 2>&1 \
    || fail "tls_certificate_parse_failed"
openssl x509 -in "$ca_certificate" -noout >/dev/null 2>&1 \
    || fail "tls_ca_parse_failed"
openssl pkey -in "$private_key" -passin pass: -check -noout >/dev/null 2>&1 \
    || fail "tls_private_key_parse_failed"

openssl x509 -in "$certificate" -checkend "$SYMBA_GRPC_TLS_MIN_REMAINING_SECONDS" -noout \
    >/dev/null 2>&1 || fail "tls_certificate_expired_or_expiring"
openssl x509 -in "$ca_certificate" -checkend "$SYMBA_GRPC_TLS_MIN_REMAINING_SECONDS" -noout \
    >/dev/null 2>&1 || fail "tls_ca_expired_or_expiring"

certificate_fingerprint=$(
    openssl x509 -in "$certificate" -noout -sha256 -fingerprint \
        | sed 's/^[^=]*=//' | tr '[:upper:]' '[:lower:]' | tr -d ':'
)
ca_fingerprint=$(
    openssl x509 -in "$ca_certificate" -noout -sha256 -fingerprint \
        | sed 's/^[^=]*=//' | tr '[:upper:]' '[:lower:]' | tr -d ':'
)
[ "$certificate_fingerprint" = "$expected_certificate_fingerprint" ] \
    || fail "tls_certificate_fingerprint_mismatch"
[ "$ca_fingerprint" = "$expected_ca_fingerprint" ] \
    || fail "tls_ca_fingerprint_mismatch"

openssl verify \
    -CAfile "$ca_certificate" \
    -purpose sslserver \
    -verify_hostname "$SYMBA_GRPC_TLS_EXPECTED_DNS_NAME" \
    "$certificate" >/dev/null 2>&1 \
    || fail "tls_dns_chain_verification_failed"
openssl verify \
    -CAfile "$ca_certificate" \
    -purpose sslserver \
    -verify_ip "$SYMBA_GRPC_TLS_EXPECTED_IP" \
    "$certificate" >/dev/null 2>&1 \
    || fail "tls_ip_chain_verification_failed"

subject_alt_name=$(openssl x509 -in "$certificate" -noout -ext subjectAltName 2>/dev/null) \
    || fail "tls_subject_alt_name_missing"
printf '%s\n' "$subject_alt_name" | grep -Fq 'DNS:' \
    || fail "tls_dns_subject_alt_name_missing"
printf '%s\n' "$subject_alt_name" | grep -Fq 'IP Address:' \
    || fail "tls_ip_subject_alt_name_missing"
openssl x509 -in "$certificate" -noout -checkhost "$SYMBA_GRPC_TLS_EXPECTED_DNS_NAME" \
    >/dev/null 2>&1 || fail "tls_dns_subject_alt_name_mismatch"
openssl x509 -in "$certificate" -noout -checkip "$SYMBA_GRPC_TLS_EXPECTED_IP" \
    >/dev/null 2>&1 || fail "tls_ip_subject_alt_name_mismatch"

extended_key_usage=$(openssl x509 -in "$certificate" -noout -ext extendedKeyUsage 2>/dev/null) \
    || fail "tls_extended_key_usage_missing"
printf '%s\n' "$extended_key_usage" | grep -Eq 'TLS Web Server Authentication|serverAuth' \
    || fail "tls_server_auth_eku_missing"
key_usage=$(openssl x509 -in "$certificate" -noout -ext keyUsage 2>/dev/null) \
    || fail "tls_key_usage_missing"
printf '%s\n' "$key_usage" | grep -Fq 'Digital Signature' \
    || fail "tls_digital_signature_usage_missing"
basic_constraints=$(openssl x509 -in "$certificate" -noout -ext basicConstraints 2>/dev/null) \
    || fail "tls_basic_constraints_missing"
printf '%s\n' "$basic_constraints" | grep -Fq 'CA:FALSE' \
    || fail "tls_leaf_basic_constraints_invalid"
ca_basic_constraints=$(openssl x509 -in "$ca_certificate" -noout -ext basicConstraints 2>/dev/null) \
    || fail "tls_ca_basic_constraints_missing"
printf '%s\n' "$ca_basic_constraints" | grep -Fq 'CA:TRUE' \
    || fail "tls_ca_basic_constraints_invalid"

umask 077
certificate_public_key=$(mktemp /tmp/symba-cert-public-key.XXXXXX) \
    || fail "tls_temporary_file_failed"
private_public_key=$(mktemp /tmp/symba-private-public-key.XXXXXX) \
    || fail "tls_temporary_file_failed"
cleanup() {
    rm -f "$certificate_public_key" "$private_public_key"
}
trap cleanup EXIT HUP INT TERM
openssl x509 -in "$certificate" -pubkey -noout > "$certificate_public_key" 2>/dev/null \
    || fail "tls_certificate_public_key_failed"
openssl pkey -in "$private_key" -passin pass: -pubout > "$private_public_key" 2>/dev/null \
    || fail "tls_private_public_key_failed"
cmp -s "$certificate_public_key" "$private_public_key" \
    || fail "tls_certificate_private_key_mismatch"

public_key_algorithm=$(
    openssl x509 -in "$certificate" -noout -text \
        | sed -n 's/^[[:space:]]*Public Key Algorithm: //p' \
        | head -n 1
)
case "$public_key_algorithm" in
    rsaEncryption|rsassaPss)
        public_key_bits=$(
            openssl pkey -pubin -in "$certificate_public_key" -noout -text \
                | sed -n 's/^Public-Key: (\([0-9][0-9]*\) bit).*/\1/p' \
                | head -n 1
        )
        [ -n "$public_key_bits" ] && [ "$public_key_bits" -ge 3072 ] \
            || fail "tls_rsa_key_too_weak"
        ;;
    id-ecPublicKey)
        public_key_curve=$(
            openssl pkey -pubin -in "$certificate_public_key" -noout -text \
                | sed -n 's/^ASN1 OID: //p' \
                | head -n 1
        )
        case "$public_key_curve" in
            prime256v1|secp384r1|secp521r1) ;;
            *) fail "tls_ec_curve_not_allowed" ;;
        esac
        ;;
    ED25519|ED448) ;;
    *) fail "tls_public_key_algorithm_not_allowed" ;;
esac

signature_algorithm=$(
    openssl x509 -in "$certificate" -noout -text \
        | sed -n 's/^[[:space:]]*Signature Algorithm: //p' \
        | head -n 1 \
        | tr '[:upper:]' '[:lower:]'
)
case "$signature_algorithm" in
    *sha256*|*sha384*|*sha512*|ed25519|ed448) ;;
    *) fail "tls_signature_algorithm_not_allowed" ;;
esac

trap - EXIT HUP INT TERM
cleanup
echo "symba runtime preflight passed"
