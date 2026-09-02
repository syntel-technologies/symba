#!/bin/sh
set -eu

fail() {
    echo "symba release admission failed: $1" >&2
    exit 78
}

provenance_dir=${1:-/usr/local/share/symba}
image_release_id_file="$provenance_dir/release-id"
image_manifest_sha_file="$provenance_dir/release-manifest-sha256"

for path in "$image_release_id_file" "$image_manifest_sha_file"; do
    [ -f "$path" ] || fail "image_provenance_missing"
    [ ! -L "$path" ] || fail "image_provenance_symlink"
    [ "$(wc -l < "$path" | tr -d ' ')" = "1" ] \
        || fail "image_provenance_not_single_line"
done

image_release_id=$(cat "$image_release_id_file")
image_manifest_sha=$(cat "$image_manifest_sha_file")
desired_release_id=${KNOR_RELEASE_ID:-}
desired_manifest_sha=${KNOR_RELEASE_MANIFEST_SHA256:-}

validate_release_id() {
    value=$1
    case "$value" in
        ''|[._-]*|*[!A-Za-z0-9._-]*) fail "release_id_invalid" ;;
    esac
    [ "${#value}" -le 128 ] || fail "release_id_invalid"
}

validate_manifest_sha() {
    value=$1
    if [ "$value" = "unreleased" ]; then
        return
    fi
    [ "${#value}" -eq 64 ] || fail "manifest_sha256_invalid"
    case "$value" in
        *[!0-9a-f]*) fail "manifest_sha256_invalid" ;;
    esac
}

validate_release_id "$image_release_id"
validate_release_id "$desired_release_id"
validate_manifest_sha "$image_manifest_sha"
validate_manifest_sha "$desired_manifest_sha"

if [ "${SYMBA_RELEASE_REQUIRE_FINAL:-false}" = "true" ]; then
    [ "$image_manifest_sha" != "unreleased" ] \
        || fail "final_image_provenance_required"
    [ "$desired_manifest_sha" != "unreleased" ] \
        || fail "final_desired_provenance_required"
fi

[ "$image_release_id" = "$desired_release_id" ] \
    || fail "release_id_mismatch"
[ "$image_manifest_sha" = "$desired_manifest_sha" ] \
    || fail "manifest_sha256_mismatch"

