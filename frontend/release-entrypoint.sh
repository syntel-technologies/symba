#!/bin/sh
set -eu

fail() {
    echo "symba frontend release admission failed: $1" >&2
    exit 78
}

provenance_dir=/usr/local/share/symba
release_id_file="$provenance_dir/release-id"
manifest_sha_file="$provenance_dir/release-manifest-sha256"
for path in "$release_id_file" "$manifest_sha_file"; do
    [ -f "$path" ] || fail "image_provenance_missing"
    [ ! -L "$path" ] || fail "image_provenance_symlink"
    [ "$(wc -l < "$path" | tr -d ' ')" = "1" ] \
        || fail "image_provenance_not_single_line"
done

image_release_id=$(cat "$release_id_file")
image_manifest_sha=$(cat "$manifest_sha_file")
desired_release_id=${KNOR_RELEASE_ID:-}
desired_manifest_sha=${KNOR_RELEASE_MANIFEST_SHA256:-}

case "$image_release_id" in
    ''|[._-]*|*[!A-Za-z0-9._-]*) fail "release_id_invalid" ;;
esac
case "$desired_release_id" in
    ''|[._-]*|*[!A-Za-z0-9._-]*) fail "release_id_invalid" ;;
esac
[ "${#image_release_id}" -le 128 ] || fail "release_id_invalid"
[ "${#desired_release_id}" -le 128 ] || fail "release_id_invalid"
for value in "$image_manifest_sha" "$desired_manifest_sha"; do
    if [ "$value" != "unreleased" ]; then
        [ "${#value}" -eq 64 ] || fail "manifest_sha256_invalid"
        case "$value" in
            *[!0-9a-f]*) fail "manifest_sha256_invalid" ;;
        esac
    fi
done
if [ "${SYMBA_RELEASE_REQUIRE_FINAL:-false}" = "true" ]; then
    [ "$image_manifest_sha" != "unreleased" ] \
        || fail "final_image_provenance_required"
    [ "$desired_manifest_sha" != "unreleased" ] \
        || fail "final_desired_provenance_required"
fi
[ "$image_release_id" = "$desired_release_id" ] || fail "release_id_mismatch"
[ "$image_manifest_sha" = "$desired_manifest_sha" ] \
    || fail "manifest_sha256_mismatch"

exec /docker-entrypoint.sh "$@"
