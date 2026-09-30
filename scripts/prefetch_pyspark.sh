#!/usr/bin/env bash
# Downloads pyspark's sdist once into .docker-cache/, resumably (curl -C -),
# so the stream-processor and airflow Docker builds install it from a local
# file instead of the network -- see the comment above the COPY in both
# Dockerfiles. pyspark is a ~320MB download; fetching it once here, outside
# any concurrent multi-image `docker compose build`, avoids the bandwidth
# contention (and the resulting read-timeout failures) that a `make up-full`
# hits when 7 images pip-install at once over a modest connection.
#
# Safe to re-run: skips the download if the file already exists with the
# right size, and verifies the sha256 against PyPI's published digest either
# way.
set -euo pipefail

VERSION="${PYSPARK_VERSION:-3.5.9}"
CACHE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.docker-cache"
FILE="$CACHE_DIR/pyspark-${VERSION}.tar.gz"

mkdir -p "$CACHE_DIR"

URL="$(python3 -c "
import urllib.request, json, sys
with urllib.request.urlopen('https://pypi.org/pypi/pyspark/${VERSION}/json', timeout=10) as r:
    data = json.load(r)
for f in data['urls']:
    if f['packagetype'] == 'sdist':
        print(f['url']); sys.exit()
")"
EXPECTED_SHA="$(python3 -c "
import urllib.request, json, sys
with urllib.request.urlopen('https://pypi.org/pypi/pyspark/${VERSION}/json', timeout=10) as r:
    data = json.load(r)
for f in data['urls']:
    if f['packagetype'] == 'sdist':
        print(f['digests']['sha256']); sys.exit()
")"

echo "Fetching pyspark ${VERSION} into $FILE (resumable; safe to interrupt and re-run)..."
curl -C - -o "$FILE" "$URL" --retry 10 --retry-delay 3 --retry-all-errors

ACTUAL_SHA="$(sha256sum "$FILE" | cut -d' ' -f1)"
if [ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]; then
  echo "Checksum mismatch! expected $EXPECTED_SHA, got $ACTUAL_SHA" >&2
  echo "Deleting corrupt download; re-run this script." >&2
  rm -f "$FILE"
  exit 1
fi

echo "OK: $FILE ($(stat -c%s "$FILE") bytes, sha256 verified)"
