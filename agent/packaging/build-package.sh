#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="${DIST_DIR:-$AGENT_DIR/dist}"
AGENT_VERSION="${AGENT_VERSION:-0.1.0-dev}"
PLATFORM="linux"
ARCHITECTURE="amd64"

GH_OST_VERSION="1.1.10"
GH_OST_URL="https://github.com/github/gh-ost/releases/download/v1.1.10/gh-ost-binary-linux-amd64-20260604164252.tar.gz"
GH_OST_SHA256="6dcc1d9a36b4de14c49490e710d244b75717fa80cc35c65f20e06d7e4efa28b2"
PT_TOOLKIT_VERSION="3.7.1-4"
PT_OSC_URL="https://percona.com/get/pt-online-schema-change"
PT_OSC_SHA256="26fa107b0204c11d346e23e6ab42342c4442d085c5e181aa91fdbf4dc87d794d"
PT_ARCHIVER_URL="https://percona.com/get/pt-archiver"
PT_ARCHIVER_SHA256="2695943b1843cfa0c65d85eb82efd0878df3e5aff918f3e4bde3c5b92c992c69"

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

download_verified() {
  local url="$1"
  local sha256="$2"
  local destination="$3"

  curl -fsSL --retry 3 "$url" -o "$destination"
  echo "$sha256  $destination" | sha256sum --check --status
}

for command_name in curl go sha256sum tar; do
  require_command "$command_name"
done

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

package_name="datamingle-agent-${AGENT_VERSION}-${PLATFORM}-${ARCHITECTURE}"
package_root="$work_dir/$package_name"
mkdir -p "$package_root/bin" "$package_root/data/tools" "$DIST_DIR"
cp "$SCRIPT_DIR/README.md" "$package_root/README.md"

(
  cd "$AGENT_DIR"
  CGO_ENABLED=0 GOOS="$PLATFORM" GOARCH="$ARCHITECTURE" \
    go build -trimpath -o "$package_root/bin/datamingle-agent" ./cmd/datamingle-agent
)

gh_ost_archive="$work_dir/gh-ost.tar.gz"
gh_ost_dir="$package_root/data/tools/gh-ost/$GH_OST_VERSION/$PLATFORM-$ARCHITECTURE"
mkdir -p "$gh_ost_dir"
download_verified "$GH_OST_URL" "$GH_OST_SHA256" "$gh_ost_archive"
tar -xzf "$gh_ost_archive" -C "$gh_ost_dir" gh-ost
chmod 0755 "$gh_ost_dir/gh-ost"
printf '%s' "$GH_OST_SHA256" > "$gh_ost_dir/gh-ost.sha256"

pt_osc_dir="$package_root/data/tools/pt-online-schema-change/$PT_TOOLKIT_VERSION/$PLATFORM-$ARCHITECTURE"
mkdir -p "$pt_osc_dir"
download_verified "$PT_OSC_URL" "$PT_OSC_SHA256" "$pt_osc_dir/pt-online-schema-change"
chmod 0755 "$pt_osc_dir/pt-online-schema-change"
printf '%s' "$PT_OSC_SHA256" > "$pt_osc_dir/pt-online-schema-change.sha256"

pt_archiver_dir="$package_root/data/tools/pt-archiver/$PT_TOOLKIT_VERSION/$PLATFORM-$ARCHITECTURE"
mkdir -p "$pt_archiver_dir"
download_verified "$PT_ARCHIVER_URL" "$PT_ARCHIVER_SHA256" "$pt_archiver_dir/pt-archiver"
chmod 0755 "$pt_archiver_dir/pt-archiver"
printf '%s' "$PT_ARCHIVER_SHA256" > "$pt_archiver_dir/pt-archiver.sha256"

archive_path="$DIST_DIR/$package_name.tar.gz"
tar -C "$work_dir" -czf "$archive_path" "$package_name"
sha256sum "$archive_path" > "$archive_path.sha256"
echo "$archive_path"
