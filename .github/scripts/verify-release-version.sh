#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "用法: $0 <crate-name> [cargo-bin]" >&2
  exit 2
fi

crate_name="$1"
cargo_bin="${2:-cargo}"
if [[ ! "$crate_name" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "crate 名不合法: $crate_name" >&2
  exit 1
fi
repository_root="$(cd "$(dirname "$0")/../.." && pwd)"
metadata="$("$cargo_bin" metadata --locked --no-deps --format-version 1 --manifest-path "$repository_root/Cargo.toml")"
package_count="$(printf '%s' "$metadata" | jq -r --arg name "$crate_name" '[.packages[] | select(.name == $name)] | length')"
if [[ "$package_count" != "1" ]]; then
  echo "工作区必须恰好存在一个 crate $crate_name，实际数量: $package_count" >&2
  exit 1
fi
version="$(printf '%s' "$metadata" | jq -r --arg name "$crate_name" '.packages[] | select(.name == $name) | .version')"
if [[ ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "$crate_name 只能通过该通道发布稳定 SemVer，当前版本: $version" >&2
  exit 1
fi

query_root="$(mktemp -d "${RUNNER_TEMP:-/tmp}/release-version-${crate_name}-${version}.XXXXXX")"
# 业务作用：只清理本次 crates.io 版本查询创建的临时目录，避免跨 crate 误删发布证据。
# 参数说明：无。
# 返回：目录存在时精确删除；目录已经不存在时保持幂等成功。
cleanup() {
  if [[ -n "$query_root" && -d "$query_root" ]]; then
    rm -rf -- "$query_root"
  fi
}
trap cleanup EXIT

response_file="$query_root/response.json"
http_code="$(curl --silent --show-error --output "$response_file" --write-out '%{http_code}' \
  --user-agent "nasa-runtime-release/1.0" "https://crates.io/api/v1/crates/${crate_name}")"
case "$http_code" in
  200)
    registry_version="$(jq -r '.crate.max_stable_version // empty' "$response_file")"
    if [[ -z "$registry_version" ]]; then
      echo "$crate_name 的 crates.io 响应缺少 max_stable_version" >&2
      exit 1
    fi
    if [[ ! "$registry_version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
      echo "$crate_name 的 crates.io 最高稳定版本不是受支持的 SemVer: $registry_version" >&2
      exit 1
    fi
    IFS=. read -r local_major local_minor local_patch <<< "$version"
    IFS=. read -r registry_major registry_minor registry_patch <<< "$registry_version"
    if (( local_major < registry_major \
      || (local_major == registry_major && local_minor < registry_minor) \
      || (local_major == registry_major && local_minor == registry_minor && local_patch < registry_patch) )); then
      echo "$crate_name 工作区版本 $version 低于 crates.io 最高稳定版本 $registry_version" >&2
      exit 1
    fi
    if [[ "$version" == "$registry_version" ]]; then
      echo "$crate_name $version 已是 crates.io 最高稳定版本，后续必须校验归档内容等价"
    else
      echo "$crate_name $version 高于 crates.io 最高稳定版本 $registry_version"
    fi
    ;;
  404)
    if [[ "$version" != "1.0.0" ]]; then
      echo "$crate_name 尚未存在于 crates.io，首次公开版本必须是 1.0.0，当前版本: $version" >&2
      exit 1
    fi
    echo "$crate_name 尚未存在于 crates.io，首次公开版本为 1.0.0"
    ;;
  *)
    echo "查询 $crate_name 的 crates.io 版本返回 HTTP $http_code" >&2
    cat "$response_file" >&2
    exit 1
    ;;
esac
