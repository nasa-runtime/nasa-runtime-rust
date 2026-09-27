#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "用法: $0 <crate-name> [cargo-bin]" >&2
  exit 2
fi

crate_name="$1"
cargo_bin="${2:-cargo}"
repository_root="$(cd "$(dirname "$0")/../.." && pwd)"
metadata="$("$cargo_bin" metadata --locked --no-deps --format-version 1 --manifest-path "$repository_root/Cargo.toml")"
version="$(printf '%s' "$metadata" | jq -r --arg name "$crate_name" '.packages[] | select(.name == $name) | .version')"
if [[ -z "$version" || "$version" == "null" ]]; then
  echo "工作区不存在 crate: $crate_name" >&2
  exit 1
fi

resolution_root="$(mktemp -d "${RUNNER_TEMP:-/tmp}/registry-resolution-${crate_name}-${version}.XXXXXX")"
# 业务作用：只删除本次 mktemp 创建的独立解析目录，避免发布 runner 累积临时锁文件与索引结果。
# 参数说明：无。
# 返回：目录存在时删除后成功；目录已经不存在时保持幂等成功。
cleanup() {
  if [[ -n "$resolution_root" && -d "$resolution_root" ]]; then
    rm -rf -- "$resolution_root"
  fi
}
trap cleanup EXIT

mkdir -p "$resolution_root/src"
printf 'fn main() {}\n' > "$resolution_root/src/main.rs"
# 显式 workspace 边界阻止探针继承临时目录上层的成员约束与 path patch。
printf '[workspace]\n\n[package]\nname = "registry-resolution-probe"\nversion = "0.0.0"\nedition = "2021"\npublish = false\n\n[dependencies]\n%s = "=%s"\n' \
  "$crate_name" "$version" > "$resolution_root/Cargo.toml"

for attempt in $(seq 1 60); do
  if "$cargo_bin" metadata --format-version 1 --manifest-path "$resolution_root/Cargo.toml" \
    > "$resolution_root/metadata.json" 2> "$resolution_root/metadata.err"; then
    resolved_source="$(jq -r --arg name "$crate_name" --arg version "$version" \
      '.packages[] | select(.name == $name and .version == $version) | .source // empty' \
      "$resolution_root/metadata.json")"
    if [[ "$resolved_source" == registry+* ]]; then
      echo "$crate_name $version 已由无 patch 的独立下游解析到 registry"
      exit 0
    fi
  fi

  if [[ "$attempt" -lt 60 ]]; then
    sleep 5
  fi
done

cat "$resolution_root/metadata.err" >&2
echo "$crate_name $version 在五分钟内未能由无 patch 的独立下游解析到 registry" >&2
exit 1
