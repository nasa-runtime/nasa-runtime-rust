#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "用法: $0 <completed-batch> [cargo-bin]" >&2
  exit 2
fi

completed_batch="$1"
cargo_bin="${2:-cargo}"
repository_root="$(cd "$(dirname "$0")/../.." && pwd)"
release_script="$repository_root/.github/scripts/release-crates.sh"
completed_crates="$($release_script "$completed_batch")"
if [[ -z "$completed_crates" ]]; then
  echo "未知或空发布批次: $completed_batch" >&2
  exit 1
fi
if [[ -n "$(git -C "$repository_root" status --porcelain --untracked-files=no)" ]]; then
  echo "批间转换要求 tracked 工作树干净，避免覆盖尚未提交的业务改动" >&2
  exit 1
fi

transition_root="$(mktemp -d "${RUNNER_TEMP:-/tmp}/prepare-release-${completed_batch}.XXXXXX")"
crate_names_file="$transition_root/crates.txt"
next_manifest="$transition_root/Cargo.toml"

# 业务作用：只清理本次批间转换生成的 crate 清单和候选 manifest。
# 参数说明：无。
# 返回：目录存在时精确删除；目录已经不存在时保持幂等成功。
cleanup() {
  if [[ -n "$transition_root" && -d "$transition_root" ]]; then
    rm -rf -- "$transition_root"
  fi
}
trap cleanup EXIT

for crate_name in $completed_crates; do
  "$repository_root/.github/scripts/verify-registry-resolution.sh" "$crate_name" "$cargo_bin"
  printf '%s\n' "$crate_name" >> "$crate_names_file"
done

awk -v names_file="$crate_names_file" '
  BEGIN {
    while ((getline name < names_file) > 0) published[name] = 1
    close(names_file)
  }
  /^\[patch\.crates-io\][[:space:]]*$/ { in_patch = 1; print; next }
  in_patch && /^\[/ { in_patch = 0 }
  in_patch {
    line = $0
    key = line
    sub(/^[[:space:]]*/, "", key)
    sub(/[[:space:]]*=.*/, "", key)
    if (published[key] && line ~ /path[[:space:]]*=/) next
  }
  { print }
' "$repository_root/Cargo.toml" > "$next_manifest"

if cmp -s "$repository_root/Cargo.toml" "$next_manifest"; then
  echo "批次 $completed_batch 没有需要解除的根级 path patch"
else
  mv "$next_manifest" "$repository_root/Cargo.toml"
fi

# Cargo 复用既有锁定结果，仅为刚解除 patch 的依赖补充 registry package，不主动升级无关依赖。
"$cargo_bin" metadata --no-deps --format-version 1 \
  --manifest-path "$repository_root/Cargo.toml" > /dev/null

echo "批次 $completed_batch 已切换为 registry 来源；请审阅并提交 Cargo.toml 与 Cargo.lock，再执行下一批"
