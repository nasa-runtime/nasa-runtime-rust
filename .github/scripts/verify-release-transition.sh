#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "用法: $0 <batch> [cargo-bin]" >&2
  exit 2
fi

release_batch="$1"
cargo_bin="${2:-cargo}"
repository_root="$(cd "$(dirname "$0")/../.." && pwd)"
plan="$($repository_root/.github/scripts/release-crates.sh --plan)"
current_stage="$(printf '%s\n' "$plan" | awk -F '\t' -v batch="$release_batch" '$1 == batch { print $2; exit }')"
if [[ -z "$current_stage" ]]; then
  echo "未知发布批次: $release_batch" >&2
  exit 1
fi

transition_root="$(mktemp -d "${RUNNER_TEMP:-/tmp}/release-transition-${release_batch}.XXXXXX")"
predecessors_file="$transition_root/predecessors.txt"

# 业务作用：只清理本次批间依赖来源检查生成的前置 crate 清单。
# 参数说明：无。
# 返回：目录存在时精确删除；目录已经不存在时保持幂等成功。
cleanup() {
  if [[ -n "$transition_root" && -d "$transition_root" ]]; then
    rm -rf -- "$transition_root"
  fi
}
trap cleanup EXIT

printf '%s\n' "$plan" | awk -F '\t' -v stage="$current_stage" '
  $2 < stage {
    count = split($3, names, " ")
    for (item = 1; item <= count; item++) print names[item]
  }
' > "$predecessors_file"

# 业务作用：判断指定 crate 是否仍由根级 crates.io patch 指向本地工作区。
# 参数说明：`$1` 为 crate 名称。
# 返回：仍存在本地 path 覆盖时成功，不存在时返回非零状态。
has_local_patch() {
  local crate_name="$1"
  awk -v target="$crate_name" '
    /^\[patch\.crates-io\][[:space:]]*$/ { in_patch = 1; next }
    in_patch && /^\[/ { in_patch = 0 }
    in_patch {
      line = $0
      key = line
      sub(/^[[:space:]]*/, "", key)
      sub(/[[:space:]]*=.*/, "", key)
      if (key == target && line ~ /path[[:space:]]*=/) found = 1
    }
    END { exit(found ? 0 : 1) }
  ' "$repository_root/Cargo.toml"
}

blocked=0
while IFS= read -r crate_name; do
  [[ -z "$crate_name" ]] && continue
  if has_local_patch "$crate_name"; then
    echo "批次 $release_batch 的前置 crate $crate_name 仍由根级 path patch 指向本地工作区" >&2
    blocked=1
  fi
done < "$predecessors_file"
if [[ "$blocked" != "0" ]]; then
  echo "先在前置批次进入 registry 后运行 prepare-next-release-batch.sh，再提交 Cargo.toml 与 Cargo.lock" >&2
  exit 1
fi

# 完整解析会拒绝尚未记录 registry 来源的旧锁文件，避免声明检查通过后才在打包阶段失败。
"$cargo_bin" metadata --locked --format-version 1 \
  --manifest-path "$repository_root/Cargo.toml" > /dev/null
echo "批次 $release_batch 的全部前置 crate 已解除根级 path patch，锁文件可按当前 manifest 解析"
