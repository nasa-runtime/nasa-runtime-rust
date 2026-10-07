#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "用法: $0 <batch> [cargo-bin]" >&2
  exit 2
fi

release_batch="$1"
cargo_bin="${2:-cargo}"
repository_root="$(cd "$(dirname "$0")/../.." && pwd)"
plan="$("$repository_root/.github/scripts/release-crates.sh" --plan)"
current_stage="$(printf '%s\n' "$plan" | awk -F '\t' -v batch="$release_batch" '$1 == batch { print $2; exit }')"
if [[ -z "$current_stage" ]]; then
  echo "未知发布批次: $release_batch" >&2
  exit 1
fi

transition_root="$(mktemp -d "${RUNNER_TEMP:-/tmp}/release-transition-${release_batch}.XXXXXX")"
predecessors_file="$transition_root/predecessors.txt"
current_crates="$("$repository_root/.github/scripts/release-crates.sh" "$release_batch")"
metadata_file="$transition_root/metadata.json"
paths_file="$transition_root/paths.tsv"

# 业务作用：只清理本次依赖来源检查生成的清单与 Cargo 元数据。
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
# 当前批次也必须解除根覆盖，避免归档验证和实际上传使用不同的依赖来源。
for crate_name in $current_crates; do
  if has_local_patch "$crate_name"; then
    echo "批次 $release_batch 的 crate $crate_name 仍由根级 path patch 覆盖" >&2
    blocked=1
  fi
done
while IFS= read -r crate_name; do
  [[ -z "$crate_name" ]] && continue
  if has_local_patch "$crate_name"; then
    echo "批次 $release_batch 的前置 crate $crate_name 仍由根级 path patch 指向本地工作区" >&2
    blocked=1
  fi
done < "$predecessors_file"

# 尚未轮到的下游可以保留本地装配；当前上传包和已上线前置依赖必须使用 registry 声明。
# 单独检查命令状态，避免过程替换吞掉 Cargo 解析失败后误放行。
"$cargo_bin" metadata --locked --no-deps --format-version 1 \
  --manifest-path "$repository_root/Cargo.toml" > "$metadata_file"
jq -r --arg current "$current_crates" --rawfile predecessors "$predecessors_file" '
  ($current | split(" ")) as $owners
  | ($predecessors | split("\n")) as $previous
  | .packages[] as $owner
  | $owner.dependencies[]
  | select(.path != null)
  | . as $dependency
  | select(($owners | index($owner.name)) != null or ($previous | index($dependency.name)) != null)
  | [$owner.name, .name, .path] | @tsv
' "$metadata_file" > "$paths_file"
if [[ -s "$paths_file" ]]; then
  cat "$paths_file" >&2
  echo "上述直接 path 依赖必须先改为保留版本与 feature 的 registry 依赖" >&2
  blocked=1
fi
if [[ "$blocked" != "0" ]]; then
  echo "依赖来源不满足当前批次要求；转换后提交相关 manifest 与 Cargo.lock" >&2
  exit 1
fi

# 完整解析会拒绝尚未记录 registry 来源的旧锁文件，避免声明检查通过后才在打包阶段失败。
"$cargo_bin" metadata --locked --format-version 1 \
  --manifest-path "$repository_root/Cargo.toml" > /dev/null
echo "批次 $release_batch 的直接 path 与前置覆盖检查通过，锁文件可按当前 manifest 解析"
