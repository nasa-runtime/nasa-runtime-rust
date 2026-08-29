#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -gt 1 ]]; then
  echo "用法: $0 [cargo-bin]" >&2
  exit 2
fi

cargo_bin="${1:-cargo}"
repository_root="$(cd "$(dirname "$0")/../.." && pwd)"
plan_root="$(mktemp -d "${RUNNER_TEMP:-/tmp}/release-plan.XXXXXX")"

# 业务作用：只清理本次发布拓扑解析生成的计划、探针 manifest 与 Cargo 元数据。
# 参数说明：无。
# 返回：目录存在时精确删除；目录已经不存在时保持幂等成功。
cleanup() {
  if [[ -n "$plan_root" && -d "$plan_root" ]]; then
    rm -rf -- "$plan_root"
  fi
}
trap cleanup EXIT

metadata_file="$plan_root/workspace-metadata.json"
plan_file="$plan_root/release-plan.tsv"
versioned_plan_file="$plan_root/release-versioned-plan.tsv"
edges_file="$plan_root/workspace-edges.tsv"
versions_file="$plan_root/workspace-versions.tsv"
resolved_file="$plan_root/resolved-requirements.tsv"
probe_counter=0
last_probe_error=""

# 业务作用：使用独立且无 workspace patch 的 Cargo 项目确认 registry 能否满足完整依赖合同。
# 参数说明：`$1` 为 crate 名称，`$2` 为 Cargo 版本要求，`$3` 表示是否启用默认 feature，`$4` 为显式 feature 的 JSON 数组。
# 返回：版本、默认 feature 与显式 feature 可从 registry 一并解析时成功并缓存结论；否则返回失败并保留探针诊断。
registry_requirement_resolves() {
  local package_name="$1"
  local requirement="$2"
  local uses_default_features="$3"
  local dependency_features="$4"
  local cached
  local probe_root
  local resolved_source

  last_probe_error=""
  cached="$(awk -F '\t' -v name="$package_name" -v req="$requirement" \
    -v defaults="$uses_default_features" -v features="$dependency_features" '
    $1 == name && $2 == req && $3 == defaults && $4 == features { print $5; exit }
  ' "$resolved_file")"
  if [[ "$cached" == "registry" ]]; then
    return 0
  fi

  if [[ ! "$package_name" =~ ^[A-Za-z0-9_-]+$ ]] ||
    [[ "$requirement" == *$'\n'* || "$requirement" == *$'\t'* || "$requirement" == *'"'* ]] ||
    [[ "$uses_default_features" != "true" && "$uses_default_features" != "false" ]]; then
    last_probe_error="$plan_root/invalid-probe.err"
    printf '%s\n' "registry 探针收到无法安全写入 manifest 的依赖合同" > "$last_probe_error"
    return 1
  fi

  probe_counter=$((probe_counter + 1))
  probe_root="$plan_root/probe-$probe_counter"
  mkdir -p "$probe_root/src"
  printf 'fn main() {}\n' > "$probe_root/src/main.rs"
  printf '[package]\nname = "release-plan-probe-%s"\nversion = "0.0.0"\nedition = "2021"\npublish = false\n\n[dependencies]\ncandidate = { package = "%s", version = "%s", default-features = %s, features = %s }\n' \
    "$probe_counter" "$package_name" "$requirement" "$uses_default_features" \
    "$dependency_features" > "$probe_root/Cargo.toml"
  if ! "$cargo_bin" metadata --format-version 1 --manifest-path "$probe_root/Cargo.toml" \
    > "$probe_root/metadata.json" 2> "$probe_root/metadata.err"; then
    last_probe_error="$probe_root/metadata.err"
    return 1
  fi

  resolved_source="$(jq -r --arg name "$package_name" '
    [.packages[] | select(.name == $name and (.source // "" | startswith("registry+")))]
    | if length > 0 then "registry" else "" end
  ' "$probe_root/metadata.json")"
  if [[ "$resolved_source" != "registry" ]]; then
    last_probe_error="$probe_root/source.err"
    printf '%s\n' "Cargo 没有从 registry 解析目标 crate" > "$last_probe_error"
    return 1
  fi

  printf '%s\t%s\t%s\t%s\tregistry\n' "$package_name" "$requirement" \
    "$uses_default_features" "$dependency_features" >> "$resolved_file"
}

# 业务作用：输出依赖合同无法从 registry 解析时的完整上下文，防止只报告版本而隐藏 feature 条件。
# 参数说明：`$1` 为依赖边上下文。
# 返回：无返回值；诊断写入标准错误，不改变探针结论。
report_registry_failure() {
  local context="$1"

  echo "${context}，registry 无法解析该依赖合同且发布计划没有可用前置批次" >&2
  if [[ -n "$last_probe_error" && -f "$last_probe_error" ]]; then
    cat "$last_probe_error" >&2
  fi
}

"$cargo_bin" metadata --locked --no-deps --format-version 1 \
  --manifest-path "$repository_root/Cargo.toml" > "$metadata_file"
"$repository_root/.github/scripts/release-crates.sh" --plan > "$plan_file"
"$repository_root/.github/scripts/release-crates.sh" --versioned-plan > "$versioned_plan_file"
: > "$resolved_file"

while IFS=$'\t' read -r batch stage items; do
  plain_stage="$(awk -F '\t' -v batch="$batch" '$1 == batch { print $2; exit }' "$plan_file")"
  if [[ "$plain_stage" != "$stage" ]]; then
    echo "批次 $batch 的普通计划与版本计划阶段不一致" >&2
    exit 1
  fi
  for item in $items; do
    crate_name="${item%@*}"
    planned_version="${item##*@}"
    workspace_version="$(jq -r --arg name "$crate_name" '.packages[] | select(.name == $name) | .version' "$metadata_file")"
    if [[ -z "$workspace_version" || "$workspace_version" == "null" ]]; then
      echo "版本计划中的 $crate_name 不属于当前工作区" >&2
      exit 1
    fi
    if [[ "$planned_version" != "$workspace_version" ]]; then
      echo "$crate_name 的发布计划版本 $planned_version 与 manifest 版本 $workspace_version 不一致" >&2
      exit 1
    fi
  done
done < "$versioned_plan_file"

while IFS=$'\t' read -r _batch _stage crates; do
  for crate_name in $crates; do
    package_count="$(jq -r --arg name "$crate_name" '[.packages[] | select(.name == $name)] | length' "$metadata_file")"
    if [[ "$package_count" != "1" ]]; then
      echo "发布计划中的 $crate_name 必须恰好对应一个工作区 crate，实际数量: $package_count" >&2
      exit 1
    fi
    duplicate_count="$(awk -F '\t' -v name="$crate_name" '
      { for (field_no = 3; field_no <= NF; field_no++) { count = split($field_no, names, " "); for (item_no = 1; item_no <= count; item_no++) if (names[item_no] == name) matches++ } }
      END { print matches + 0 }
    ' "$plan_file")"
    if [[ "$duplicate_count" != "1" ]]; then
      echo "$crate_name 在发布计划中出现 $duplicate_count 次" >&2
      exit 1
    fi
  done
done < "$plan_file"

jq -r '.packages[] | [.name, .version] | @tsv' "$metadata_file" > "$versions_file"
unplanned_published=0
while IFS=$'\t' read -r package_name package_version; do
  planned_stage="$(awk -F '\t' -v name="$package_name" '
    { count = split($3, names, " "); for (item_no = 1; item_no <= count; item_no++) if (names[item_no] == name) { print $2; exit } }
  ' "$plan_file")"
  if [[ -n "$planned_stage" ]]; then
    continue
  fi

  if ! registry_requirement_resolves "$package_name" "=$package_version" true '[]'; then
    report_registry_failure "工作区版本 ${package_name} ${package_version} 未进入发布计划"
    exit 1
  fi
  unplanned_published=$((unplanned_published + 1))
done < "$versions_file"

jq -r '
  .packages[] as $owner
  | $owner.dependencies[]
  | select(.kind != "dev")
  | . as $dependency
  | ($dependency.rename // $dependency.name) as $dependency_key
  | ([
      ($dependency.features // [])[],
      ($owner.features
        | to_entries[]
        | .value[]
        | select(startswith($dependency_key + "/") or startswith($dependency_key + "?/"))
        | sub("^[^/]+/"; ""))
    ] | unique) as $dependency_features
  | [
      $owner.name,
      $dependency.name,
      $dependency.req,
      ($dependency.uses_default_features | tostring),
      ($dependency_features | @json)
    ]
  | @tsv
' "$metadata_file" > "$edges_file"

checked_edges=0
registry_edges=0
feature_checked_edges=0
while IFS=$'\t' read -r owner dependency requirement uses_default_features dependency_features; do
  owner_stage="$(awk -F '\t' -v name="$owner" '
    { count = split($3, names, " "); for (item_no = 1; item_no <= count; item_no++) if (names[item_no] == name) { print $2; exit } }
  ' "$plan_file")"
  if [[ -z "$owner_stage" ]]; then
    continue
  fi
  dependency_is_workspace="$(jq -r --arg name "$dependency" '[.packages[] | select(.name == $name)] | length' "$metadata_file")"
  if [[ "$dependency_is_workspace" != "1" ]]; then
    continue
  fi
  checked_edges=$((checked_edges + 1))

  dependency_stage="$(awk -F '\t' -v name="$dependency" '
    { count = split($3, names, " "); for (item_no = 1; item_no <= count; item_no++) if (names[item_no] == name) { print $2; exit } }
  ' "$plan_file")"

  if [[ "$dependency_features" != "[]" ]]; then
    feature_checked_edges=$((feature_checked_edges + 1))
    edge_context="${owner} 要求 ${dependency} ${requirement}（default-features=${uses_default_features}, features=${dependency_features}）"
    if ! registry_requirement_resolves "$dependency" "$requirement" \
      "$uses_default_features" "$dependency_features"; then
      if [[ -n "$dependency_stage" && "$dependency_stage" -lt "$owner_stage" ]]; then
        workspace_version="$(awk -F '\t' -v name="$dependency" '$1 == name { print $2; exit }' "$versions_file")"
        missing_workspace_features="$(jq -r --arg name "$dependency" --argjson required "$dependency_features" '
          [.packages[]
            | select(.name == $name)
            | .features as $available
            | $required[]
            | select(. as $feature | ($available | has($feature) | not))]
          | unique
          | join(",")
        ' "$metadata_file")"
        if [[ -n "$missing_workspace_features" ]]; then
          echo "${edge_context}，前置批次的工作区 crate 缺少 feature: ${missing_workspace_features}" >&2
          exit 1
        fi
        if [[ "$requirement" != "^${workspace_version}" && "$requirement" != "=${workspace_version}" ]]; then
          echo "${edge_context}，registry 尚无该 feature 合同时，版本要求必须以待发布工作区版本 ${workspace_version} 为下限" >&2
          exit 1
        fi
        continue
      fi
      report_registry_failure "$edge_context"
      exit 1
    fi
  fi

  if [[ -n "$dependency_stage" && "$dependency_stage" -lt "$owner_stage" ]]; then
    continue
  fi

  if ! registry_requirement_resolves "$dependency" "$requirement" \
    "$uses_default_features" "$dependency_features"; then
    report_registry_failure "${owner} 要求 ${dependency} ${requirement}（default-features=${uses_default_features}, features=${dependency_features}）"
    exit 1
  fi
  registry_edges=$((registry_edges + 1))
done < "$edges_file"

echo "发布计划已覆盖全部未发布工作区版本；${unplanned_published} 个未入计划版本已存在于 registry"
echo "发布拓扑已覆盖 ${checked_edges} 条工作区依赖边；其中 ${feature_checked_edges} 条复验传递 feature，${registry_edges} 条由无 patch registry 解析兜底"
