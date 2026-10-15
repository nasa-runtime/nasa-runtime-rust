#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "用法: $0 <batch|--plan|--versioned-plan>" >&2
  exit 2
fi

# 业务作用：返回单个发布批次的 crate 与目标版本，作为批次和版本的唯一事实来源。
# 参数说明：`$1` 为 workflow 暴露的批次标识。
# 返回：已知批次输出空格分隔的 `crate@version`；未知批次返回非零状态。
release_items() {
  case "$1" in
    discovery-foundation)
      printf '%s\n' "nadisc@2.0.3 nanacos-sdk@1.0.0 nafka-macro@2.0.3 napp-macro@2.0.3 rest-client-macro@2.0.3 naml@2.0.2"
      ;;
    backend-adapters)
      printf '%s\n' "nanacos@2.0.3 nadisc-consul@1.0.0 nadisc-kubernetes@1.0.0 nafka@2.0.3"
      ;;
    discovery-runtime)
      printf '%s\n' "nadisc-runtime@1.0.0 rest-discovery@2.0.3 nafana@2.0.1"
      ;;
    client-transports)
      printf '%s\n' "nagrpc@2.0.3 rest-discovery-nacos@2.0.3"
      ;;
    application-runtime)
      printf '%s\n' "napp@2.0.3"
      ;;
    facade)
      printf '%s\n' "nasa@2.0.3"
      ;;
    *)
      echo "未知发布批次: $1" >&2
      return 1
      ;;
  esac
}

# 业务作用：把版本化发布项转换为 Cargo 接受的 crate 名列表，避免验证与上传逻辑重复维护版本。
# 参数说明：`$1` 为发布批次标识。
# 返回：已知批次输出空格分隔的 crate 名；未知批次返回非零状态。
release_crates() {
  local item
  local items
  local crate_names=()

  items="$(release_items "$1")" || return 1
  for item in $items; do
    crate_names+=("${item%@*}")
  done
  printf '%s\n' "${crate_names[*]}"
}

# 业务作用：按严格依赖顺序输出批次和拓扑阶段，供发布门禁统一解析前后置关系。
# 参数说明：无。
# 返回：逐行输出批次标识与单调递增的拓扑阶段。
release_stages() {
  cat <<'PLAN'
discovery-foundation 1
backend-adapters 2
discovery-runtime 3
client-transports 4
application-runtime 5
facade 6
PLAN
}

if [[ "$1" == "--plan" || "$1" == "--versioned-plan" ]]; then
  while IFS=' ' read -r batch stage; do
    if [[ "$1" == "--versioned-plan" ]]; then
      printf '%s\t%s\t%s\n' "$batch" "$stage" "$(release_items "$batch")"
    else
      printf '%s\t%s\t%s\n' "$batch" "$stage" "$(release_crates "$batch")"
    fi
  done < <(release_stages)
  exit 0
fi

release_crates "$1"
