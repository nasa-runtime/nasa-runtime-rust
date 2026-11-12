#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "用法: $0 <batch|--plan|--versioned-plan|--patch-candidates>" >&2
  exit 2
fi

# 业务作用：返回单个发布批次的 crate 与目标版本，作为批次和版本的唯一事实来源。
# 参数说明：
# - `$1`：workflow 暴露的批次标识。
# 返回：已知批次输出空格分隔的 `crate@version`；未知批次返回非零状态。
release_items() {
  local items=()
  case "$1" in
    update-foundation)
      items=(
        "macro-support@2.0.1"
        "naauthz@2.0.1"
        "nabase@2.0.1"
        "nabudget@2.0.1"
        "nadisc@2.0.4"
        "nagrpc-build@2.0.1"
        "naidempotency@2.0.1"
        "naimg@2.0.1"
        "nainbox-core@2.0.1"
        "nametrics-core@2.0.1"
        "namigrate-core@2.0.1"
        "nanacos-sdk@1.0.1"
        "nanum@2.0.1"
        "naopenapi@2.0.1"
        "naoutbox-core@2.0.1"
        "nasaga-core@2.0.1"
        "nasecret@2.0.1"
        "natelemetry@2.0.1"
        "naws-proto-derive@2.0.1"
        "ncrypto@2.0.1"
        "rest-client-macro@2.0.4"
      )
      ;;
    update-support-components)
      items=(
        "async-macro@2.0.1"
        "hystrix-macro@2.0.1"
        "naaudit@2.0.1"
        "nacache-macro@2.0.1"
        "nadis-derive@2.0.1"
        "nadisc-consul@1.0.1"
        "nafana-macro@2.0.1"
        "nafka-macro@2.0.4"
        "nalog@2.0.1"
        "namapper-macro@2.0.1"
        "naml@2.0.3"
        "nanacos@2.0.4"
        "nanotify-core@2.0.1"
        "naobject@2.0.1"
        "napart@2.0.1"
        "napp-macro@2.0.4"
        "nasaga-macro@2.0.1"
        "nasecret-http@2.0.1"
        "nasecret-vault@2.0.1"
        "natx-macro@2.0.1"
        "nauth-oauth@2.0.1"
        "naweb-macro@2.0.1"
        "naws-proto@2.0.1"
      )
      ;;
    update-core-services)
      items=(
        "config-boot@2.0.2"
        "hystrix@2.0.1"
        "nadis@2.0.1"
        "nadisc-kubernetes@1.0.1"
        "nafka@2.0.4"
        "namapper-core@2.0.1"
        "natx-core@2.0.1"
        "naweb@2.0.1"
        "rest-discovery@2.0.4"
      )
      ;;
    new-update-resource-runtimes)
      items=(
        "cacheable@2.0.1"
        "nadisc-runtime@1.0.1"
        "nafana@2.0.2"
        "naidempotency-redis@2.0.1"
        "namigrate@2.0.1"
        "namigrate-pgsql@2.0.1"
        "nasaga-backend@2.0.1"
        "nasched@2.0.1"
        "natx@2.0.1"
        "natx-pgsql@2.0.1"
        "natx-sqlite@1.0.0"
        "naws@2.0.1"
        "rest-discovery-nacos@2.0.4"
      )
      ;;
    new-update-storage-adapters)
      items=(
        "nagrpc@2.0.5"
        "naidempotency-mysql@2.0.1"
        "naidempotency-pgsql@2.0.1"
        "nainbox-mysql@2.0.1"
        "nainbox-pgsql@2.0.1"
        "namapper@2.0.1"
        "namapper-pgsql@2.0.1"
        "namapper-sqlite@1.0.0"
        "namigrate-sqlite@1.0.0"
        "naoutbox-mysql@2.0.1"
        "naoutbox-pgsql@2.0.1"
        "nasaga-mysql@2.0.2"
        "nasaga-pgsql@2.0.1"
      )
      ;;
    update-workflow-support)
      items=(
        "naaudit-mysql@2.0.1"
        "naaudit-pgsql@2.0.1"
        "nasaga-runtime-core@2.0.2"
      )
      ;;
    update-saga-runtime)
      items=(
        "nasaga-runtime@2.0.3"
        "nasaga-runtime-pgsql@2.0.3"
      )
      ;;
    update-application-runtime)
      items=(
        "napp@2.0.7"
      )
      ;;
    update-facade)
      items=(
        "nasa@2.0.6"
      )
      ;;
    *)
      echo "未知发布批次: $1" >&2
      return 1
      ;;
  esac
  printf '%s\n' "${items[*]}"
}

# 业务作用：把版本化发布项转换为 Cargo 接受的 crate 名列表，避免验证与上传逻辑重复维护版本。
# 参数说明：
# - `$1`：发布批次标识。
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
update-foundation 1
update-support-components 2
update-core-services 3
new-update-resource-runtimes 4
new-update-storage-adapters 5
update-workflow-support 6
update-saga-runtime 7
update-application-runtime 8
update-facade 9
PLAN
}

# 业务作用：限定增量发布期间允许重新使用本地源码的组件及目标版本。
# 参数说明：无。
# 返回：仅输出当前明确安排的补丁组件，版本仍取自统一发布计划。
patch_candidates() {
  local item
  local items
  items="$(release_items new-update-storage-adapters) $(release_items update-application-runtime)" || return 1
  for item in $items; do
    case "${item%@*}" in
      nagrpc|napp) printf '%s\n' "$item" ;;
    esac
  done
}

if [[ "$1" == "--patch-candidates" ]]; then
  patch_candidates
  exit 0
fi

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
