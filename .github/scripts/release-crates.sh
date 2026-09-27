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
    naml)
      printf '%s\n' "naml@2.0.0"
      ;;
    napart)
      printf '%s\n' "napart@2.0.0"
      ;;
    1)
      printf '%s\n' "macro-support@2.0.0 naauthz@2.0.0 nabase@2.0.0 nabudget@2.0.0 nadisc@2.0.0 nagrpc-build@2.0.0 naidempotency@2.0.0 naimg@2.0.0 nainbox-core@2.0.0 nametrics-core@2.0.0"
      ;;
    2)
      printf '%s\n' "namigrate-core@2.0.0 nanum@2.0.0 naopenapi@2.0.0 naoutbox-core@2.0.0 nasaga-core@2.0.0 nasecret@2.0.0 natelemetry@2.0.0 naws-proto-derive@2.0.0 ncrypto@2.0.0 rest-client-macro@2.0.0"
      ;;
    3)
      printf '%s\n' "async-macro@2.0.0 hystrix-macro@2.0.0 naaudit@2.0.0 nacache-macro@2.0.0 nadis-derive@2.0.0 nafana-macro@2.0.0 nafka-macro@2.0.0 nagrpc@2.0.0 nalog@2.0.0 namapper-macro@2.0.0 nanacos@2.0.0"
      ;;
    4)
      printf '%s\n' "nanotify-core@2.0.0 naobject@2.0.0 napp-macro@2.0.0 nasaga-macro@2.0.0 nasecret-http@2.0.0 nasecret-vault@2.0.0 natx-macro@2.0.0 nauth-oauth@2.0.0 naweb-macro@2.0.0 naws-proto@2.0.0"
      ;;
    5)
      printf '%s\n' "config-boot@2.0.0 hystrix@2.0.0 nadis@2.0.0 nafka@2.0.0 namapper-core@2.0.0 natx-core@2.0.0 naweb@2.0.0 rest-discovery@2.0.0"
      ;;
    6)
      printf '%s\n' "cacheable@2.0.0 nafana@2.0.0 naidempotency-redis@2.0.0 namigrate@2.0.0 namigrate-pgsql@2.0.0 nasaga-backend@2.0.0 nasched@2.0.0 natx@2.0.0 natx-pgsql@2.0.0 naws@2.0.0 rest-discovery-nacos@2.0.0"
      ;;
    7)
      printf '%s\n' "naidempotency-mysql@2.0.0 naidempotency-pgsql@2.0.0 nainbox-mysql@2.0.0 nainbox-pgsql@2.0.0 namapper@2.0.0 namapper-pgsql@2.0.0 naoutbox-mysql@2.0.0 naoutbox-pgsql@2.0.0 nasaga-mysql@2.0.0 nasaga-pgsql@2.0.0 nasaga-runtime-core@2.0.0"
      ;;
    8)
      printf '%s\n' "naaudit-mysql@2.0.0 naaudit-pgsql@2.0.0 nasaga-runtime@2.0.0 nasaga-runtime-pgsql@2.0.0"
      ;;
    9)
      printf '%s\n' "napp@2.0.0"
      ;;
    10)
      printf '%s\n' "nasa@2.0.0"
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
  local crate_names=()

  for item in $(release_items "$1"); do
    crate_names+=("${item%@*}")
  done
  printf '%s\n' "${crate_names[*]}"
}

# 业务作用：按严格依赖顺序输出批次和拓扑阶段，供发布门禁统一解析前后置关系。
# 参数说明：无。
# 返回：逐行输出批次标识与单调递增的拓扑阶段。
release_stages() {
  cat <<'PLAN'
1 1
2 2
3 3
4 4
naml 5
napart 6
5 7
6 8
7 9
8 10
9 11
10 12
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
