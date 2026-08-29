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
      printf '%s\n' "naml@1.0.3"
      ;;
    napart)
      printf '%s\n' "napart@1.1.2"
      ;;
    1)
      printf '%s\n' "macro-support@1.0.2 natx-core@1.0.0 namigrate-core@1.0.0 namapper-core@1.0.0 nabase@1.0.2 naauthz@1.0.2 nasecret@1.0.1"
      ;;
    2)
      printf '%s\n' "naidempotency@1.0.2 nainbox-core@1.0.2 naoutbox-core@1.0.2 nametrics-core@1.0.2 natelemetry@1.0.2"
      ;;
    3)
      printf '%s\n' "async-macro@1.0.2 nadis-derive@1.0.1 namapper-macro@1.0.2 napp-macro@1.0.2 natx-macro@1.0.1 rest-client-macro@1.0.2 nanum@1.0.2"
      ;;
    4)
      printf '%s\n' "natx@1.0.3 natx-pgsql@1.0.0 nanacos@1.0.2 nadis@1.0.2 nafka@1.0.3 naweb@1.0.2"
      ;;
    5)
      printf '%s\n' "nagrpc-build@1.0.0 nasaga-core@1.0.1 namigrate@1.0.1 namigrate-pgsql@1.0.0 naws-proto@1.0.1 ncrypto@1.0.1 nainbox-mysql@1.0.2 nainbox-pgsql@1.0.0 naidempotency-pgsql@1.0.0 naoutbox-mysql@1.0.2 naoutbox-pgsql@1.0.0 cacheable@1.0.2 hystrix@1.0.2 nafana@1.0.2 nagrpc@1.1.0"
      ;;
    6)
      printf '%s\n' "namapper-pgsql@1.0.0 naaudit-mysql@1.0.2 naaudit-pgsql@1.0.0 nasaga-macro@1.0.2 naidempotency-mysql@1.0.2 nasaga-backend@1.0.0"
      ;;
    7)
      printf '%s\n' "naobject@1.0.1 config-boot@1.0.2 naidempotency-redis@1.0.2 namapper@1.0.2 rest-discovery@1.0.2 nasaga-mysql@1.0.2 nasaga-pgsql@1.0.0 nasaga-runtime-core@1.0.0"
      ;;
    8)
      printf '%s\n' "nasaga-runtime@1.0.2 nasaga-runtime-pgsql@1.0.0 naws@1.0.2 rest-discovery-nacos@1.0.2"
      ;;
    9)
      printf '%s\n' "nasched@1.0.2 napp@1.0.3"
      ;;
    10)
      printf '%s\n' "nasa@1.0.3"
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
5 5
6 6
naml 7
7 8
napart 9
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
