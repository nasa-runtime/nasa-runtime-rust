#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "用法: $0 <batch|--plan>" >&2
  exit 2
fi

# 业务作用：返回单个发布批次包含的 crate，作为验证与上传阶段唯一的批次来源。
# 参数说明：`$1` 为 workflow 暴露的批次标识。
# 返回：已知批次输出空格分隔的 crate；未知批次返回非零状态。
release_crates() {
  case "$1" in
    naml)
      printf '%s\n' "naml"
      ;;
    napart)
      printf '%s\n' "napart"
      ;;
    1)
      printf '%s\n' "macro-support natx-core namigrate-core namapper-core nabase naauthz nabudget nadisc"
      ;;
    2)
      printf '%s\n' "naidempotency nainbox-core naoutbox-core nametrics-core natelemetry"
      ;;
    3)
      printf '%s\n' "async-macro nacache-macro nadis-derive namapper-macro napp-macro natx-macro rest-client-macro nanum"
      ;;
    4)
      printf '%s\n' "nasecret natx natx-pgsql nanacos nadis nafka naweb"
      ;;
    5)
      printf '%s\n' "nagrpc-build nasaga-core namigrate namigrate-pgsql namapper-pgsql naws-proto ncrypto nainbox-mysql nainbox-pgsql naidempotency-pgsql naoutbox-mysql naoutbox-pgsql cacheable hystrix nafana nagrpc naopenapi"
      ;;
    6)
      printf '%s\n' "naaudit-mysql naaudit-pgsql nasaga-macro naidempotency-mysql nasaga-backend"
      ;;
    7)
      printf '%s\n' "naobject config-boot naidempotency-redis namapper rest-discovery nasched nasaga-mysql nasaga-pgsql nasaga-runtime-core"
      ;;
    8)
      printf '%s\n' "nasaga-runtime nasaga-runtime-pgsql naws rest-discovery-nacos"
      ;;
    9)
      printf '%s\n' "napp"
      ;;
    10)
      printf '%s\n' "nasa"
      ;;
    *)
      echo "未知发布批次: $1" >&2
      return 1
      ;;
  esac
}

if [[ "$1" == "--plan" ]]; then
  while IFS=' ' read -r batch stage; do
    printf '%s\t%s\t%s\n' "$batch" "$stage" "$(release_crates "$batch")"
  done <<'PLAN'
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
  exit 0
fi

release_crates "$1"
