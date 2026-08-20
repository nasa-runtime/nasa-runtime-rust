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
      printf '%s\n' "macro-support nabase naauthz nabudget nadisc"
      ;;
    2)
      printf '%s\n' "naidempotency nainbox-core naoutbox-core nametrics-core natelemetry"
      ;;
    3)
      printf '%s\n' "async-macro nacache-macro nadis-derive namapper-macro napp-macro rest-client-macro nanum"
      ;;
    4)
      printf '%s\n' "nasecret natx nanacos nadis nafka naweb"
      ;;
    5)
      printf '%s\n' "nagrpc-build nasaga-core namigrate naws-proto ncrypto nainbox-mysql naoutbox-mysql cacheable hystrix nafana nagrpc naopenapi"
      ;;
    6)
      printf '%s\n' "naaudit-mysql nasaga-macro naidempotency-mysql nasaga-mysql"
      ;;
    7)
      printf '%s\n' "naobject config-boot naidempotency-redis namapper rest-discovery nasched"
      ;;
    8)
      printf '%s\n' "nasaga-runtime naws rest-discovery-nacos"
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
