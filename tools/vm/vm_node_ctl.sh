#!/usr/bin/env bash
#
# Speedboat 单节点远程控制（部署于各 VM 的 ~/speedboat-test/ 内，由宿主编排脚本调用）。
#
# 职责：在远端以固定工作目录、固定 Java（Dragonwell 8）、固定 classpath 启停被测节点。
# 身份由 -Dspeedboat.local.ip 显式钉定，避免多网卡枚举顺序漂移。
#
# 用法：
#   vm_node_ctl.sh start <localIp> <configFile> <mode> <lockName> <logFile>
#   vm_node_ctl.sh stop        # 优雅停机（SIGTERM，触发 shutdown hook）
#   vm_node_ctl.sh kill9       # 模拟崩溃（SIGKILL，无收尾）
#   vm_node_ctl.sh alive       # 输出 alive/dead
#   vm_node_ctl.sh clean       # 清空 data/ 与 logs/
#
set -euo pipefail

BASE="${HOME}/speedboat-test"
JAVA="${HOME}/lang/dragonwell-8.29.28/bin/java"
CP="${BASE}/runner:${BASE}/speedboat.jar"
MAIN="cn.itcraft.speedboat.sample.VmClusterNode"
PID_FILE="${BASE}/run/node.pid"

case "${1:-}" in
  start)
    local_ip="$2"; cfg="$3"; mode="$4"; lock="$5"; logf="$6"
    cd "${BASE}"
    mkdir -p logs run
    nohup "${JAVA}" \
        -Dlogback.configurationFile="${BASE}/logback-vm.xml" \
        -Dspeedboat.local.ip="${local_ip}" \
        -cp "${CP}" "${MAIN}" \
        --config "${BASE}/${cfg}" --mode "${mode}" --lock "${lock}" \
        > "${BASE}/${logf}" 2>&1 &
    echo $! > "${PID_FILE}"
    echo "started pid=$(cat "${PID_FILE}")"
    ;;
  stop)
    if [ -f "${PID_FILE}" ]; then
      pid="$(cat "${PID_FILE}")"
      kill "${pid}" 2>/dev/null || true
      for _ in $(seq 1 30); do
        kill -0 "${pid}" 2>/dev/null || break
        sleep 0.5
      done
      kill -9 "${pid}" 2>/dev/null || true
      echo "stopped pid=${pid}"
    else
      echo "no-pid"
    fi
    ;;
  kill9)
    if [ -f "${PID_FILE}" ]; then
      pid="$(cat "${PID_FILE}")"
      kill -9 "${pid}" 2>/dev/null || true
      echo "killed9 pid=${pid}"
    else
      echo "no-pid"
    fi
    ;;
  alive)
    if [ -f "${PID_FILE}" ] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
      echo alive
    else
      echo dead
    fi
    ;;
  clean)
    rm -rf "${BASE}/data" "${BASE}/logs"
    mkdir -p "${BASE}/logs" "${BASE}/run"
    echo cleaned
    ;;
  *)
    echo "usage: $0 {start|stop|kill9|alive|clean} ..." >&2
    exit 64
    ;;
esac
