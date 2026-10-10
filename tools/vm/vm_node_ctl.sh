#!/usr/bin/env bash
#
# Speedboat 单节点远程控制（部署于各 VM 的 ~/speedboat-test/ 内，由宿主编排脚本调用）。
#
# 职责：在远端以固定工作目录、固定 Java（Dragonwell 8）、固定 classpath 启停被测节点。
# 身份由 -Dspeedboat.local.ip 显式钉定，避免多网卡枚举顺序漂移。
#
# 用法：
#   vm_node_ctl.sh start <localIp> <configFile> <mode> <lockName> <logFile> [localPort]
#     可选 localPort：同 IP 多端口（单机多进程）身份钉定，见 -Dspeedboat.local.port
#   vm_node_ctl.sh stop        # 优雅停机（SIGTERM，触发 shutdown hook）
#   vm_node_ctl.sh kill9       # 模拟崩溃（SIGKILL，无收尾）
#   vm_node_ctl.sh alive       # 输出 alive/dead
#   vm_node_ctl.sh clean       # 清空 data/ 与 logs/
#
set -euo pipefail

BASE="${HOME}/speedboat-test"
MAIN="cn.itcraft.speedboat.sample.VmClusterNode"
PID_FILE="${BASE}/run/node.pid"
# 宿主机同机多进程共用 BASE 时（阶段六备机房三进程），以 SB_PID_SUFFIX 区分
# pid 文件（-h0/-h1/-h2）；虚机单进程环境不设该变量，行为与原状完全一致。
PID_FILE="${BASE}/run/node${SB_PID_SUFFIX:-}.pid"

# ---------------------------------------------------------------- Java 解析
# 各环境差异较大：vbox 三机装了 Dragonwell 8；六台实机只带 JRE/JDK 8 或 11，
# 且主机房（17x）甚至没有 javac。解析优先级：
#   1) SB_JAVA 环境变量（显式指定，最高优先）
#   2) Dragonwell 8（vbox 既有路径，保持原行为）
#   3) /usr/lib/jvm 下的 Java 8（对齐目标运行时）
#   4) PATH 上的 java（兜底；被测 jar 以 --release 8 构建，Java 8/11 均可运行）
resolve_java() {
  if [ -n "${SB_JAVA:-}" ]; then
    echo "${SB_JAVA}"
    return
  fi
  if [ -x "${HOME}/lang/dragonwell-8.29.28/bin/java" ]; then
    echo "${HOME}/lang/dragonwell-8.29.28/bin/java"
    return
  fi
  local j
  for j in /usr/lib/jvm/java-1.8.0*/bin/java /usr/lib/jvm/jre-1.8.0*/bin/java; do
    if [ -x "${j}" ]; then
      echo "${j}"
      return
    fi
  done
  command -v java 2>/dev/null || echo "java"
}
JAVA="$(resolve_java)"

# ------------------------------------------------------------- classpath 解析
# 优先用预编译的 vm-runner.jar（目标机无 javac 时由宿主机编译后下发，字节码架构无关），
# 回退到 runner/ 编译产物目录（vbox 既有路径）。
CP=""
if [ -f "${BASE}/vm-runner.jar" ]; then
  CP="${BASE}/vm-runner.jar"
fi
if [ -d "${BASE}/runner" ]; then
  CP="${CP:+${CP}:}${BASE}/runner"
fi
CP="${CP:+${CP}:}${BASE}/speedboat.jar"

case "${1:-}" in
  start)
    local_ip="$2"; cfg="$3"; mode="$4"; lock="$5"; logf="$6"
    # 可选第 7 参 localPort：单机多进程（同 IP 多端口）身份钉定，
    # 传入时以 -Dspeedboat.local.port 精确匹配 nodes 条目；缺省不传，行为与原状一致
    local_port="${7:-}"
    cd "${BASE}"
    mkdir -p logs run
    if [ ! -x "${JAVA}" ] && ! command -v "${JAVA}" >/dev/null 2>&1; then
      echo "java-not-found: ${JAVA}" >&2
      exit 65
    fi
    echo "java=${JAVA} cp=${CP}" >> "${BASE}/logs/ctl.log"
    PORT_ARGS=()
    if [ -n "${local_port}" ]; then
      PORT_ARGS=(-Dspeedboat.local.port="${local_port}")
    fi
    nohup "${JAVA}" \
        -Dlogback.configurationFile="${BASE}/logback-vm.xml" \
        -Dspeedboat.local.ip="${local_ip}" \
        "${PORT_ARGS[@]}" \
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
