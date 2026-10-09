#!/usr/bin/env bash
#
# Speedboat 实机验证编排公共库（被 vm-cluster-test.sh / vm-crossdc-test.sh source）。
#
# 提供与具体环境无关的能力：
#   - SSH 通道（SSH_USER 由调用方指定）
#   - 结构化日志解析（event=STATE 行的 key=value 取值）
#   - 远端进程存活探测
#   - "给定 host 集合内唯一 Leader"的轮询等待与定位
#   - 断言计数与最终汇总报告
#
# 调用方约定（source 之前必须定义）：
#   SSH_USER   远端 ssh 用户名，如 vboxuser / tomcat
#   HOSTS      全部 host 短名数组（含序，用于遍历）
#   IP         关联数组：host 短名 -> IP，用于 nodeId 校验与 start
#   PORT       关联数组：host 短名 -> 端口，用于 nodeId 校验（可省略，届时跳过该校验）
#   SSH / SCP  ssh / scp 参数数组（须在 source 之前定义）
#   host_target() 可选覆盖：短名 -> SSH 目标（默认原样返回）
#
# 注意：本文件被 source，因此不单独声明 shebang 语义；调用方脚本应先 set -uo pipefail。
set -uo pipefail

PASS=0; FAIL=0; declare -a FAILURES

ts() { date +%H:%M:%S; }
log() { echo "[$(ts)] $*"; }

# 断言：check <描述> <命令...>；命令退出码 0 记 PASS
check() {
  local desc=$1; shift
  if "$@"; then PASS=$((PASS+1)); echo "  [PASS] $desc"
  else FAIL=$((FAIL+1)); FAILURES+=("$desc"); echo "  [FAIL] $desc"; fi
}

# 短名 -> SSH 目标解析。默认原样返回（适用于 vboxdeb001 这类可直接解析的主机名）；
# 调用方可覆盖本函数以支持别名（如 h174 -> 192.168.193.174）。
host_target() { echo "$1"; }

# 远端执行：rsh <host> <命令...>
rsh() { local h=$1; shift; "${SSH[@]}" "${SSH_USER}@$(host_target "$h")" "$@"; }

# 远端下发：scph <host> <本地文件> <远端路径（可含 ~）>
scph() {
  local h=$1 local_file=$2 remote_path=$3
  "${SCP[@]}" "$local_file" "${SSH_USER}@$(host_target "$h"):${remote_path}"
}

# 从一行结构化日志中取 key=value（如 node / term / leader / isMain / dc）
fld() { echo "$1" | sed -n "s/.*[[:space:]]$2=\([^[:space:]]*\).*/\1/p"; }

# 各节点日志在远端的相对路径
rlog() { echo "logs/$1.log"; }

# 最近一条 STATE 行（进程已死时返回的是其最后一行，仍可用于读取 term/leader）
state_line() { rsh "$1" "grep 'event=STATE' ~/speedboat-test/$(rlog "$1") 2>/dev/null | tail -1"; }

# 进程存活探测
alive() { [ "$(rsh "$1" '~/speedboat-test/vm_node_ctl.sh alive' 2>/dev/null)" = "alive" ]; }

# 轮询等待"给定 host 集合内恰好一个 isMain=true，且集合内各存活节点 leader 字段一致"。
# 用法：wait_single_leader <秒> <host...>；命中返回 0，超时返回 1。
#
# 判定口径用 isMain 而非 leader 字段：kill 之后残留的 leader 字段可能仍指向已死节点，
# 若仅按 leader 字段判唯一，会把"全网仍同意一个已死节点"误判为已选出新主。
wait_single_leader() {
  local secs=$1; shift
  local -a hs=("$@")
  local i h
  for ((i = 0; i < secs; i++)); do
    local mains=0 mnode=""
    for h in "${hs[@]}"; do
      alive "$h" || continue
      local line im
      line=$(state_line "$h"); im=$(fld "$line" isMain)
      if [ "$im" = "true" ]; then mains=$((mains + 1)); mnode=$(fld "$line" node); fi
    done
    if [ "$mains" -eq 1 ]; then
      local ok=1
      for h in "${hs[@]}"; do
        alive "$h" || continue
        [ "$(fld "$(state_line "$h")" leader)" = "$mnode" ] || ok=0
      done
      [ "$ok" = "1" ] && return 0
    fi
    sleep 1
  done
  return 1
}

# 返回给定 host 集合内当前 isMain=true 的 host（空表示未定）
leader_host_in() {
  local -a hs=("$@")
  local h
  for h in "${hs[@]}"; do
    alive "$h" || continue
    [ "$(fld "$(state_line "$h")" isMain)" = "true" ] && { echo "$h"; return; }
  done
  echo ""
}

# 给定 host 集合内 isMain=true 的数量
count_main_in() {
  local -a hs=("$@")
  local h n=0
  for h in "${hs[@]}"; do
    alive "$h" || continue
    [ "$(fld "$(state_line "$h")" isMain)" = "true" ] && n=$((n + 1))
  done
  echo "$n"
}

# nodeId 推导校验：node-<ip>-<port>（身份确定性）
node_id_ok() {
  local h=$1 nid
  nid=$(fld "$(state_line "$h")" node)
  [ "$nid" = "node-${IP[$h]}-${PORT[$h]}" ]
}

# 最终汇总报告；全部通过返回 0
report() {
  echo
  echo "========================================"
  echo "实机验证结果： $PASS 通过, $FAIL 失败"
  if [ "$FAIL" -gt 0 ]; then
    echo "失败项："
    local f
    for f in "${FAILURES[@]}"; do echo "  - $f"; done
  fi
  echo "========================================"
  [ "$FAIL" -eq 0 ]
}
